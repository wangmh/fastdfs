/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_service.c

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include "fdfs_define.h"
#include "logger.h"
#include "fdfs_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "pthread_func.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "storage_service.h"
#include "storage_func.h"
#include "storage_sync.h"
#include "storage_global.h"
#include "base64.h"
#include "hash.h"
#include "fdht_client.h"
#include "fdfs_global.h"
#include "tracker_client.h"
#include "storage_client.h"

pthread_mutex_t g_storage_thread_lock;
int g_storage_thread_count = 0;
static int last_stat_change_count = 1;  //for sync to stat file

static pthread_mutex_t path_index_thread_lock;
static pthread_mutex_t stat_count_thread_lock;

extern int storage_client_create_link(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, const char *master_filename,\
		const char *src_filename, const int src_filename_len, \
		const char *src_file_sig, const int src_file_sig_len, \
		const char *group_name, const char *prefix_name, \
		const char *file_ext_name, \
		char *meta_buff, const int meta_size, \
		char *remote_filename, int *filename_len);

#define FDHT_KEY_NAME_FILE_ID	"fid"
#define FDHT_KEY_NAME_REF_COUNT	"ref"
#define FDHT_KEY_NAME_FILE_SIG	"sig"

int storage_service_init()
{
	int result;

	if ((result=init_pthread_lock(&g_storage_thread_lock)) != 0)
	{
		return result;
	}

	if ((result=init_pthread_lock(&path_index_thread_lock)) != 0)
	{
		return result;
	}

	if ((result=init_pthread_lock(&stat_count_thread_lock)) != 0)
	{
		return result;
	}

	last_stat_change_count = g_stat_change_count;

	return result;
}

void storage_service_destroy()
{
	pthread_mutex_destroy(&g_storage_thread_lock);
	pthread_mutex_destroy(&path_index_thread_lock);
	pthread_mutex_destroy(&stat_count_thread_lock);
}

static int storage_gen_filename(StorageClientInfo *pClientInfo, \
		const int64_t file_size, \
		const char *szFormattedExt, const int ext_name_len, \
		const time_t timestamp, char *filename, int *filename_len)
{
	int result;
	int r;
	char buff[sizeof(int) * 5];
	char encoded[sizeof(int) * 8 + 1];
	char szStorageIp[IP_ADDRESS_SIZE];
	int n;
	int len;
	in_addr_t server_ip;

	server_ip = getSockIpaddr(pClientInfo->sock, \
			szStorageIp, IP_ADDRESS_SIZE);
	r = rand();

	int2buff(htonl(server_ip), buff);
	int2buff(timestamp, buff+sizeof(int));
	long2buff(file_size, buff+sizeof(int)*2);
	int2buff(r, buff+sizeof(int)*4);

	base64_encode_ex(&g_base64_context, buff, sizeof(int) * 5, encoded, \
			filename_len, false);

	if (g_file_distribute_path_mode == FDFS_FILE_DIST_PATH_ROUND_ROBIN)
	{
		len = sprintf(buff, STORAGE_DATA_DIR_FORMAT"/", \
				g_dist_path_index_high);
		len += sprintf(buff + len, STORAGE_DATA_DIR_FORMAT"/", \
				g_dist_path_index_low);

		if (++g_dist_write_file_count >= g_file_distribute_rotate_count)
		{
			g_dist_write_file_count = 0;
	
			if ((result=pthread_mutex_lock( \
					&path_index_thread_lock)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_lock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
			}

			++g_dist_path_index_low;
			if (g_dist_path_index_low >= g_subdir_count_per_path)
			{  //rotate
				g_dist_path_index_high++;
				if (g_dist_path_index_high >= \
					g_subdir_count_per_path)  //rotate
				{
					g_dist_path_index_high = 0;
				}
				g_dist_path_index_low = 0;
			}

			++g_stat_change_count;

			if ((result=pthread_mutex_unlock( \
					&path_index_thread_lock)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_unlock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
			}
		}
	}  //random
	else
	{
		n = PJWHash(encoded, *filename_len) % (1 << 16);

		len = sprintf(buff,STORAGE_DATA_DIR_FORMAT"/",((n >> 8)&0xFF) \
				% g_subdir_count_per_path);
		len += sprintf(buff+len, STORAGE_DATA_DIR_FORMAT"/",(n & 0xFF)\
				 % g_subdir_count_per_path);
	}

	memcpy(filename, buff, len);
	memcpy(filename+len, encoded, *filename_len);
	memcpy(filename+len+(*filename_len), szFormattedExt, ext_name_len);
	*filename_len += len + ext_name_len;
	*(filename + (*filename_len)) = '\0';

	return 0;
}

static int storage_sort_metadata_buff(char *meta_buff, const int meta_size)
{
	FDFSMetaData *meta_list;
	int meta_count;
	int meta_bytes;
	int result;

	meta_list = fdfs_split_metadata(meta_buff, &meta_count, &result);
	if (meta_list == NULL)
	{
		return result;
	}

	/*
	{
	int i;
	//printf("meta_count=%d\n", meta_count);
	for (i=0; i<meta_count; i++)
	{
		//printf("%d. %s=%s\n", i+1, meta_list[i].name, meta_list[i].value);
	}
	}
	*/

	qsort((void *)meta_list, meta_count, sizeof(FDFSMetaData), \
		metadata_cmp_by_name);

	fdfs_pack_metadata(meta_list, meta_count, meta_buff, &meta_bytes);
	free(meta_list);

	return 0;
}

#define FILE_SIGNATURE_SIZE	24

#define STORAGE_GEN_FILE_SIGNATURE(file_size, hash_codes, sig_buff) \
	long2buff(file_size, sig_buff); \
	int2buff(hash_codes[0], sig_buff+8);  \
	int2buff(hash_codes[1], sig_buff+12); \
	int2buff(hash_codes[2], sig_buff+16); \
	int2buff(hash_codes[3], sig_buff+20); \


#define STORAGE_CREATE_FLAG_NONE  0
#define STORAGE_CREATE_FLAG_FILE  1
#define STORAGE_CREATE_FLAG_LINK  2

#define STORAGE_DELETE_FLAG_NONE  0
#define STORAGE_DELETE_FLAG_FILE  1
#define STORAGE_DELETE_FLAG_LINK  2

typedef struct 
{
	char src_true_filename[128];
	char src_file_sig[64];
	int src_file_sig_len;
} SourceFileInfo;

#define storage_save_file(pClientInfo, pGroupArray, store_path_index, \
		file_size, master_filename, prefix_name, file_ext_name, \
		meta_buff, meta_size, filename, filename_len, create_flag) \
	storage_deal_file(pClientInfo, pGroupArray, store_path_index, NULL, \
		file_size, master_filename, prefix_name, file_ext_name, \
		meta_buff, meta_size, filename, filename_len, create_flag)

static int storage_deal_file(StorageClientInfo *pClientInfo, \
		GroupArray *pGroupArray, \
		const int store_path_index, const SourceFileInfo *pSrcFileInfo,\
		const int64_t file_size, const char *master_filename, \
		 const char *prefix_name, const char *file_ext_name,  \
		char *meta_buff, const int meta_size, \
		char *filename, int *filename_len, int *create_flag)
{
#define FILE_TIMESTAMP_ADVANCED_SECS	0

	int result;
	int i;
	char *pStorePath;
	char src_full_filename[MAX_PATH_SIZE+64];
	char full_filename[MAX_PATH_SIZE+64];
	char meta_filename[MAX_PATH_SIZE+64];
	char szFormattedExt[FDFS_FILE_EXT_NAME_MAX_LEN + 2];
	char *p;
	int ext_name_len;
	int pad_len;
	time_t start_time;
	unsigned int file_hash_codes[4];
	FDHTKeyInfo key_info;
	char value[128];
	bool bGenFilename;
	char *pValue;
	int value_len;

	*create_flag = STORAGE_CREATE_FLAG_NONE;

	ext_name_len = strlen(file_ext_name);
	if (ext_name_len == 0)
	{
		pad_len = FDFS_FILE_EXT_NAME_MAX_LEN + 1;
	}
	else
	{
		pad_len = FDFS_FILE_EXT_NAME_MAX_LEN - ext_name_len;
	}

	p = szFormattedExt;
	for (i=0; i<pad_len; i++)
	{
		*p++ = '0' + (int)(10.0 * (double)rand() / RAND_MAX);
	}

	if (ext_name_len > 0)
	{
		*p++ = '.';
		memcpy(p, file_ext_name, ext_name_len);
		p += ext_name_len;
	}
	*p = '\0';

	pStorePath = g_store_paths[store_path_index];
	start_time = time(NULL);

        bGenFilename = (*filename_len == 0 || (pSrcFileInfo == NULL && \
			g_check_file_duplicate));
	if (bGenFilename)
	{
		for (i=0; i<10; i++)
		{
		if ((result=storage_gen_filename(pClientInfo, file_size, \
				szFormattedExt, FDFS_FILE_EXT_NAME_MAX_LEN+1, \
				start_time + FILE_TIMESTAMP_ADVANCED_SECS, \
				filename, filename_len)) != 0)
		{
			return result;
		}

		sprintf(full_filename, "%s/data/%s", pStorePath, filename);
		if (!fileExists(full_filename))
		{
			break;
		}

		*full_filename = '\0';
		}

		if (*full_filename == '\0')
		{
		logError("file: "__FILE__", line: %d, " \
			"Can't generate uniq filename", __LINE__);
		*filename = '\0';
		*filename_len = 0;
		return ENOENT;
		}
	}
	else
	{
		sprintf(full_filename, "%s/data/%s", pStorePath, filename);
		if (fileExists(full_filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"slave file: %s already exist", \
				__LINE__, full_filename);
			return EEXIST;
		}
	}

	memset(&key_info, 0, sizeof(key_info));
	key_info.namespace_len = g_namespace_len;
	memcpy(key_info.szNameSpace, g_key_namespace, g_namespace_len);

	if (pSrcFileInfo == NULL)	//upload file
	{
		if (g_check_file_duplicate)
		{
			int nSigLen;
			char szFileSig[FILE_SIGNATURE_SIZE];
			char src_filename[MAX_PATH_SIZE];
			TrackerServerInfo trackerServer;

			if ((result=tcprecvfile_ex(pClientInfo->sock, 
				full_filename, file_size, \
				g_fsync_after_written_bytes, \
				file_hash_codes, g_fdfs_network_timeout)) != 0)
			{
				*filename = '\0';
				*filename_len = 0;
				return result;
			}

			STORAGE_GEN_FILE_SIGNATURE(file_size, file_hash_codes, \
					szFileSig)

			nSigLen = FILE_SIGNATURE_SIZE;
			key_info.obj_id_len = nSigLen;
			memcpy(key_info.szObjectId, szFileSig, nSigLen);
			key_info.key_len = sizeof(FDHT_KEY_NAME_FILE_ID) - 1;
			memcpy(key_info.szKey, FDHT_KEY_NAME_FILE_ID, \
				sizeof(FDHT_KEY_NAME_FILE_ID) - 1);

			pValue = value;
			value_len = sizeof(value) - 1;
			result = fdht_get_ex1(pGroupArray, g_keep_alive, \
					&key_info, FDHT_EXPIRES_NONE, \
					&pValue, &value_len, malloc);
			if (result == 0)
			{   //exists
				char *pGroupName;
				char *pSrcFilename;
				char *pSeperator;

				if (unlink(full_filename) != 0)
				{
					result = errno != 0 ? errno : EPERM;
					logError("file: "__FILE__", line: %d, "\
						"unlink %s fail, errno: %d, " \
						"error info: %s", \
						__LINE__, full_filename, \
						result, strerror(result));

					*filename = '\0';
					*filename_len = 0;
					return result;
				}

				*(value + value_len) = '\0';

				pSeperator = strchr(value, '/');
				if (pSeperator == NULL)
				{
					logError("file: "__FILE__", line: %d, "\
						"value %s is invalid", \
						__LINE__, value);

					*filename = '\0';
					*filename_len = 0;
					return EINVAL;
				}

				if ((result=tracker_get_connection_r( \
						&trackerServer)) != 0)
				{
					*filename = '\0';
					*filename_len = 0;
					return result;
				}

				*pSeperator = '\0';
				pGroupName = value;
				pSrcFilename = pSeperator + 1;
				result = storage_client_create_link(\
					&trackerServer, NULL, master_filename, \
					pSrcFilename, value_len - \
					(pSrcFilename - value), \
					key_info.szObjectId, \
					key_info.obj_id_len, \
					pGroupName, prefix_name, file_ext_name,\
					meta_buff, meta_size, \
					filename, filename_len);

				fdfs_quit(&trackerServer);
				tracker_disconnect_server(&trackerServer);

				*create_flag = STORAGE_CREATE_FLAG_LINK;
				return result;
			}
			else if (result == ENOENT)
			{
				*filename_len=sprintf(src_filename, "%c" \
					STORAGE_DATA_DIR_FORMAT"/%s", \
					STORAGE_STORE_PATH_PREFIX_CHAR, \
					store_path_index, filename);
				value_len = sprintf(value, "%s/%s", \
						g_group_name, src_filename);
				if ((result=fdht_set_ex(pGroupArray, \
						g_keep_alive, &key_info, \
						FDHT_EXPIRES_NEVER, \
						value, value_len)) != 0)
				{
					logError("file: "__FILE__", line: %d, "\
						"client ip: %s, fdht_set fail,"\
						"errno: %d, error info: %s", \
						__LINE__, pClientInfo->ip_addr, \
						result, strerror(result));

					unlink(full_filename);
					*filename = '\0';
					*filename_len = 0;
					return result;
				}

				key_info.obj_id_len = value_len;
				memcpy(key_info.szObjectId, value, value_len);
				key_info.key_len = sizeof( \
						FDHT_KEY_NAME_REF_COUNT) - 1;
				memcpy(key_info.szKey, FDHT_KEY_NAME_REF_COUNT,\
						key_info.key_len);
				if ((result=fdht_set_ex(pGroupArray, \
						g_keep_alive, &key_info, \
						FDHT_EXPIRES_NEVER, "0", 1))!=0)
				{
					logError("file: "__FILE__", line: %d, "\
						"client ip: %s, fdht_set fail,"\
						"errno: %d, error info: %s", \
						__LINE__, pClientInfo->ip_addr, \
						result, strerror(result));

					unlink(full_filename);
					*filename = '\0';
					*filename_len = 0;
					return result;
				}

				if ((result=tracker_get_connection_r( \
						&trackerServer)) != 0)
				{
					unlink(full_filename);
					*filename = '\0';
					*filename_len = 0;
					return result;
				}

				result = storage_binlog_write(time(NULL), \
					STORAGE_OP_TYPE_SOURCE_CREATE_FILE, \
					src_filename);
				if (result != 0)
				{
					unlink(full_filename);
					*filename = '\0';
					*filename_len = 0;
					return result;
				}

				result = storage_client_create_link(\
					&trackerServer, NULL, master_filename, \
					src_filename, *filename_len, \
					szFileSig, nSigLen, \
					g_group_name, prefix_name, \
					file_ext_name, meta_buff, meta_size, \
					filename, filename_len);

				fdfs_quit(&trackerServer);
				tracker_disconnect_server(&trackerServer);

				if (result != 0)
				{
					unlink(full_filename);
					*filename = '\0';
					*filename_len = 0;
					return result;
				}

				*create_flag = STORAGE_CREATE_FLAG_FILE | \
						STORAGE_CREATE_FLAG_LINK;
				return result;
			}
			else //error
			{
				logError("file: "__FILE__", line: %d, " \
					"fdht_get fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(errno));

				unlink(full_filename);
				*filename = '\0';
				*filename_len = 0;
				return result;
			}
		}
		else
		{
			if ((result=tcprecvfile(pClientInfo->sock, 
				full_filename, file_size, \
				g_fsync_after_written_bytes, \
				g_fdfs_network_timeout)) != 0)
			{
				*filename = '\0';
				*filename_len = 0;
				return result;
			}
		}
	}
	else //create link
	{
		sprintf(src_full_filename, "%s/data/%s", pStorePath, \
			pSrcFileInfo->src_true_filename);
		if (symlink(src_full_filename, full_filename) != 0)
		{
			result = errno != 0 ? errno : ENOENT;
			logError("file: "__FILE__", line: %d, " \
				"link file %s to %s fail, " \
				"errno: %d, error info: %s", __LINE__, \
				src_full_filename, full_filename, \
				result, strerror(result));
			*filename = '\0';
			*filename_len = 0;
			return result;
		}
	}

	if (meta_size > 0)
	{
		if ((result=storage_sort_metadata_buff(meta_buff, \
				meta_size)) != 0)
		{
			*filename = '\0';
			*filename_len = 0;
			unlink(full_filename);
			return result;
		}

		sprintf(meta_filename, "%s"STORAGE_META_FILE_EXT, \
				full_filename);
		if ((result=writeToFile(meta_filename, meta_buff, \
				meta_size)) != 0)
		{
			*filename = '\0';
			*filename_len = 0;
			unlink(full_filename);
			return result;
		}
	}
	else
	{
		*meta_filename = '\0';
	}

	if (pSrcFileInfo == NULL && bGenFilename)	//upload file
	{
	time_t end_time;
	end_time = time(NULL);
	if (end_time - start_time > FILE_TIMESTAMP_ADVANCED_SECS)
	{  //need to rename filename
		char new_full_filename[MAX_PATH_SIZE+64];
		char new_meta_filename[MAX_PATH_SIZE+64];
		char new_filename[64];
		int new_filename_len;

		*new_full_filename = '\0';
		for (i=0; i<10; i++)
		{
			if ((result=storage_gen_filename(pClientInfo,file_size,\
				szFormattedExt, FDFS_FILE_EXT_NAME_MAX_LEN+1, \
				end_time, new_filename, &new_filename_len))!=0)
			{
				break;
			}

			sprintf(new_full_filename, "%s/data/%s", \
				pStorePath, new_filename);
			if (!fileExists(new_full_filename))
			{
				break;
			}

			*new_full_filename = '\0';
		}

		do
		{
		if (*new_full_filename == '\0')
		{
			logWarning("file: "__FILE__", line: %d, " \
				"Can't generate uniq filename", __LINE__);
			break;
		}

		if (rename(full_filename, new_full_filename) != 0)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"rename %s to %s fail, " \
				"errno: %d, error info: %s", __LINE__, \
				full_filename, new_full_filename, \
				errno, strerror(errno));
			break;
		}

		if (*meta_filename != '\0')
		{
			sprintf(new_meta_filename, "%s"STORAGE_META_FILE_EXT, \
					new_full_filename);
			if (rename(meta_filename, new_meta_filename) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"rename %s to %s fail, " \
					"errno: %d, error info: %s", __LINE__,\
					meta_filename, new_meta_filename, \
					errno, strerror(errno));

				unlink(new_full_filename);
				*filename = '\0';
				*filename_len = 0;
				return errno != 0 ? errno : EPERM;
			}
		}

		*filename_len = new_filename_len;
		memcpy(filename, new_filename, new_filename_len+1);
		} while (0);
	}
	}

	*filename_len=sprintf(full_filename, "%c"STORAGE_DATA_DIR_FORMAT"/%s", \
			STORAGE_STORE_PATH_PREFIX_CHAR, \
			store_path_index, filename);
	memcpy(filename, full_filename, (*filename_len) + 1);

	if (pSrcFileInfo != NULL && g_check_file_duplicate)   //create link
	{
		*create_flag = STORAGE_CREATE_FLAG_LINK;

		key_info.obj_id_len = snprintf(key_info.szObjectId, \
			sizeof(key_info.szObjectId), \
			"%s/%c"STORAGE_DATA_DIR_FORMAT"/%s", \
			g_group_name, STORAGE_STORE_PATH_PREFIX_CHAR, \
			store_path_index, pSrcFileInfo->src_true_filename);

		key_info.key_len = sizeof(FDHT_KEY_NAME_REF_COUNT) - 1;
		memcpy(key_info.szKey, FDHT_KEY_NAME_REF_COUNT, \
			key_info.key_len);
		value_len = sizeof(value) - 1;
		if ((result=fdht_inc_ex(pGroupArray, g_keep_alive, &key_info, \
			FDHT_EXPIRES_NEVER, 1, value, &value_len)) != 0)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"client ip: %s, fdht_inc fail," \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, \
				result, strerror(result));
		}
		else
		{
		//*(value + value_len) = '\0';

		key_info.obj_id_len = snprintf(key_info.szObjectId, \
			sizeof(key_info.szObjectId), \
			"%s/%s", g_group_name, filename);
		key_info.key_len = sizeof(FDHT_KEY_NAME_FILE_SIG) - 1;
		memcpy(key_info.szKey, FDHT_KEY_NAME_FILE_SIG, \
			key_info.key_len);
		if ((result=fdht_set_ex(pGroupArray, g_keep_alive, \
			&key_info, FDHT_EXPIRES_NEVER, \
			pSrcFileInfo->src_file_sig, \
			pSrcFileInfo->src_file_sig_len)) != 0)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"client ip: %s, fdht_set fail," \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, \
				result, strerror(result));
		}

		/*
		logInfo("create link, counter=%s, object_id=%s(%d), key=%s, file_sig_len=(%d)", \
			value, key_info.szObjectId, key_info.obj_id_len, \
			FDHT_KEY_NAME_FILE_SIG, \
			pSrcFileInfo->src_file_sig_len);
		*/
		}
	}
	else //upload file
	{
		*create_flag = STORAGE_CREATE_FLAG_FILE;
	}

	return 0;
}

static int storage_do_set_metadata(StorageClientInfo *pClientInfo, \
		const char *full_meta_filename, char *meta_buff, \
		const int meta_bytes, const char op_flag, char *sync_flag)
{
	FDFSMetaData *old_meta_list;
	FDFSMetaData *new_meta_list;
	FDFSMetaData *all_meta_list;
	FDFSMetaData *pOldMeta;
	FDFSMetaData *pNewMeta;
	FDFSMetaData *pAllMeta;
	FDFSMetaData *pOldMetaEnd;
	FDFSMetaData *pNewMetaEnd;
	char *file_buff;
	char *all_meta_buff;
	int64_t file_bytes;
	int old_meta_count;
	int new_meta_count;
	int all_meta_bytes;
	int result;

	*sync_flag = '\0';
	if (op_flag == STORAGE_SET_METADATA_FLAG_OVERWRITE)
	{
		if (meta_bytes == 0)
		{
			if (!fileExists(full_meta_filename))
			{
				return 0;
			}

			if (unlink(full_meta_filename) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, " \
					"delete meta file %s fail," \
					"errno: %d, error info: %s", \
					__LINE__, \
					pClientInfo->ip_addr, \
					full_meta_filename, \
					errno, strerror(errno));
				return errno != 0 ? errno : EACCES;
			}

			*sync_flag = STORAGE_OP_TYPE_SOURCE_DELETE_FILE;
			return 0;
		}

		if ((result=storage_sort_metadata_buff(meta_buff, \
				meta_bytes)) != 0)
		{
			return result;
		}

		if (fileExists(full_meta_filename))
		{
			*sync_flag = STORAGE_OP_TYPE_SOURCE_UPDATE_FILE;
		}
		else
		{
			*sync_flag = STORAGE_OP_TYPE_SOURCE_CREATE_FILE;
		}
		return writeToFile(full_meta_filename, meta_buff, meta_bytes);
	}

	result = getFileContent(full_meta_filename, &file_buff, &file_bytes);
	if (result == ENOENT)
	{
		if (meta_bytes == 0)
		{
			return 0;
		}

		if ((result=storage_sort_metadata_buff(meta_buff, \
				meta_bytes)) != 0)
		{
			return result;
		}

		*sync_flag = STORAGE_OP_TYPE_SOURCE_CREATE_FILE;
		return writeToFile(full_meta_filename, meta_buff, meta_bytes);
	}
	else if (result != 0)
	{
		return result;
	}

	old_meta_list = fdfs_split_metadata(file_buff, &old_meta_count, &result);
	if (old_meta_list == NULL)
	{
		free(file_buff);
		return result;
	}

	new_meta_list = fdfs_split_metadata(meta_buff, &new_meta_count, &result);
	if (new_meta_list == NULL)
	{
		free(file_buff);
		free(old_meta_list);
		return result;
	}

	all_meta_list = (FDFSMetaData *)malloc(sizeof(FDFSMetaData) * \
				(old_meta_count + new_meta_count));
	if (all_meta_list == NULL)
	{
		free(file_buff);
		free(old_meta_list);
		free(new_meta_list);

		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", __LINE__, \
			(int)sizeof(FDFSMetaData) \
			 * (old_meta_count + new_meta_count));
		return ENOMEM;
	}

	qsort((void *)new_meta_list, new_meta_count, sizeof(FDFSMetaData), \
		metadata_cmp_by_name);

	pOldMetaEnd = old_meta_list + old_meta_count;
	pNewMetaEnd = new_meta_list + new_meta_count;
	pOldMeta = old_meta_list;
	pNewMeta = new_meta_list;
	pAllMeta = all_meta_list;
	while (pOldMeta < pOldMetaEnd && pNewMeta < pNewMetaEnd)
	{
		result = strcmp(pOldMeta->name, pNewMeta->name);
		if (result < 0)
		{
			memcpy(pAllMeta, pOldMeta, sizeof(FDFSMetaData));
			pOldMeta++;
		}
		else if (result == 0)
		{
			memcpy(pAllMeta, pNewMeta, sizeof(FDFSMetaData));
			pOldMeta++;
			pNewMeta++;
		}
		else  //result > 0
		{
			memcpy(pAllMeta, pNewMeta, sizeof(FDFSMetaData));
			pNewMeta++;
		}

		pAllMeta++;
	}

	while (pOldMeta < pOldMetaEnd)
	{
		memcpy(pAllMeta, pOldMeta, sizeof(FDFSMetaData));
		pOldMeta++;
		pAllMeta++;
	}

	while (pNewMeta < pNewMetaEnd)
	{
		memcpy(pAllMeta, pNewMeta, sizeof(FDFSMetaData));
		pNewMeta++;
		pAllMeta++;
	}

	free(file_buff);
	free(old_meta_list);
	free(new_meta_list);

	all_meta_buff = fdfs_pack_metadata(all_meta_list, \
			pAllMeta - all_meta_list, NULL, &all_meta_bytes);
	free(all_meta_list);
	if (all_meta_buff == NULL)
	{
		return ENOMEM;
	}

	*sync_flag = STORAGE_OP_TYPE_SOURCE_UPDATE_FILE;
	result = writeToFile(full_meta_filename, all_meta_buff, all_meta_bytes);
	free(all_meta_buff);

	return result;
}

/**
8 bytes: filename length
8 bytes: meta data size
1 bytes: operation flag, 
     'O' for overwrite all old metadata
     'M' for merge, insert when the meta item not exist, otherwise update it
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename
meta data bytes: each meta data seperated by \x01,
		 name and value seperated by \x02
**/
static int storage_server_set_metadata(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	char *in_buff;
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char filename[128];
	char true_filename[128];
	char meta_filename[128+sizeof(STORAGE_META_FILE_EXT)];
	char full_filename[MAX_PATH_SIZE + 64 + sizeof(STORAGE_META_FILE_EXT)];
	char op_flag;
	char sync_flag;
	char *meta_buff;
	int meta_bytes;
	char *pBasePath;
	int filename_len;
	int true_filename_len;
	int result;

	memset(&resp, 0, sizeof(resp));
	in_buff = NULL;
	do
	{
		if (nInPackLen <= 2 * FDFS_PROTO_PKG_LEN_SIZE + 1 + \
					FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length > %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_SET_METADATA, \
				pClientInfo->ip_addr,  \
				nInPackLen, 2 * FDFS_PROTO_PKG_LEN_SIZE + 1 \
				+ FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		in_buff = (char *)malloc(nInPackLen + 1);
		if (in_buff == NULL)
		{
			resp.status = ENOMEM;

			logError("file: "__FILE__", line: %d, " \
				"malloc "INT64_PRINTF_FORMAT" bytes fail", \
				__LINE__, nInPackLen + 1);
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
			nInPackLen, g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		*(in_buff + nInPackLen) = '\0';
		filename_len = buff2long(in_buff);
		meta_bytes = buff2long(in_buff + FDFS_PROTO_PKG_LEN_SIZE);
		if (filename_len <= 0 || filename_len >= sizeof(filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, invalid filename length: %d", \
				__LINE__, pClientInfo->ip_addr, filename_len);
			resp.status = EINVAL;
			break;
		}

		op_flag = *(in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE);
		if (op_flag != STORAGE_SET_METADATA_FLAG_OVERWRITE && \
			op_flag != STORAGE_SET_METADATA_FLAG_MERGE)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, " \
				"invalid operation flag: 0x%02X", \
				__LINE__, pClientInfo->ip_addr, op_flag);
			resp.status = EINVAL;
			break;
		}

		if (meta_bytes < 0 || meta_bytes != nInPackLen - \
				(2 * FDFS_PROTO_PKG_LEN_SIZE + 1 + \
				FDFS_GROUP_NAME_MAX_LEN + filename_len))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, invalid meta bytes: %d", \
				__LINE__, pClientInfo->ip_addr, meta_bytes);
			resp.status = EINVAL;
			break;
		}

		memcpy(group_name, in_buff + 2*FDFS_PROTO_PKG_LEN_SIZE+1, \
			FDFS_GROUP_NAME_MAX_LEN);
		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
		if (strcmp(group_name, g_group_name) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, group_name: %s " \
				"not correct, should be: %s", \
				__LINE__, pClientInfo->ip_addr, \
				group_name, g_group_name);
			resp.status = EINVAL;
			break;
		}

		memcpy(filename, in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE + 1 + \
			FDFS_GROUP_NAME_MAX_LEN, filename_len);
		*(filename + filename_len) = '\0';
		true_filename_len = filename_len;
		if ((resp.status=storage_split_filename(filename, \
			&true_filename_len, true_filename, &pBasePath)) != 0)
		{
			break;
		}
		if ((resp.status=fdfs_check_data_filename(true_filename, \
			true_filename_len)) != 0)
		{
			break;
		}

		meta_buff = in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE + 1 + \
				FDFS_GROUP_NAME_MAX_LEN + filename_len;
		*(meta_buff + meta_bytes) = '\0';

		sprintf(full_filename, "%s/data/%s",pBasePath,true_filename);
		if (!fileExists(full_filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, filename: %s not exist", \
				__LINE__, pClientInfo->ip_addr, true_filename);
			resp.status = ENOENT;
			break;
		}

		sprintf(meta_filename,"%s"STORAGE_META_FILE_EXT,true_filename);
		strcat(full_filename, STORAGE_META_FILE_EXT);

		resp.status = storage_do_set_metadata(pClientInfo, \
			full_filename, meta_buff, meta_bytes, \
			op_flag, &sync_flag);
		if (resp.status != 0)
		{
			break;
		}

		if (sync_flag != '\0')
		{
			sprintf(meta_filename,"%s"STORAGE_META_FILE_EXT,filename);
			resp.status = storage_binlog_write( \
					time(NULL), sync_flag, meta_filename);
		}

	} while (0);

	resp.cmd = STORAGE_PROTO_CMD_RESP;

	if (in_buff != NULL)
	{
		free(in_buff);
	}

	if ((result=tcpsenddata_nb(pClientInfo->sock, (void *)&resp, \
		sizeof(resp), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

/**
IP_ADDRESS_SIZE bytes: tracker client ip address
**/
static int storage_server_report_client_ip(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader *pResp;
	char tracker_client_ip[IP_ADDRESS_SIZE];
	char out_buff[sizeof(TrackerHeader)];
	int result;

	memset(&out_buff, 0, sizeof(out_buff));
	pResp = (TrackerHeader *)out_buff;
	do
	{
		if (nInPackLen != IP_ADDRESS_SIZE)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length: %d", __LINE__, \
				STORAGE_PROTO_CMD_REPORT_CLIENT_IP, \
				pClientInfo->ip_addr,  \
				nInPackLen, IP_ADDRESS_SIZE);
			pResp->status = EINVAL;
			break;
		}

		if ((pResp->status=tcprecvdata_nb(pClientInfo->sock, \
			tracker_client_ip, nInPackLen, g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				pResp->status, strerror(pResp->status));
			break;
		}

		*(tracker_client_ip + (IP_ADDRESS_SIZE - 1)) = '\0';
		strcpy(pClientInfo->tracker_client_ip, tracker_client_ip);

		logInfo("file: "__FILE__", line: %d, " \
			"client ip: %s, tracker client ip is %s", \
			__LINE__, pClientInfo->ip_addr, \
			tracker_client_ip);
	} while (0);

	long2buff(0, pResp->pkg_len);
	pResp->cmd = STORAGE_PROTO_CMD_RESP;

	if ((result=tcpsenddata_nb(pClientInfo->sock, (void *)out_buff, \
		sizeof(TrackerHeader), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return pResp->status;
}

/**
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename
**/
static int storage_server_query_file_info(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader *pResp;
	char in_buff[128];
	char out_buff[sizeof(TrackerHeader) + 2 * FDFS_PROTO_PKG_LEN_SIZE];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char true_filename[128];
	char full_filename[MAX_PATH_SIZE + 64 + sizeof(STORAGE_META_FILE_EXT)];
	char *filename;
	char *pBasePath;
	char *p;
	struct stat file_stat;
	int filename_len;
	int true_filename_len;
	int out_len;
	int result;
	time_t file_mtime;

	memset(&out_buff, 0, sizeof(out_buff));
	pResp = (TrackerHeader *)out_buff;
	out_len = sizeof(TrackerHeader);
	do
	{
		if (nInPackLen <= FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length > %d", __LINE__, \
				STORAGE_PROTO_CMD_QUERY_FILE_INFO, \
				pClientInfo->ip_addr,  \
				nInPackLen, FDFS_PROTO_PKG_LEN_SIZE);
			pResp->status = EINVAL;
			break;
		}

		if (nInPackLen >= sizeof(in_buff))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length < %d", __LINE__, \
				STORAGE_PROTO_CMD_QUERY_FILE_INFO, \
				pClientInfo->ip_addr,  \
				nInPackLen, (int)sizeof(in_buff));
			pResp->status = EINVAL;
			break;
		}

		if ((pResp->status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
			nInPackLen, g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				pResp->status, strerror(pResp->status));
			break;
		}

		filename_len = nInPackLen - FDFS_GROUP_NAME_MAX_LEN;

		memcpy(group_name, in_buff, FDFS_GROUP_NAME_MAX_LEN);
		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
		if (strcmp(group_name, g_group_name) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, group_name: %s " \
				"not correct, should be: %s", \
				__LINE__, pClientInfo->ip_addr, \
				group_name, g_group_name);
			pResp->status = EINVAL;
			break;
		}

		filename = in_buff + FDFS_GROUP_NAME_MAX_LEN;
		*(filename + filename_len) = '\0';

		true_filename_len = filename_len;
		if ((pResp->status=storage_split_filename(filename, \
			&true_filename_len, true_filename, &pBasePath)) != 0)
		{
			break;
		}
		if ((pResp->status=fdfs_check_data_filename(true_filename, \
			true_filename_len)) != 0)
		{
			break;
		}

		sprintf(full_filename, "%s/data/%s", pBasePath, true_filename);
		if (lstat(full_filename, &file_stat) != 0)
		{
			pResp->status = errno != 0 ? errno : ENOENT;
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, call lstat file %s fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, true_filename, 
				pResp->status, strerror(pResp->status));
			break;
		}

		file_mtime = file_stat.st_mtime;
		if (S_ISLNK(file_stat.st_mode))
		{
			if (stat(full_filename, &file_stat) != 0)
			{
			pResp->status = errno != 0 ? errno : ENOENT;
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, call stat file %s fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, true_filename, 
				pResp->status, strerror(pResp->status));
			break;
			}
		}

		p = out_buff + sizeof(TrackerHeader);
		long2buff(file_stat.st_size, p);
		p += FDFS_PROTO_PKG_LEN_SIZE;
		long2buff(file_mtime, p);
		p += FDFS_PROTO_PKG_LEN_SIZE;

		out_len = p - out_buff;
	} while (0);

	long2buff(out_len - sizeof(TrackerHeader), pResp->pkg_len);
	pResp->cmd = STORAGE_PROTO_CMD_RESP;

	if ((result=tcpsenddata_nb(pClientInfo->sock, (void *)out_buff, \
		out_len, g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return pResp->status;
}

/**
1 byte: store path index
8 bytes: meta data bytes
8 bytes: file size 
FDFS_FILE_EXT_NAME_MAX_LEN bytes: file ext name, do not include dot (.)
meta data bytes: each meta data seperated by \x01,
		 name and value seperated by \x02
file size bytes: file content
**/
static int storage_upload_file(StorageClientInfo *pClientInfo, \
	GroupArray *pGroupArray, const int64_t nInPackLen, int *create_flag)
{
	TrackerHeader resp;
	int out_len;
	char in_buff[1+2*FDFS_PROTO_PKG_LEN_SIZE+FDFS_FILE_EXT_NAME_MAX_LEN+1];
	char meta_buff[4 * 1024];
	char out_buff[128];
	char filename[128];
	char *pMetaData;
	char *file_ext_name;
	int meta_bytes;
	int64_t file_bytes;
	int filename_len;
	int result;
	int store_path_index;

	memset(&resp, 0, sizeof(resp));
	pMetaData = meta_buff;
	*filename = '\0';
	filename_len = 0;
	do
	{
		if (nInPackLen < 1 + 2 * FDFS_PROTO_PKG_LEN_SIZE + 
				FDFS_FILE_EXT_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length >= %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, 1 + 2 * FDFS_PROTO_PKG_LEN_SIZE + \
				FDFS_FILE_EXT_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
			1+2*FDFS_PROTO_PKG_LEN_SIZE+FDFS_FILE_EXT_NAME_MAX_LEN,\
			g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		store_path_index = *in_buff;
		if (store_path_index < 0 || store_path_index >= g_path_count)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, store_path_index: %d " \
				"is invalid", __LINE__, \
				pClientInfo->ip_addr, store_path_index);
			resp.status = EINVAL;
			break;
		}

		meta_bytes = buff2long(in_buff + 1);
		file_bytes = buff2long(in_buff + 1 + FDFS_PROTO_PKG_LEN_SIZE);
		if (meta_bytes < 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, pkg length is not correct, " \
				"invalid meta bytes: %d", \
				__LINE__, pClientInfo->ip_addr, \
				meta_bytes);
			resp.status = EINVAL;
			break;
		}

		if (file_bytes < 0 || file_bytes != nInPackLen - \
			(1 + 2 * FDFS_PROTO_PKG_LEN_SIZE + \
			FDFS_FILE_EXT_NAME_MAX_LEN + meta_bytes))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, pkg length is not correct, " \
				"invalid file bytes: "INT64_PRINTF_FORMAT"", \
				__LINE__, pClientInfo->ip_addr, \
				file_bytes);
			resp.status = EINVAL;
			break;
		}

		file_ext_name = in_buff + 1 + 2 * FDFS_PROTO_PKG_LEN_SIZE;
		file_ext_name[FDFS_FILE_EXT_NAME_MAX_LEN] = '\0';

		if (meta_bytes > 0)
		{
			if (meta_bytes > sizeof(meta_buff))
			{
				pMetaData = (char *)malloc(meta_bytes + 1);
				if (pMetaData == NULL)
				{
					resp.status = ENOMEM;

					logError("file: "__FILE__", line: %d, " \
						"malloc %d bytes fail", \
						__LINE__, meta_bytes + 1);
					break;
				}
			}

			if ((resp.status=tcprecvdata_nb(pClientInfo->sock, \
				pMetaData, meta_bytes, g_fdfs_network_timeout)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip:%s, recv data fail, " \
					"errno: %d, error info: %s.", \
					__LINE__, pClientInfo->ip_addr, \
					resp.status, strerror(resp.status));
				break;
			}

			*(pMetaData + meta_bytes) = '\0';
		}
		else
		{
			*pMetaData = '\0';
		}

		resp.status = storage_save_file(pClientInfo, pGroupArray, \
			store_path_index, file_bytes, NULL, NULL, \
			file_ext_name, pMetaData, meta_bytes, \
			filename, &filename_len, create_flag);

		if (resp.status!=0 || (*create_flag & STORAGE_CREATE_FLAG_LINK))
		{
			break;
		}

		resp.status = storage_binlog_write(time(NULL), \
				STORAGE_OP_TYPE_SOURCE_CREATE_FILE, filename);
		if (resp.status != 0)
		{
			break;
		}

		if (meta_bytes > 0)
		{
			char meta_filename[128];
			sprintf(meta_filename, "%s"STORAGE_META_FILE_EXT, \
				filename);
			resp.status = storage_binlog_write(time(NULL), \
					STORAGE_OP_TYPE_SOURCE_CREATE_FILE, \
					meta_filename);
		}

	} while (0);

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	if (resp.status == 0)
	{
		out_len = FDFS_GROUP_NAME_MAX_LEN + filename_len;
		long2buff(out_len, resp.pkg_len);

		memcpy(out_buff, &resp, sizeof(resp));
		memcpy(out_buff+sizeof(resp), g_group_name, \
			FDFS_GROUP_NAME_MAX_LEN);
		memcpy(out_buff+sizeof(resp)+FDFS_GROUP_NAME_MAX_LEN, \
			filename, filename_len);
	}
	else
	{
		out_len = 0;
		long2buff(out_len, resp.pkg_len);
		memcpy(out_buff, &resp, sizeof(resp));
	}

	if (pMetaData != meta_buff && pMetaData != NULL)
	{
		free(pMetaData);
	}

	if ((result=tcpsenddata_nb(pClientInfo->sock, out_buff, \
		sizeof(resp) + out_len, g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

static int storage_deal_active_test(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	int result;

	memset(&resp, 0, sizeof(resp));
	do
	{
		if (nInPackLen != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length 0", __LINE__, \
				FDFS_PROTO_CMD_ACTIVE_TEST, \
				pClientInfo->ip_addr,  nInPackLen);
			resp.status = EINVAL;
			break;
		}
	} while (0);

	resp.cmd = TRACKER_PROTO_CMD_SERVER_RESP;
	if ((result=tcpsenddata_nb(pClientInfo->sock, \
		&resp, sizeof(resp), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

/**
8 bytes: master filename length
8 bytes: meta data bytes
8 bytes: file size
FDFS_FILE_PREFIX_MAX_LEN bytes  : filename prefix
FDFS_FILE_EXT_NAME_MAX_LEN bytes: file ext name, do not include dot (.)
master filename bytes: master filename
meta data bytes: each meta data seperated by \x01,
		 name and value seperated by \x02
file size bytes: file content
**/
static int storage_upload_slave_file(StorageClientInfo *pClientInfo, \
	GroupArray *pGroupArray, const int64_t nInPackLen, int *create_flag)
{
	TrackerHeader resp;
	int out_len;
	char in_buff[3*FDFS_PROTO_PKG_LEN_SIZE+FDFS_FILE_PREFIX_MAX_LEN+\
			FDFS_FILE_EXT_NAME_MAX_LEN+1];
	char master_filename[128];
	char true_filename[128];
	char prefix_name[FDFS_FILE_PREFIX_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE];
	char meta_buff[4 * 1024];
	char out_buff[128];
	char filename[128];
	char *pMetaData;
	char *file_ext_name;
	int meta_bytes;
	int master_filename_len;
	int64_t file_bytes;
	int filename_len;
	int result;
	int store_path_index;

	memset(&resp, 0, sizeof(resp));
	pMetaData = meta_buff;
	*filename = '\0';
	filename_len = 0;
	do
	{
		if (nInPackLen <= 3 * FDFS_PROTO_PKG_LEN_SIZE + \
			FDFS_FILE_PREFIX_MAX_LEN + FDFS_FILE_EXT_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length > %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, 3 * FDFS_PROTO_PKG_LEN_SIZE + \
				FDFS_FILE_PREFIX_MAX_LEN + \
				FDFS_FILE_EXT_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
			3*FDFS_PROTO_PKG_LEN_SIZE+FDFS_FILE_PREFIX_MAX_LEN + \
			FDFS_FILE_EXT_NAME_MAX_LEN, g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		master_filename_len = buff2long(in_buff);
		meta_bytes = buff2long(in_buff + FDFS_PROTO_PKG_LEN_SIZE);
		file_bytes = buff2long(in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE);
		if (master_filename_len <= FDFS_FILE_PATH_LEN || \
			master_filename_len >= sizeof(master_filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, invalid master_filename " \
				"bytes: %d", __LINE__, \
				pClientInfo->ip_addr, master_filename_len);
			resp.status = EINVAL;
			break;
		}

		if (meta_bytes < 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, invalid meta bytes: %d", \
				__LINE__, pClientInfo->ip_addr, meta_bytes);
			resp.status = EINVAL;
			break;
		}

		if (file_bytes < 0 || file_bytes != nInPackLen - \
			(3 * FDFS_PROTO_PKG_LEN_SIZE + FDFS_FILE_PREFIX_MAX_LEN
			+ FDFS_FILE_EXT_NAME_MAX_LEN + master_filename_len 
			+ meta_bytes))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, pkg length is not correct, " \
				"invalid file bytes: "INT64_PRINTF_FORMAT, \
				__LINE__, pClientInfo->ip_addr, file_bytes);
			resp.status = EINVAL;
			break;
		}

		memcpy(prefix_name, in_buff + 3 * FDFS_PROTO_PKG_LEN_SIZE, \
			FDFS_FILE_PREFIX_MAX_LEN);
		*(prefix_name + FDFS_FILE_PREFIX_MAX_LEN) = '\0';
		if (strlen(prefix_name) == 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, empty prefix name", \
				__LINE__, pClientInfo->ip_addr);
			resp.status = EINVAL;
			break;
		}

		file_ext_name = in_buff + 3 * FDFS_PROTO_PKG_LEN_SIZE + \
					FDFS_FILE_PREFIX_MAX_LEN;
		*(file_ext_name + FDFS_FILE_EXT_NAME_MAX_LEN) = '\0';

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, \
			master_filename, master_filename_len, \
			g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}
		*(master_filename + master_filename_len) = '\0';

		filename_len = master_filename_len;
		if ((resp.status=storage_split_filename_ex(master_filename, \
			&filename_len, true_filename, \
			&store_path_index)) != 0)
		{
			break;
		}
		if ((resp.status=fdfs_check_data_filename(true_filename, \
			filename_len)) != 0)
		{
			break;
		}

		snprintf(full_filename, sizeof(full_filename), \
			"%s/data/%s", g_store_paths[store_path_index], \
			true_filename);
		if (!fileExists(full_filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, master file: %s " \
				"not exist", __LINE__, \
				pClientInfo->ip_addr, full_filename);
			resp.status = ENOENT;
			break;
		}
	
		if ((resp.status=fdfs_gen_slave_filename(true_filename, \
                	prefix_name, file_ext_name, \
                	filename, &filename_len)) != 0)
		{
			break;
		}

		snprintf(full_filename, sizeof(full_filename), \
			"%s/data/%s", g_store_paths[store_path_index], \
			filename);
		if (fileExists(full_filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, slave file: %s " \
				"already exist", __LINE__, \
				pClientInfo->ip_addr, full_filename);
			resp.status = EEXIST;
			break;
		}
	
		if (meta_bytes > 0)
		{
			if (meta_bytes > sizeof(meta_buff))
			{
				pMetaData = (char *)malloc(meta_bytes + 1);
				if (pMetaData == NULL)
				{
					resp.status = ENOMEM;

					logError("file: "__FILE__", line: %d, " \
						"malloc %d bytes fail", \
						__LINE__, meta_bytes + 1);
					break;
				}
			}

			if ((resp.status=tcprecvdata_nb(pClientInfo->sock, \
				pMetaData, meta_bytes, g_fdfs_network_timeout)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip:%s, recv data fail, " \
					"errno: %d, error info: %s.", \
					__LINE__, pClientInfo->ip_addr, \
					resp.status, strerror(resp.status));
				break;
			}

			*(pMetaData + meta_bytes) = '\0';
		}
		else
		{
			*pMetaData = '\0';
		}

		resp.status = storage_save_file(pClientInfo, pGroupArray, \
			store_path_index, file_bytes, master_filename, \
			prefix_name, file_ext_name, pMetaData, meta_bytes, \
			filename, &filename_len, create_flag);

		if (resp.status!=0 || (*create_flag & STORAGE_CREATE_FLAG_LINK))
		{
			break;
		}

		resp.status = storage_binlog_write(time(NULL), \
				STORAGE_OP_TYPE_SOURCE_CREATE_FILE, filename);
		if (resp.status != 0)
		{
			break;
		}

		if (meta_bytes > 0)
		{
			char meta_filename[128];
			sprintf(meta_filename, "%s"STORAGE_META_FILE_EXT, \
				filename);
			resp.status = storage_binlog_write(time(NULL), \
					STORAGE_OP_TYPE_SOURCE_CREATE_FILE, \
					meta_filename);
		}
	} while (0);

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	if (resp.status == 0)
	{
		out_len = FDFS_GROUP_NAME_MAX_LEN + filename_len;
		long2buff(out_len, resp.pkg_len);

		memcpy(out_buff, &resp, sizeof(resp));
		memcpy(out_buff+sizeof(resp), g_group_name, \
			FDFS_GROUP_NAME_MAX_LEN);
		memcpy(out_buff+sizeof(resp)+FDFS_GROUP_NAME_MAX_LEN, \
			filename, filename_len);
	}
	else
	{
		out_len = 0;
		long2buff(out_len, resp.pkg_len);
		memcpy(out_buff, &resp, sizeof(resp));
	}

	if (pMetaData != meta_buff && pMetaData != NULL)
	{
		free(pMetaData);
	}

	if ((result=tcpsenddata_nb(pClientInfo->sock, out_buff, \
		sizeof(resp) + out_len, g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

/**
8 bytes: filename bytes
8 bytes: file size
4 bytes: source op timestamp
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename bytes : filename
file size bytes: file content
**/
static int storage_sync_copy_file(StorageClientInfo *pClientInfo, \
		const int64_t nInPackLen, const char proto_cmd, int *timestamp)
{
	TrackerHeader resp;
	char in_buff[2 * FDFS_PROTO_PKG_LEN_SIZE + \
			4 + FDFS_GROUP_NAME_MAX_LEN + 1];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char true_filename[128];
	char filename[128];
	char full_filename[MAX_PATH_SIZE];
	char *pBasePath;
	int filename_len;
	int64_t file_bytes;
	int result;

	memset(&resp, 0, sizeof(resp));
	do
	{
		if (nInPackLen <= 2 * FDFS_PROTO_PKG_LEN_SIZE + \
					4 + FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT"is not correct, " \
				"expect length > %d", \
				__LINE__, \
				proto_cmd, \
				pClientInfo->ip_addr,  nInPackLen, \
				2 * FDFS_PROTO_PKG_LEN_SIZE + \
					4 + FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
			2*FDFS_PROTO_PKG_LEN_SIZE+4+FDFS_GROUP_NAME_MAX_LEN, \
			g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv data fail, " \
				"expect pkg length: "INT64_PRINTF_FORMAT", " \
				"errno: %d, error info: %s", \
				__LINE__, \
				pClientInfo->ip_addr, nInPackLen, \
				resp.status, strerror(resp.status));
			break;
		}

		filename_len = buff2long(in_buff);
		file_bytes = buff2long(in_buff + FDFS_PROTO_PKG_LEN_SIZE);
		if (filename_len < 0 || filename_len >= sizeof(filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, in request pkg, " \
				"filename length: %d is invalid, " \
				"which < 0 or >= %d", \
				__LINE__, pClientInfo->ip_addr, \
				filename_len,  (int)sizeof(filename));
			resp.status = EPIPE;
			break;
		}

		if (file_bytes < 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, in request pkg, " \
				"file size: "INT64_PRINTF_FORMAT" is invalid, "\
				"which < 0", __LINE__, \
				pClientInfo->ip_addr, file_bytes);
			resp.status = EPIPE;
			break;
		}

		*timestamp = buff2int(in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE);
		memcpy(group_name, in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE + 4, \
				FDFS_GROUP_NAME_MAX_LEN);
		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
		if (strcmp(group_name, g_group_name) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, group_name: %s " \
				"not correct, should be: %s", \
				__LINE__, pClientInfo->ip_addr, \
				group_name, g_group_name);
			resp.status = EINVAL;
			break;
		}

		if (file_bytes != nInPackLen - (2*FDFS_PROTO_PKG_LEN_SIZE + \
				4 + FDFS_GROUP_NAME_MAX_LEN + filename_len))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, in request pkg, " \
				"file size: "INT64_PRINTF_FORMAT \
				" != remain bytes: "INT64_PRINTF_FORMAT"", \
				__LINE__, pClientInfo->ip_addr, file_bytes, \
				nInPackLen - (2*FDFS_PROTO_PKG_LEN_SIZE + \
				FDFS_GROUP_NAME_MAX_LEN + filename_len));
			resp.status = EPIPE;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, filename, \
			filename_len, g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv filename fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}
		*(filename + filename_len) = '\0';
		if ((resp.status=storage_split_filename(filename, \
			&filename_len, true_filename, &pBasePath)) != 0)
		{
			break;
		}
		if ((resp.status=fdfs_check_data_filename(true_filename, \
			filename_len)) != 0)
		{
			break;
		}

		snprintf(full_filename, sizeof(full_filename), \
				"%s/data/%s", pBasePath, true_filename);

		if ((proto_cmd == STORAGE_PROTO_CMD_SYNC_CREATE_FILE) && \
			fileExists(full_filename))
		{
			logWarning("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, data file: %s " \
				"already exists, ignore it", \
				__LINE__, proto_cmd, \
				pClientInfo->ip_addr, full_filename);

			if ((resp.status=tcpdiscard(pClientInfo->sock, \
					file_bytes, g_fdfs_network_timeout)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, discard buff fail, " \
					"buff size: "INT64_PRINTF_FORMAT", " \
					"errno: %d, error info: %s.", \
					__LINE__, pClientInfo->ip_addr, \
					file_bytes, \
					resp.status, strerror(resp.status));
				break;
			}
		}
		else if ((resp.status=tcprecvfile(pClientInfo->sock, 
				full_filename, file_bytes, \
				g_fsync_after_written_bytes, \
				g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv file buff fail, " \
				"file size: "INT64_PRINTF_FORMAT \
				", errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				file_bytes, resp.status, strerror(resp.status));
			break;
		}

		if (proto_cmd == STORAGE_PROTO_CMD_SYNC_CREATE_FILE)
		{
			resp.status = storage_binlog_write(*timestamp, \
				STORAGE_OP_TYPE_REPLICA_CREATE_FILE, \
				filename);
		}
		else
		{
			resp.status = storage_binlog_write(*timestamp, \
				STORAGE_OP_TYPE_REPLICA_UPDATE_FILE, \
				filename);
		}

	} while (0);

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	if ((result=tcpsenddata_nb(pClientInfo->sock, \
		&resp, sizeof(resp), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

/**
8 bytes: dest(link) filename length
8 bytes: source filename length
4 bytes: source op timestamp
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
dest filename length: dest filename
source filename length: source filename
**/
static int storage_sync_link_file(StorageClientInfo *pClientInfo, \
		const int64_t nInPackLen, int *timestamp)
{
	TrackerHeader resp;
	char in_buff[2 * FDFS_PROTO_PKG_LEN_SIZE + \
			4 + FDFS_GROUP_NAME_MAX_LEN + 128];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char dest_filename[128];
	char src_filename[128];
	char dest_true_filename[128];
	char src_true_filename[128];
	char dest_full_filename[MAX_PATH_SIZE];
	char src_full_filename[MAX_PATH_SIZE];
	char *pDestBasePath;
	char *pSrcBasePath;
	int dest_filename_len;
	int src_filename_len;
	int result;

	memset(&resp, 0, sizeof(resp));
	do
	{
		if (nInPackLen <= 2 * FDFS_PROTO_PKG_LEN_SIZE + \
					4 + FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, package size " \
				INT64_PRINTF_FORMAT"is not correct, " \
				"expect length > %d", \
				__LINE__, \
				pClientInfo->ip_addr,  nInPackLen, \
				2 * FDFS_PROTO_PKG_LEN_SIZE + \
					4 + FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}
		if (nInPackLen > sizeof(in_buff))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, package size " \
				INT64_PRINTF_FORMAT"is not correct, " \
				"expect length <= %d", \
				__LINE__, \
				pClientInfo->ip_addr, nInPackLen, \
				(int)sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
			nInPackLen, g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv data fail, " \
				"expect pkg length: "INT64_PRINTF_FORMAT", " \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, nInPackLen, \
				resp.status, strerror(resp.status));
			break;
		}

		dest_filename_len = buff2long(in_buff);
		src_filename_len = buff2long(in_buff + FDFS_PROTO_PKG_LEN_SIZE);
		if (dest_filename_len < 0 || \
			dest_filename_len >= sizeof(dest_filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, in request pkg, " \
				"filename length: %d is invalid, " \
				"which < 0 or >= %d", \
				__LINE__, pClientInfo->ip_addr, \
				dest_filename_len, (int)sizeof(dest_filename));
			resp.status = EINVAL;
			break;
		}
		if (src_filename_len < 0 || \
			src_filename_len >= sizeof(src_filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, in request pkg, " \
				"filename length: %d is invalid, " \
				"which < 0 or >= %d", \
				__LINE__, pClientInfo->ip_addr, \
				src_filename_len, (int)sizeof(src_filename));
			resp.status = EINVAL;
			break;
		}

		*timestamp = buff2int(in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE);
		memcpy(group_name, in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE + 4, \
				FDFS_GROUP_NAME_MAX_LEN);
		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
		if (strcmp(group_name, g_group_name) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, group_name: %s " \
				"not correct, should be: %s", \
				__LINE__, pClientInfo->ip_addr, \
				group_name, g_group_name);
			resp.status = EINVAL;
			break;
		}

		if (nInPackLen != 2 * FDFS_PROTO_PKG_LEN_SIZE + \
			4 + FDFS_GROUP_NAME_MAX_LEN + dest_filename_len + \
			src_filename_len)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, in request pkg, " \
				"pgk length: "INT64_PRINTF_FORMAT \
				" != bytes: %d", \
				__LINE__, pClientInfo->ip_addr, \
				nInPackLen, 2 * FDFS_PROTO_PKG_LEN_SIZE + \
				FDFS_GROUP_NAME_MAX_LEN + dest_filename_len + \
				src_filename_len);
			resp.status = EINVAL;
			break;
		}

		memcpy(dest_filename, in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE + \
			4 + FDFS_GROUP_NAME_MAX_LEN, dest_filename_len);
		*(dest_filename + dest_filename_len) = '\0';

		memcpy(src_filename, in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE + \
			4 + FDFS_GROUP_NAME_MAX_LEN + dest_filename_len, \
			src_filename_len);
		*(src_filename + src_filename_len) = '\0';

		if ((resp.status=storage_split_filename(dest_filename, \
			&dest_filename_len, dest_true_filename, \
			&pDestBasePath)) != 0)
		{
			break;
		}
		if ((resp.status=fdfs_check_data_filename(dest_true_filename, \
			dest_filename_len)) != 0)
		{
			break;
		}
		snprintf(dest_full_filename, sizeof(dest_full_filename), \
			"%s/data/%s", pDestBasePath, dest_true_filename);

		if ((resp.status=storage_split_filename(src_filename, \
			&src_filename_len, src_true_filename, \
			&pSrcBasePath)) != 0)
		{
			break;
		}
		if ((resp.status=fdfs_check_data_filename(src_true_filename, \
			src_filename_len)) != 0)
		{
			break;
		}
		snprintf(src_full_filename, sizeof(src_full_filename), \
			"%s/data/%s", pSrcBasePath, src_true_filename);

		if (fileExists(dest_full_filename))
		{
			logWarning("file: "__FILE__", line: %d, " \
				"client ip: %s, data file: %s " \
				"already exists, ignore it", \
				__LINE__, \
				pClientInfo->ip_addr, dest_full_filename);
		}
		else if (!fileExists(src_full_filename))
		{
			logWarning("file: "__FILE__", line: %d, " \
				"client ip: %s, source data file: %s " \
				"not exists, ignore it", \
				__LINE__, \
				pClientInfo->ip_addr, src_full_filename);
		}
		else if (symlink(src_full_filename, dest_full_filename) != 0)
		{
			resp.status = errno != 0 ? errno : EPERM;
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, link file %s to %s fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, \
				src_full_filename, dest_full_filename, \
				resp.status, strerror(resp.status));
			break;
		}

		resp.status = storage_binlog_write(*timestamp, \
			STORAGE_OP_TYPE_REPLICA_CREATE_LINK, \
			dest_filename);

	} while (0);

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	if ((result=tcpsenddata_nb(pClientInfo->sock, \
		&resp, sizeof(resp), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

/**
pkg format:
Header
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename
**/
static int storage_server_get_metadata(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	int result;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 128];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)+64];
	char true_filename[128];
	char *filename;
	char *pBasePath;
	char *file_buff;
	int filename_len;
	int64_t file_bytes;

	memset(&resp, 0, sizeof(resp));
	file_buff = NULL;
	file_bytes = 0;
	do
	{
		if (nInPackLen <= FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length > %d", __LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if (nInPackLen >= sizeof(in_buff))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is too large, " \
				"expect length should < %d", __LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, (int)sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
			nInPackLen, g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		memcpy(group_name, in_buff, FDFS_GROUP_NAME_MAX_LEN);
		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
		if (strcmp(group_name, g_group_name) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, group_name: %s " \
				"not correct, should be: %s", \
				__LINE__, pClientInfo->ip_addr, \
				group_name, g_group_name);
			resp.status = EINVAL;
			break;
		}

		*(in_buff + nInPackLen) = '\0';
		filename = in_buff + FDFS_GROUP_NAME_MAX_LEN;
		filename_len = nInPackLen - FDFS_GROUP_NAME_MAX_LEN;
		if ((resp.status=storage_split_filename(filename, \
			&filename_len, true_filename, &pBasePath)) != 0)
		{
			break;
		}
		if ((resp.status=fdfs_check_data_filename(true_filename, \
			filename_len)) != 0)
		{
			break;
		}

		sprintf(full_filename, "%s/data/%s", pBasePath, true_filename);
		if (!fileExists(full_filename))
		{
			resp.status = ENOENT;
			break;
		}

		strcat(full_filename, STORAGE_META_FILE_EXT);
		resp.status = getFileContent(full_filename, \
				&file_buff, &file_bytes);
		if (resp.status == ENOENT)
		{
			resp.status = 0;
		}
	} while (0);

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	long2buff(file_bytes, resp.pkg_len);
	if ((result=tcpsenddata_nb(pClientInfo->sock, \
		&resp, sizeof(resp), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));

		if (file_buff != NULL)
		{
			free(file_buff);
		}
		return result;
	}

	if (resp.status != 0)
	{
		if (file_buff != NULL)
		{
			free(file_buff);
		}

		return resp.status;
	}


	if (file_buff != NULL)
	{
		result = tcpsenddata_nb(pClientInfo->sock, \
			file_buff, file_bytes, g_fdfs_network_timeout);
		free(file_buff);
		if(result != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, send data fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, \
				result, strerror(result));

			return result;
		}
	}

	return resp.status;
}

/**
pkg format:
Header
8 bytes: file offset
8 bytes: download file bytes
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename
**/
static int storage_server_download_file(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	int result;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 128];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)+16];
	char true_filename[128];
	char *pBasePath;
	char *filename;
	int filename_len;
	int64_t file_offset;
	int64_t download_bytes;
	int64_t file_bytes;
	struct stat stat_buf;

	memset(&resp, 0, sizeof(resp));
	file_offset = 0;
	download_bytes = 0;
	do
	{
		if (nInPackLen <= 16 + FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length > %d", __LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, 16 + FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if (nInPackLen >= sizeof(in_buff))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is too large, " \
				"expect length should < %d", __LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, (int)sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
				nInPackLen, g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		file_offset = buff2long(in_buff);
		download_bytes = buff2long(in_buff + 8);
		if (file_offset < 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, invalid file offset: " \
				INT64_PRINTF_FORMAT,  __LINE__, \
				pClientInfo->ip_addr, file_offset);
			resp.status = EINVAL;
			break;
		}
		if (download_bytes < 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, invalid download file bytes: " \
				INT64_PRINTF_FORMAT,  __LINE__, \
				pClientInfo->ip_addr, download_bytes);
			resp.status = EINVAL;
			break;
		}

		memcpy(group_name, in_buff+16, FDFS_GROUP_NAME_MAX_LEN);
		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
		if (strcmp(group_name, g_group_name) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, group_name: %s " \
				"not correct, should be: %s", \
				__LINE__, pClientInfo->ip_addr, \
				group_name, g_group_name);
			resp.status = EINVAL;
			break;
		}

		*(in_buff + nInPackLen) = '\0';
		filename = in_buff + 16 + FDFS_GROUP_NAME_MAX_LEN;
		filename_len = nInPackLen - (16 + FDFS_GROUP_NAME_MAX_LEN);

		if ((resp.status=storage_split_filename(filename, \
			&filename_len, true_filename, &pBasePath)) != 0)
		{
			break;
		}

		if ((resp.status=fdfs_check_data_filename(true_filename, \
			filename_len)) != 0)
		{
			break;
		}

		sprintf(full_filename, "%s/data/%s", pBasePath, true_filename);
		if (stat(full_filename, &stat_buf) == 0)
		{
			if (!S_ISREG(stat_buf.st_mode))
			{
				logError("file: "__FILE__", line: %d, " \
					"%s is not a regular file", \
					__LINE__, full_filename);
				resp.status = EISDIR;
				break;
			}

			resp.status = 0;
			file_bytes = stat_buf.st_size;
		}
		else
		{
			file_bytes = 0;
			resp.status = errno != 0 ? errno : ENOENT;

			logError("file: "__FILE__", line: %d, " \
				"call stat fail, file: %s, "\
				"error no: %d, error info: %s", \
				__LINE__, full_filename, \
				errno, strerror(errno));
			break;
		}

		if (download_bytes == 0)
		{
			download_bytes  = file_bytes - file_offset;
		}
		else if (download_bytes > file_bytes - file_offset)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, invalid download file bytes: " \
				INT64_PRINTF_FORMAT" > file remain bytes: " \
				INT64_PRINTF_FORMAT,  __LINE__, \
				pClientInfo->ip_addr, download_bytes, \
				file_bytes - file_offset);
			resp.status = EINVAL;
			break;
		}
	} while (0);

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	if (resp.status == 0)
	{
		long2buff(download_bytes, resp.pkg_len);
	}
	if ((result=tcpsenddata_nb(pClientInfo->sock, \
		&resp, sizeof(resp), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	if (resp.status != 0)
	{
		return resp.status;
	}

	result = tcpsendfile_ex(pClientInfo->sock, full_filename, \
			file_offset, download_bytes, g_fdfs_network_timeout);
	if(result != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send file fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));

		return result;
	}

	return resp.status;
}

/**
pkg format:
Header
4 bytes: source delete timestamp
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename
**/
static int storage_sync_delete_file(StorageClientInfo *pClientInfo, \
		const int64_t nInPackLen, int *timestamp)
{
	TrackerHeader resp;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 128];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)];
	char true_filename[128];
	char *pBasePath;
	char *filename;
	int filename_len;
	int result;

	memset(&resp, 0, sizeof(resp));
	do
	{
		if (nInPackLen <= 4 + FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length <= %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_SYNC_DELETE_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, 4 + FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if (nInPackLen >= sizeof(in_buff))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is too large, " \
				"expect length should < %d", __LINE__, \
				STORAGE_PROTO_CMD_SYNC_DELETE_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, (int)sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
			nInPackLen, g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		*timestamp = buff2int(in_buff);
		memcpy(group_name, in_buff + 4, FDFS_GROUP_NAME_MAX_LEN);
		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
		if (strcmp(group_name, g_group_name) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, group_name: %s " \
				"not correct, should be: %s", \
				__LINE__, pClientInfo->ip_addr, \
				group_name, g_group_name);
			resp.status = EINVAL;
			break;
		}

		*(in_buff + nInPackLen) = '\0';
		filename = in_buff + 4 + FDFS_GROUP_NAME_MAX_LEN;
		filename_len = nInPackLen - (4 + FDFS_GROUP_NAME_MAX_LEN);
		if ((resp.status=storage_split_filename(filename, \
			&filename_len, true_filename, &pBasePath)) != 0)
		{
			break;
		}
		if ((resp.status=fdfs_check_data_filename(true_filename, \
			filename_len)) != 0)
		{
			break;
		}

		sprintf(full_filename, "%s/data/%s", pBasePath, true_filename);
		if (unlink(full_filename) != 0)
		{
			if (errno == ENOENT)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"cmd=%d, client ip: %s, " \
					"file %s not exist, " \
					"maybe delete later?", \
					__LINE__, \
					STORAGE_PROTO_CMD_SYNC_DELETE_FILE, \
					pClientInfo->ip_addr, full_filename);
			}
			else
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, delete file %s fail," \
					"errno: %d, error info: %s", \
					__LINE__, pClientInfo->ip_addr, \
					full_filename, errno, strerror(errno));
				resp.status = errno != 0 ? errno : EACCES;
				break;
			}
		}

		resp.status = storage_binlog_write(*timestamp, \
				STORAGE_OP_TYPE_REPLICA_DELETE_FILE, filename);
	} while (0);

	resp.cmd = STORAGE_PROTO_CMD_RESP;

	if ((result=tcpsenddata_nb(pClientInfo->sock, \
		&resp, sizeof(resp), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));

		return result;
	}

	return resp.status;
}

/**
pkg format:
Header
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename
**/
static int storage_server_delete_file(StorageClientInfo *pClientInfo, \
	GroupArray *pGroupArray, const int64_t nInPackLen, int *delete_flag)
{
	TrackerHeader resp;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 128];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)];
	char meta_filename[MAX_PATH_SIZE+sizeof(in_buff)];
	char true_filename[128];
	char value[128];
	FDHTKeyInfo key_info_fid;
	FDHTKeyInfo key_info_ref;
	FDHTKeyInfo key_info_sig;
	char *pValue;
	int value_len;
	char *pBasePath;
	char *filename;
	int filename_len;
	int src_file_nlink;
	struct stat stat_buf;
	int result;

	*delete_flag = STORAGE_DELETE_FLAG_NONE;
	memset(&resp, 0, sizeof(resp));
	do
	{
		if (nInPackLen <= FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length <= %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if (nInPackLen >= sizeof(in_buff))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is too large, " \
				"expect length should < %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, (int)sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
			nInPackLen, g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		memcpy(group_name, in_buff, FDFS_GROUP_NAME_MAX_LEN);
		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
		if (strcmp(group_name, g_group_name) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, group_name: %s " \
				"not correct, should be: %s", \
				__LINE__, pClientInfo->ip_addr, \
				group_name, g_group_name);
			resp.status = EINVAL;
			break;
		}

		*(in_buff + nInPackLen) = '\0';
		filename = in_buff + FDFS_GROUP_NAME_MAX_LEN;
		filename_len = nInPackLen - FDFS_GROUP_NAME_MAX_LEN;

		src_file_nlink = -1;
		if (g_check_file_duplicate)
		{
			memset(&key_info_sig, 0, sizeof(key_info_sig));
			key_info_sig.namespace_len = g_namespace_len;
			memcpy(key_info_sig.szNameSpace, g_key_namespace, \
				g_namespace_len);
			key_info_sig.obj_id_len = snprintf(\
				key_info_sig.szObjectId, \
				sizeof(key_info_sig.szObjectId), "%s/%s", \
				group_name, filename);

			key_info_sig.key_len = sizeof(FDHT_KEY_NAME_FILE_SIG)-1;
			memcpy(key_info_sig.szKey, FDHT_KEY_NAME_FILE_SIG, \
				key_info_sig.key_len);
			pValue = value;
			value_len = sizeof(value) - 1;
			result = fdht_get_ex1(pGroupArray, g_keep_alive, \
					&key_info_sig, FDHT_EXPIRES_NONE, \
					&pValue, &value_len, malloc);
			if (result == 0)
			{
				memcpy(&key_info_fid, &key_info_sig, \
					sizeof(FDHTKeyInfo));
				key_info_fid.obj_id_len = value_len;
				memcpy(key_info_fid.szObjectId, pValue, \
					value_len);

				key_info_fid.key_len = \
					sizeof(FDHT_KEY_NAME_FILE_ID) - 1;
				memcpy(key_info_fid.szKey, \
					FDHT_KEY_NAME_FILE_ID, \
					key_info_fid.key_len);
				value_len = sizeof(value) - 1;
				result = fdht_get_ex1(pGroupArray, \
					g_keep_alive, &key_info_fid, \
					FDHT_EXPIRES_NONE, &pValue, \
					&value_len, malloc);
				if (result == 0)
				{
				memcpy(&key_info_ref, &key_info_sig, \
					sizeof(FDHTKeyInfo));
				key_info_ref.obj_id_len = value_len;
				memcpy(key_info_ref.szObjectId, pValue, \
					value_len);
				key_info_ref.key_len = \
					sizeof(FDHT_KEY_NAME_REF_COUNT)-1;
				memcpy(key_info_ref.szKey, \
					FDHT_KEY_NAME_REF_COUNT, \
					key_info_ref.key_len);
				value_len = sizeof(value) - 1;

				result = fdht_get_ex1(pGroupArray, \
					g_keep_alive, &key_info_ref, \
					FDHT_EXPIRES_NONE, &pValue, \
					&value_len, malloc);
				if (result == 0)
				{
					*(pValue + value_len) = '\0';
					src_file_nlink = atoi(pValue);
				}
				else if (result != ENOENT)
				{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, fdht_get fail," \
					"errno: %d, error info: %s", \
					__LINE__, pClientInfo->ip_addr, \
					result, strerror(result));
				resp.status = result;
				break;
				}
				}
				else if (result != ENOENT)
				{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, fdht_get fail," \
					"errno: %d, error info: %s", \
					__LINE__, pClientInfo->ip_addr, \
					result, strerror(result));
				resp.status = result;
				break;
				}
			}
			else if (result != ENOENT)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, fdht_get fail," \
					"errno: %d, error info: %s", \
					__LINE__, pClientInfo->ip_addr, \
					result, strerror(result));
				resp.status = result;
				break;
			}
		}

		if ((resp.status=storage_split_filename(filename, \
			&filename_len, true_filename, &pBasePath)) != 0)
		{
			break;
		}
		if ((resp.status=fdfs_check_data_filename(true_filename, \
			filename_len)) != 0)
		{
			break;
		}

		sprintf(full_filename, "%s/data/%s", pBasePath, true_filename);
		if (lstat(full_filename, &stat_buf) != 0)
		{
			resp.status = errno != 0 ? errno : ENOENT;
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, stat file %s fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				full_filename, \
				resp.status, strerror(resp.status));
			break;
		}
		if (S_ISREG(stat_buf.st_mode))
		{
			*delete_flag |= STORAGE_DELETE_FLAG_FILE;
		}
		else
		{
			*delete_flag |= STORAGE_DELETE_FLAG_LINK;
		}

		if (unlink(full_filename) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, delete file %s fail," \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, full_filename, \
				errno, strerror(errno));
			resp.status = errno != 0 ? errno : EACCES;
			break;
		}

		resp.status = storage_binlog_write(time(NULL), \
				STORAGE_OP_TYPE_SOURCE_DELETE_FILE, filename);
		if (resp.status != 0)
		{
			break;
		}

		sprintf(meta_filename, "%s"STORAGE_META_FILE_EXT, \
				full_filename);
		if (unlink(meta_filename) != 0)
		{
			if (errno != ENOENT)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, delete file %s fail," \
					"errno: %d, error info: %s", \
					__LINE__, \
					pClientInfo->ip_addr, meta_filename, \
					errno, strerror(errno));
				resp.status = errno != 0 ? errno : EACCES;
				break;
			}

			break;  //meta file do not exist, do not log to binlog
		}

		sprintf(meta_filename, "%s"STORAGE_META_FILE_EXT, filename);
		resp.status = storage_binlog_write(time(NULL), \
			STORAGE_OP_TYPE_SOURCE_DELETE_FILE, meta_filename);

		if (resp.status != 0 || src_file_nlink < 0)
		{
			break;
		}

		if (g_check_file_duplicate)
		{
			char *pSeperator;

			if ((result=fdht_delete_ex(pGroupArray, g_keep_alive, \
					&key_info_sig)) != 0)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"client ip: %s, fdht_delete fail," \
					"errno: %d, error info: %s", \
					__LINE__, pClientInfo->ip_addr, \
					result, strerror(result));
			}

			value_len = sizeof(value) - 1;
			result = fdht_inc_ex(pGroupArray, g_keep_alive, \
				&key_info_ref, FDHT_EXPIRES_NEVER, -1, \
				value, &value_len);
			if (result != 0)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"client ip: %s, fdht_inc fail," \
					"errno: %d, error info: %s", \
					__LINE__, pClientInfo->ip_addr, \
					result, strerror(result));
				break;
			}

			if (!(value_len == 1 && *value == '0')) //value == 0
			{
				break;
			}

			if ((result=fdht_delete_ex(pGroupArray, g_keep_alive, \
					&key_info_fid)) != 0)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"client ip: %s, fdht_delete fail," \
					"errno: %d, error info: %s", \
					__LINE__, pClientInfo->ip_addr, \
					result, strerror(result));
			}
			if ((result=fdht_delete_ex(pGroupArray, g_keep_alive, \
					&key_info_ref)) != 0)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"client ip: %s, fdht_delete fail," \
					"errno: %d, error info: %s", \
					__LINE__, pClientInfo->ip_addr, \
					result, strerror(result));
			}

			*(key_info_ref.szObjectId+key_info_ref.obj_id_len)='\0';
			pSeperator = strchr(key_info_ref.szObjectId, '/');
			if (pSeperator == NULL)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"invalid file_id: %s", __LINE__, \
					key_info_ref.szObjectId);
				break;
			}

			pSeperator++;
			value_len = key_info_ref.obj_id_len - (pSeperator - \
					key_info_ref.szObjectId);
			memcpy(value, pSeperator, value_len + 1);
			if (storage_split_filename(value, \
				&value_len, true_filename, &pBasePath) != 0)
			{
				break;
			}
			if (fdfs_check_data_filename(true_filename, \
						value_len) != 0)
			{
				break;
			}

			sprintf(full_filename, "%s/data/%s", pBasePath, \
				true_filename);
			if (unlink(full_filename) != 0)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"client ip: %s, delete source file " \
					"%s fail, errno: %d, error info: %s", \
					__LINE__, pClientInfo->ip_addr, \
					full_filename, errno, strerror(errno));
				break;
			}

			storage_binlog_write(time(NULL), \
				STORAGE_OP_TYPE_SOURCE_DELETE_FILE, value);
			*delete_flag |= STORAGE_DELETE_FLAG_FILE;
		}

	} while (0);

	resp.cmd = STORAGE_PROTO_CMD_RESP;

	if ((result=tcpsenddata_nb(pClientInfo->sock, \
		&resp, sizeof(resp), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));

		return result;
	}

	return resp.status;
}

/**
pkg format:
Header
8 bytes: master filename len
8 bytes: source filename len
8 bytes: source file signature len
8 bytes: meta data bytes
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
FDFS_FILE_PREFIX_MAX_LEN bytes  : filename prefix, can be empty
FDFS_FILE_EXT_NAME_MAX_LEN bytes: file ext name, do not include dot (.)
master filename len: master filename
source filename len: source filename
source file signature len: source file signature
meta data bytes: each meta data seperated by \x01,
		 name and value seperated by \x02
**/
static int storage_create_link(StorageClientInfo *pClientInfo, \
		GroupArray *pGroupArray, const int64_t nInPackLen)
{
	TrackerHeader resp;
	int out_len;
	char in_buff[4 * FDFS_PROTO_PKG_LEN_SIZE + FDFS_GROUP_NAME_MAX_LEN + \
		FDFS_FILE_PREFIX_MAX_LEN + FDFS_FILE_EXT_NAME_MAX_LEN + 256];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char prefix_name[FDFS_FILE_PREFIX_MAX_LEN + 1];
	char file_ext_name[FDFS_FILE_EXT_NAME_MAX_LEN + 1];
	char meta_buff[4 * 1024];
	char out_buff[128];
	char src_filename[128];
	char master_filename[128];
	char true_filename[128];
	char src_full_filename[MAX_PATH_SIZE+64];
	char full_filename[MAX_PATH_SIZE];
	char filename[128];
	int create_flag;
	char *pMetaData;
	int meta_bytes;
	int src_filename_len;
	int master_filename_len;
	int filename_len;
	int len;
	int result;
	int store_path_index;
	struct stat stat_buf;
	SourceFileInfo sourceFileInfo;

	memset(&resp, 0, sizeof(resp));
	pMetaData = meta_buff;
	*filename = '\0';
	filename_len = 0;

	do
	{
		if (nInPackLen <= 4 * FDFS_PROTO_PKG_LEN_SIZE + \
			FDFS_GROUP_NAME_MAX_LEN + FDFS_FILE_PREFIX_MAX_LEN + \
			FDFS_FILE_EXT_NAME_MAX_LEN)
		{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip: %s, package size " \
			INT64_PRINTF_FORMAT" is not correct, " \
			"expect length > %d", \
			__LINE__, STORAGE_PROTO_CMD_UPLOAD_FILE, \
			pClientInfo->ip_addr,  \
			nInPackLen, 4 * FDFS_PROTO_PKG_LEN_SIZE + \
			FDFS_GROUP_NAME_MAX_LEN + FDFS_FILE_PREFIX_MAX_LEN + \
			FDFS_FILE_EXT_NAME_MAX_LEN);

			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, in_buff, \
			4*FDFS_PROTO_PKG_LEN_SIZE+FDFS_GROUP_NAME_MAX_LEN+\
			FDFS_FILE_PREFIX_MAX_LEN+FDFS_FILE_EXT_NAME_MAX_LEN, \
			g_fdfs_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		master_filename_len = buff2long(in_buff);
		src_filename_len = buff2long(in_buff+FDFS_PROTO_PKG_LEN_SIZE);
		sourceFileInfo.src_file_sig_len = buff2long(in_buff + \
				2 * FDFS_PROTO_PKG_LEN_SIZE);
		meta_bytes = buff2long(in_buff+3*FDFS_PROTO_PKG_LEN_SIZE);
		if (master_filename_len < 0 || master_filename_len >= \
			sizeof(master_filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, pkg length is not correct, " \
				"invalid master filename length: %d", \
				__LINE__, pClientInfo->ip_addr, \
				master_filename_len);
			resp.status = EINVAL;
			break;
		}

		if (src_filename_len <= 0 || src_filename_len >= \
			sizeof(src_filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, pkg length is not correct, " \
				"invalid filename length: %d", \
				__LINE__, pClientInfo->ip_addr, \
				src_filename_len);
			resp.status = EINVAL;
			break;
		}

		if (sourceFileInfo.src_file_sig_len <= 0 || \
			sourceFileInfo.src_file_sig_len >= \
			sizeof(sourceFileInfo.src_file_sig))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, pkg length is not correct, " \
				"invalid file signature length: %d", \
				__LINE__, pClientInfo->ip_addr, \
				sourceFileInfo.src_file_sig_len);
			resp.status = EINVAL;
			break;
		}

		if (meta_bytes < 0 || meta_bytes != nInPackLen - \
			(4 * FDFS_PROTO_PKG_LEN_SIZE+FDFS_GROUP_NAME_MAX_LEN + \
			FDFS_FILE_PREFIX_MAX_LEN + FDFS_FILE_EXT_NAME_MAX_LEN +\
			master_filename_len + src_filename_len + \
			sourceFileInfo.src_file_sig_len))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, pkg length is not correct, " \
				"invalid meta bytes: %d", \
				__LINE__, pClientInfo->ip_addr, meta_bytes);
			resp.status = EINVAL;
			break;
		}

		memcpy(group_name, in_buff + 4 * FDFS_PROTO_PKG_LEN_SIZE, \
			FDFS_GROUP_NAME_MAX_LEN);
		*(group_name + FDFS_GROUP_NAME_MAX_LEN) = '\0';
		if (strcmp(group_name, g_group_name) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, group_name: %s " \
				"not correct, should be: %s", \
				__LINE__, pClientInfo->ip_addr, \
				group_name, g_group_name);
			resp.status = EINVAL;
			break;
		}

		memcpy(prefix_name, in_buff + 4 * FDFS_PROTO_PKG_LEN_SIZE + \
			FDFS_GROUP_NAME_MAX_LEN, FDFS_FILE_PREFIX_MAX_LEN);
		*(prefix_name + FDFS_FILE_PREFIX_MAX_LEN) = '\0';

		memcpy(file_ext_name, in_buff + 4 * FDFS_PROTO_PKG_LEN_SIZE + \
			FDFS_GROUP_NAME_MAX_LEN + FDFS_FILE_PREFIX_MAX_LEN, \
			FDFS_FILE_EXT_NAME_MAX_LEN);
		*(file_ext_name + FDFS_FILE_EXT_NAME_MAX_LEN) = '\0';

		len = master_filename_len + src_filename_len + \
			sourceFileInfo.src_file_sig_len;
		if (len > sizeof(in_buff))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, invalid pkg length, " \
				"file relative length: %d > %d", \
				__LINE__, pClientInfo->ip_addr, \
				len, (int)sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata_nb(pClientInfo->sock, \
			in_buff, len, g_fdfs_network_timeout))!=0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		if (master_filename_len > 0)
		{
			memcpy(master_filename, in_buff, master_filename_len);
			*(master_filename + master_filename_len) = '\0';
		}
		else
		{
			*master_filename = '\0';
		}

		memcpy(src_filename, in_buff + master_filename_len, \
			src_filename_len);
		*(src_filename + src_filename_len) = '\0';

		memcpy(sourceFileInfo.src_file_sig, in_buff + \
			master_filename_len + src_filename_len, \
			sourceFileInfo.src_file_sig_len);
		*(sourceFileInfo.src_file_sig + \
			sourceFileInfo.src_file_sig_len) = '\0';

		if ((resp.status=storage_split_filename_ex(src_filename, \
			&src_filename_len, sourceFileInfo.src_true_filename, \
			&store_path_index)) != 0)
		{
			break;
		}
		if ((resp.status=fdfs_check_data_filename( \
			sourceFileInfo.src_true_filename, \
			src_filename_len)) != 0)
		{
			break;
		}
		sprintf(src_full_filename, "%s/data/%s", \
			g_store_paths[store_path_index], \
			sourceFileInfo.src_true_filename);

		result = lstat(src_full_filename, &stat_buf);
		if (result != 0 || !S_ISREG(stat_buf.st_mode))
		{
			FDHTKeyInfo key_info;

			resp.status = errno != 0 ? errno : EINVAL;
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, file: %s call stat fail " \
				"or it is not a regular file, " \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, \
				src_full_filename, \
				resp.status, strerror(resp.status));


			if (g_check_file_duplicate)
			{
			//clean invalid entry
			memset(&key_info, 0, sizeof(key_info));
			key_info.namespace_len = g_namespace_len;
			memcpy(key_info.szNameSpace, g_key_namespace, \
				g_namespace_len);

			key_info.obj_id_len = sourceFileInfo.src_file_sig_len;
			memcpy(key_info.szObjectId, sourceFileInfo.src_file_sig,
				key_info.obj_id_len);
			key_info.key_len = sizeof(FDHT_KEY_NAME_FILE_ID) - 1;
			memcpy(key_info.szKey, FDHT_KEY_NAME_FILE_ID, \
				sizeof(FDHT_KEY_NAME_FILE_ID) - 1);
			fdht_delete_ex(pGroupArray, g_keep_alive, &key_info);

			key_info.obj_id_len = snprintf(key_info.szObjectId, \
					sizeof(src_filename), "%s/%s", \
					g_group_name, src_filename);
			key_info.key_len = sizeof(FDHT_KEY_NAME_REF_COUNT) - 1;
			memcpy(key_info.szKey, FDHT_KEY_NAME_REF_COUNT, \
					key_info.key_len);
			fdht_delete_ex(pGroupArray, g_keep_alive, &key_info);
			}

			break;
		}

		if (master_filename_len > 0 && strlen(prefix_name) > 0)
		{
			int master_store_path_index;

			filename_len = master_filename_len;
			if ((resp.status=storage_split_filename_ex( \
				master_filename, &filename_len, true_filename, \
				&master_store_path_index)) != 0)
			{
				break;
			}

			if (master_store_path_index != store_path_index)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip:%s, invalid master store " \
					"path index: %d != source store path " \
					"index: %d", __LINE__, \
					pClientInfo->ip_addr, \
					master_store_path_index, \
					store_path_index);
				resp.status = EINVAL;
				break;
			}

			if ((resp.status=fdfs_check_data_filename(true_filename, \
				filename_len)) != 0)
			{
				break;
			}

			if ((resp.status=fdfs_gen_slave_filename( \
				true_filename, \
				prefix_name, file_ext_name, \
				filename, &filename_len)) != 0)
			{
				break;
			}

			snprintf(full_filename, sizeof(full_filename), \
				"%s/data/%s", g_store_paths[store_path_index], \
				filename);
			if (fileExists(full_filename))
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip: %s, slave file: %s " \
					"already exist", __LINE__, \
					pClientInfo->ip_addr, full_filename);
				resp.status = EEXIST;
				break;
			}
		}

		if (meta_bytes > 0)
		{
			if (meta_bytes > sizeof(meta_buff))
			{
				pMetaData = (char *)malloc(meta_bytes + 1);
				if (pMetaData == NULL)
				{
					resp.status = ENOMEM;

					logError("file: "__FILE__", line: %d, "\
						"malloc %d bytes fail", \
						__LINE__, meta_bytes + 1);
					break;
				}
			}

			if ((resp.status=tcprecvdata_nb(pClientInfo->sock, \
				pMetaData, meta_bytes, g_fdfs_network_timeout)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip:%s, recv data fail, " \
					"errno: %d, error info: %s.", \
					__LINE__, pClientInfo->ip_addr, \
					resp.status, strerror(resp.status));
				break;
			}

			*(pMetaData + meta_bytes) = '\0';
		}
		else
		{
			*pMetaData = '\0';
		}

		resp.status = storage_deal_file(pClientInfo, pGroupArray, \
			store_path_index, &sourceFileInfo, stat_buf.st_size, \
			master_filename, prefix_name, file_ext_name, \
			pMetaData, meta_bytes, \
			filename, &filename_len, &create_flag);
		if (resp.status != 0)
		{
			break;
		}

		resp.status = storage_binlog_write(time(NULL), \
				STORAGE_OP_TYPE_SOURCE_CREATE_LINK, filename);
		if (resp.status != 0)
		{
			break;
		}

		if (meta_bytes > 0)
		{
			char meta_filename[128];
			sprintf(meta_filename, "%s"STORAGE_META_FILE_EXT, \
				filename);
			resp.status = storage_binlog_write(time(NULL), \
					STORAGE_OP_TYPE_SOURCE_CREATE_FILE, \
					meta_filename);
		}
	} while (0);

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	if (resp.status == 0)
	{
		out_len = FDFS_GROUP_NAME_MAX_LEN + filename_len;
		long2buff(out_len, resp.pkg_len);

		memcpy(out_buff, &resp, sizeof(resp));
		memcpy(out_buff+sizeof(resp), g_group_name, \
			FDFS_GROUP_NAME_MAX_LEN);
		memcpy(out_buff+sizeof(resp)+FDFS_GROUP_NAME_MAX_LEN, \
			filename, filename_len);
	}
	else
	{
		out_len = 0;
		long2buff(out_len, resp.pkg_len);
		memcpy(out_buff, &resp, sizeof(resp));
	}

	if (pMetaData != meta_buff && pMetaData != NULL)
	{
		free(pMetaData);
	}

	if ((result=tcpsenddata_nb(pClientInfo->sock, out_buff, \
		sizeof(resp) + out_len, g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

static FDFSStorageServer *get_storage_server(const char *ip_addr)
{
	FDFSStorageServer targetServer;
	FDFSStorageServer *pTargetServer;
	FDFSStorageServer **ppFound;

	memset(&targetServer, 0, sizeof(targetServer));
	strcpy(targetServer.server.ip_addr, ip_addr);

	pTargetServer = &targetServer;
	ppFound = (FDFSStorageServer **)bsearch(&pTargetServer, \
		g_sorted_storages, g_storage_count, \
		sizeof(FDFSStorageServer *), storage_cmp_by_ip_addr);
	if (ppFound == NULL)
	{
		return NULL;
	}
	else
	{
		return *ppFound;
	}
}

#define CHECK_AND_WRITE_TO_STAT_FILE1  \
		pthread_mutex_lock(&stat_count_thread_lock); \
\
		if (pSrcStorage == NULL) \
		{ \
			pSrcStorage = get_storage_server(client_info.tracker_client_ip); \
		} \
		if (pSrcStorage != NULL) \
		{ \
			pSrcStorage->last_sync_src_timestamp = \
					src_sync_timestamp; \
			g_sync_change_count++; \
		} \
\
		g_storage_stat.last_sync_update = time(NULL); \
		++g_stat_change_count; \
		pthread_mutex_unlock(&stat_count_thread_lock);

#define CHECK_AND_WRITE_TO_STAT_FILE2(total_count, success_count)  \
		pthread_mutex_lock(&stat_count_thread_lock); \
		total_count++; \
		success_count++; \
		++g_stat_change_count; \
		pthread_mutex_unlock(&stat_count_thread_lock);

#define CHECK_AND_WRITE_TO_STAT_FILE3(total_count, success_count, timestamp)  \
		pthread_mutex_lock(&stat_count_thread_lock); \
		total_count++; \
		success_count++; \
		timestamp = time(NULL); \
		++g_stat_change_count;  \
		pthread_mutex_unlock(&stat_count_thread_lock);


void* storage_thread_entrance(void* arg)
{
/*
package format:
8 bytes length (hex string)
1 bytes cmd (char)
1 bytes status(char)
data buff (struct)
*/
	StorageClientInfo client_info;
	TrackerHeader header;
	int result;
	int64_t nInPackLen;
	int count;
	int recv_bytes;
	int log_level;
	in_addr_t client_ip;
	int src_sync_timestamp;
	FDFSStorageServer *pSrcStorage;
	struct sockaddr_in inaddr;
	socklen_t sockaddr_len;
	int server_sock;
	int create_flag;
	int delete_flag;
	GroupArray group_array;
	
	server_sock = (long)arg;

	if (g_check_file_duplicate)
	{
		if ((result=fdht_copy_group_array(&group_array, \
				&g_group_array)) != 0)
		{
			pthread_mutex_lock(&g_storage_thread_lock);
			g_storage_thread_count--;
			pthread_mutex_unlock(&g_storage_thread_lock);
			return NULL;
		}
	}

	while (g_continue_flag)
	{
	if ((result=pthread_mutex_lock(&g_storage_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	if (!g_continue_flag)
	{
		pthread_mutex_unlock(&g_storage_thread_lock);
		break;
	}

	memset(&client_info, 0, sizeof(client_info));
	//client_info.sock = nbaccept(server_sock, 0, &result);

	sockaddr_len = sizeof(inaddr);
	client_info.sock = accept(server_sock, (struct sockaddr*)&inaddr, \
				&sockaddr_len);
	if (pthread_mutex_unlock(&g_storage_thread_lock) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail", \
			__LINE__);
	}

	if(client_info.sock < 0) //error
	{
		result = errno != 0 ? errno : EINTR;
		if (result == ETIMEDOUT || result == EAGAIN)
		{
			continue;
		}

		if(result == EBADF)
		{
			logError("file: "__FILE__", line: %d, " \
				"accept failed, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			break;
		}
			
		logError("file: "__FILE__", line: %d, " \
			"accept failed, errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		continue;
	}

	if (tcpsetnonblockopt(client_info.sock) != 0)
	{
		close(client_info.sock);
		continue;
	}

	client_ip = getPeerIpaddr(client_info.sock, \
				client_info.ip_addr, IP_ADDRESS_SIZE);
	if (g_allow_ip_count >= 0)
	{
		if (bsearch(&client_ip, g_allow_ip_addrs, g_allow_ip_count, \
			sizeof(in_addr_t), cmp_by_ip_addr_t) == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"ip addr %s is not allowed to access", \
				__LINE__, client_info.ip_addr);

			close(client_info.sock);
			continue;
		}
	}

	strcpy(client_info.tracker_client_ip, client_info.ip_addr);
	pSrcStorage = NULL;
	count = 0;
	while (g_continue_flag)
	{
		result = tcprecvdata_nb_ex(client_info.sock, &header, \
			sizeof(header), g_fdfs_network_timeout, &recv_bytes);
		if (result == ETIMEDOUT)
		{
			continue;
		}

		if (result != 0)
		{
			/*
			unsigned char *p;
			unsigned char *pEnd = (unsigned char *)&header + recv_bytes);
			for (p=(unsigned char *)&header; p<pEnd; p++)
			{
				fprintf(stderr, "%02X ", *p);
			}
			fprintf(stderr, "\n");
			*/

			if (result == ENOTCONN && recv_bytes == 0)
			{
				log_level = LOG_DEBUG;
			}
			else
			{
				log_level = LOG_ERR;
			}

			if (log_level <= g_log_context.log_level)
			{
			log_it_ex(&g_log_context, log_level, \
				"file: "__FILE__", line: %d, " \
				"client ip: %s, recv data fail, " \
				"errno: %d, error info: %s", \
				__LINE__, client_info.ip_addr, \
				result, strerror(result));
			}

			break;
		}

		nInPackLen = buff2long(header.pkg_len);

		switch (header.cmd)
		{
		case STORAGE_PROTO_CMD_DOWNLOAD_FILE:
			if ((result=storage_server_download_file(&client_info, \
				nInPackLen)) != 0)
			{
				pthread_mutex_lock(&stat_count_thread_lock);
				g_storage_stat.total_download_count++;
				pthread_mutex_unlock(&stat_count_thread_lock);
				break;
			}

			CHECK_AND_WRITE_TO_STAT_FILE2( \
				g_storage_stat.total_download_count, \
				g_storage_stat.success_download_count)
			break;
		case STORAGE_PROTO_CMD_GET_METADATA:
			if ((result=storage_server_get_metadata(&client_info, \
				nInPackLen)) != 0)
			{
				pthread_mutex_lock(&stat_count_thread_lock);
				g_storage_stat.total_get_meta_count++;
				pthread_mutex_unlock(&stat_count_thread_lock);
				break;
			}

			CHECK_AND_WRITE_TO_STAT_FILE2( \
				g_storage_stat.total_get_meta_count, \
				g_storage_stat.success_get_meta_count)
			break;
		case STORAGE_PROTO_CMD_UPLOAD_FILE:
			if ((result=storage_upload_file(&client_info, \
				&group_array, nInPackLen, &create_flag)) != 0)
			{
				pthread_mutex_lock(&stat_count_thread_lock);
				if (create_flag & STORAGE_CREATE_FLAG_FILE)
				{
					g_storage_stat.total_upload_count++;
				}
				pthread_mutex_unlock(&stat_count_thread_lock);
				break;
			}

			if (create_flag & STORAGE_CREATE_FLAG_FILE)
			{
			CHECK_AND_WRITE_TO_STAT_FILE3( \
				g_storage_stat.total_upload_count, \
				g_storage_stat.success_upload_count, \
				g_storage_stat.last_source_update)
			}
			break;
		case STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE:
			if ((result=storage_upload_slave_file(&client_info, \
				&group_array, nInPackLen, &create_flag)) != 0)
			{
				pthread_mutex_lock(&stat_count_thread_lock);
				if (create_flag & STORAGE_CREATE_FLAG_FILE)
				{
					g_storage_stat.total_upload_count++;
				}
				pthread_mutex_unlock(&stat_count_thread_lock);
				break;
			}

			if (create_flag & STORAGE_CREATE_FLAG_FILE)
			{
			CHECK_AND_WRITE_TO_STAT_FILE3( \
				g_storage_stat.total_upload_count, \
				g_storage_stat.success_upload_count, \
				g_storage_stat.last_source_update)
			}
			break;
		case STORAGE_PROTO_CMD_DELETE_FILE:
			if ((result=storage_server_delete_file(&client_info, \
				&group_array, nInPackLen, &delete_flag)) != 0)
			{
				pthread_mutex_lock(&stat_count_thread_lock);
				if (delete_flag == STORAGE_DELETE_FLAG_NONE ||\
					(delete_flag & STORAGE_DELETE_FLAG_FILE))
				{
					g_storage_stat.total_delete_count++;
				}
				if (delete_flag & STORAGE_DELETE_FLAG_LINK)
				{
					g_storage_stat.total_delete_link_count++;
				}
				pthread_mutex_unlock(&stat_count_thread_lock);

				break;
			}

			if (delete_flag & STORAGE_DELETE_FLAG_FILE)
			{
				CHECK_AND_WRITE_TO_STAT_FILE3( \
				g_storage_stat.total_delete_count, \
				g_storage_stat.success_delete_count, \
				g_storage_stat.last_source_update)
			}

			if (delete_flag & STORAGE_DELETE_FLAG_LINK)
			{
				CHECK_AND_WRITE_TO_STAT_FILE3( \
				g_storage_stat.total_delete_link_count, \
				g_storage_stat.success_delete_link_count, \
				g_storage_stat.last_source_update)
			}

			break;
		case STORAGE_PROTO_CMD_CREATE_LINK:
			if ((result=storage_create_link(&client_info, \
				&group_array, nInPackLen)) != 0)
			{
				pthread_mutex_lock(&stat_count_thread_lock);
				g_storage_stat.total_create_link_count++;
				pthread_mutex_unlock(&stat_count_thread_lock);
				break;
			}

			CHECK_AND_WRITE_TO_STAT_FILE3( \
				g_storage_stat.total_create_link_count, \
				g_storage_stat.success_create_link_count, \
				g_storage_stat.last_source_update)
			break;
		case STORAGE_PROTO_CMD_SYNC_CREATE_FILE:
			if ((result=storage_sync_copy_file(&client_info, \
				nInPackLen,header.cmd,&src_sync_timestamp))!=0)
			{
				break;
			}

			CHECK_AND_WRITE_TO_STAT_FILE1
			break;
		case STORAGE_PROTO_CMD_SYNC_DELETE_FILE:
			if ((result=storage_sync_delete_file(&client_info, \
				nInPackLen, &src_sync_timestamp)) != 0)
			{
				break;
			}

			CHECK_AND_WRITE_TO_STAT_FILE1
			break;
		case STORAGE_PROTO_CMD_SYNC_UPDATE_FILE:
			if ((result=storage_sync_copy_file(&client_info, \
				nInPackLen,header.cmd,&src_sync_timestamp))!=0)
			{
				break;
			}

			CHECK_AND_WRITE_TO_STAT_FILE1
			break;
		case STORAGE_PROTO_CMD_SYNC_CREATE_LINK:
			if ((result=storage_sync_link_file(&client_info, \
				nInPackLen, &src_sync_timestamp))!=0)
			{
				break;
			}

			CHECK_AND_WRITE_TO_STAT_FILE1
			break;
		case STORAGE_PROTO_CMD_SET_METADATA:
			if ((result=storage_server_set_metadata(&client_info, \
				nInPackLen)) != 0)
			{
				pthread_mutex_lock(&stat_count_thread_lock);
				g_storage_stat.total_set_meta_count++;
				pthread_mutex_unlock(&stat_count_thread_lock);
				break;
			}

			CHECK_AND_WRITE_TO_STAT_FILE3( \
				g_storage_stat.total_set_meta_count, \
				g_storage_stat.success_set_meta_count, \
				g_storage_stat.last_source_update)
			break;
		case STORAGE_PROTO_CMD_QUERY_FILE_INFO:
			if ((result=storage_server_query_file_info(&client_info,
				nInPackLen)) != 0)
			{
				break;
			}

			break;
		case FDFS_PROTO_CMD_QUIT:
			result = ECONNRESET;  //for quit loop
			break;
		case FDFS_PROTO_CMD_ACTIVE_TEST:
			result = storage_deal_active_test(&client_info, \
				nInPackLen);
			break;
		case STORAGE_PROTO_CMD_REPORT_CLIENT_IP:
			if ((result=storage_server_report_client_ip(&client_info,
				nInPackLen)) != 0)
			{
				break;
			}

			break;
		default:
			logError("file: "__FILE__", line: %d, "   \
				"client ip: %s, unkown cmd: %d", \
				__LINE__, client_info.ip_addr, header.cmd);
			result = EINVAL;
			break;
		}

		if (result != 0)
		{
			break;
		}

		count++;
	}
	close(client_info.sock);
	}

	if ((result=pthread_mutex_lock(&g_storage_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}
	g_storage_thread_count--;
	if ((result=pthread_mutex_unlock(&g_storage_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	if (g_check_file_duplicate)
	{
		if (g_keep_alive)
		{
			fdht_disconnect_all_servers(&group_array);
		}

		fdht_free_group_array(&group_array);
	}

	while (!g_thread_kill_done)  //waiting for kill signal
	{
		usleep(50000);
	}

	return NULL;
}

int fdfs_stat_file_sync_func(void *args)
{
	int result;

	if (last_stat_change_count !=  g_stat_change_count)
	{
		if ((result=storage_write_to_stat_file()) == 0)
		{
			last_stat_change_count = g_stat_change_count;
		}

		return result;
	}
	else
	{
		return 0;
	}
}

