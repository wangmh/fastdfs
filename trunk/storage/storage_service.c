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
#include "tracker_types.h"
#include "tracker_proto.h"
#include "storage_service.h"
#include "storage_func.h"
#include "storage_sync.h"
#include "storage_global.h"
#include "fdfs_base64.h"
#include "hash.h"

pthread_mutex_t g_storage_thread_lock;
int g_storage_thread_count = 0;

static unsigned char g_path_index_high  = 0;
static unsigned char g_path_index_low = 0;
static int g_path_write_file_count = 0;

static int storage_gen_filename(StorageClientInfo *pClientInfo, \
		const int file_size, const char *szFormattedExt, 
		const int ext_name_len, const time_t timestamp, \
		char *filename, int *filename_len)
{
	int r;
	char buff[sizeof(int) * 4];
	char encoded[sizeof(int) * 6 + 1];
	char szStorageIp[IP_ADDRESS_SIZE];
	int n;
	int len;
	in_addr_t server_ip;

	server_ip = getSockIpaddr(pClientInfo->sock, \
			szStorageIp, IP_ADDRESS_SIZE);
	r = rand();

	int2buff(server_ip, buff);
	int2buff(timestamp, buff+sizeof(int));
	int2buff(file_size, buff+sizeof(int)*2);
	int2buff(r, buff+sizeof(int)*3);

	base64_encode_ex(buff, sizeof(int) * 4, encoded, filename_len, false);
	n = PJWHash(encoded, *filename_len) % (1 << 16);

	/*
	len = sprintf(buff, STORAGE_DATA_DIR_FORMAT"/", (n >> 8) & 0xFF);
	len += sprintf(buff + len, STORAGE_DATA_DIR_FORMAT"/", n & 0xFF);
	*/

	len = sprintf(buff, STORAGE_DATA_DIR_FORMAT"/", g_path_index_high);
	len += sprintf(buff + len, STORAGE_DATA_DIR_FORMAT"/", g_path_index_low);

	if (++g_path_write_file_count >= 100)
	{
		++g_path_index_low;
		if (g_path_index_low == 0)
		{
			g_path_index_high++;
		}

		g_path_write_file_count = 0;
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

static int storage_save_file(StorageClientInfo *pClientInfo, \
			const int file_size, const char *file_ext_name, \
			char *meta_buff, const int meta_size, \
			char *filename, int *filename_len)
{
#define FILE_TIMESTAMP_ADVANCED_SECS	30

	int result;
	int i;
	char full_filename[MAX_PATH_SIZE+64];
	char meta_filename[MAX_PATH_SIZE+64];
	char szFormattedExt[FDFS_FILE_EXT_NAME_MAX_LEN + 2];
	char *p;
	int ext_name_len;
	int pad_len;
	time_t start_time;
	time_t end_time;

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

	start_time = time(NULL);
	for (i=0; i<10; i++)
	{
		if ((result=storage_gen_filename(pClientInfo, file_size, \
				szFormattedExt, FDFS_FILE_EXT_NAME_MAX_LEN+1, \
				start_time + FILE_TIMESTAMP_ADVANCED_SECS, \
				filename, filename_len)) != 0)
		{
			return result;
		}

		sprintf(full_filename, "%s/data/%s", g_base_path, filename);
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

	//if ((result=recv_file_serialized(pClientInfo->sock, 
	if ((result=tcprecvfile(pClientInfo->sock, 
		full_filename, file_size)) != 0)
	{
		*filename = '\0';
		*filename_len = 0;
		return result;
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

	end_time = time(NULL);
	if (end_time - start_time > FILE_TIMESTAMP_ADVANCED_SECS)
	{  //need to rename filename
		char new_full_filename[MAX_PATH_SIZE+64];
		char new_meta_filename[MAX_PATH_SIZE+64];
		char new_filename[64];
		int new_filename_len;

		for (i=0; i<10; i++)
		{
			if ((result=storage_gen_filename(pClientInfo,file_size,\
				szFormattedExt, FDFS_FILE_EXT_NAME_MAX_LEN+1, \
				end_time, new_filename, &new_filename_len)) != 0)
			{
				return 0;
			}

			sprintf(new_full_filename, "%s/data/%s", \
				g_base_path, new_filename);
			if (!fileExists(new_full_filename))
			{
				break;
			}

			*new_full_filename = '\0';
		}

		if (*full_filename == '\0')
		{
			logWarning("file: "__FILE__", line: %d, " \
				"Can't generate uniq filename", __LINE__);
			return 0;
		}

		if (rename(full_filename, new_full_filename) != 0)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"rename %s to %s fail, " \
				"errno: %d, error info: %s", __LINE__, \
				full_filename, new_full_filename, \
				errno, strerror(errno));
			return 0;
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
			"malloc %d bytes fail", __LINE__, sizeof(FDFSMetaData) \
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
static int storage_set_metadata(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	char *in_buff;
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char filename[64];
	char meta_filename[64+sizeof(STORAGE_META_FILE_EXT)];
	char full_filename[MAX_PATH_SIZE + 64 + sizeof(STORAGE_META_FILE_EXT)];
	char op_flag;
	char sync_flag;
	char *meta_buff;
	int meta_bytes;
	int filename_len;
	int result;

	memset(&resp, 0, sizeof(resp));
	in_buff = NULL;
	while (1)
	{
		if (nInPackLen <= 2 * FDFS_PROTO_PKG_LEN_SIZE + 1 + \
					FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
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

		if ((resp.status=tcprecvdata(pClientInfo->sock, in_buff, \
			nInPackLen, g_network_timeout)) != 0)
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
		if ((resp.status=fdfs_check_data_filename(filename, \
			filename_len)) != 0)
		{
			break;
		}

		meta_buff = in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE + 1 + \
				FDFS_GROUP_NAME_MAX_LEN + filename_len;
		*(meta_buff + meta_bytes) = '\0';

		sprintf(full_filename, "%s/data/%s", g_base_path, filename);
		if (!fileExists(full_filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, filename: %s not exist", \
				__LINE__, pClientInfo->ip_addr, filename);
			resp.status = ENOENT;
			break;
		}

		sprintf(meta_filename, "%s"STORAGE_META_FILE_EXT, filename);
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
			resp.status = storage_binlog_write( \
					time(NULL), sync_flag, meta_filename);
		}

		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;

	if (in_buff != NULL)
	{
		free(in_buff);
	}

	if ((result=tcpsenddata(pClientInfo->sock, (void *)&resp, \
		sizeof(resp), g_network_timeout)) != 0)
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
8 bytes: meta data bytes
8 bytes: file size 
FDFS_FILE_EXT_NAME_MAX_LEN bytes: file ext name, do not include dot (.)
meta data bytes: each meta data seperated by \x01,
		 name and value seperated by \x02
file size bytes: file content
**/
static int storage_upload_file(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	int out_len;
	char in_buff[2*FDFS_PROTO_PKG_LEN_SIZE+FDFS_FILE_EXT_NAME_MAX_LEN+1];
	char *meta_buff;
	char *file_ext_name;
	char out_buff[128];
	char filename[128];
	int meta_bytes;
	int64_t file_bytes;
	int filename_len;
	int result;

	memset(&resp, 0, sizeof(resp));
	meta_buff = NULL;
	filename[0] = '\0';
	filename_len = 0;
	while (1)
	{
		if (nInPackLen < 2 * FDFS_PROTO_PKG_LEN_SIZE + 
				FDFS_FILE_EXT_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size " \
				INT64_PRINTF_FORMAT" is not correct, " \
				"expect length > %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, 2 * FDFS_PROTO_PKG_LEN_SIZE + \
				FDFS_FILE_EXT_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata(pClientInfo->sock, in_buff, \
			2*FDFS_PROTO_PKG_LEN_SIZE+FDFS_FILE_EXT_NAME_MAX_LEN, \
			g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		meta_bytes = buff2long(in_buff);
		file_bytes = buff2long(in_buff + FDFS_PROTO_PKG_LEN_SIZE);
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
			(2 * FDFS_PROTO_PKG_LEN_SIZE + \
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

		file_ext_name = in_buff + 2 * FDFS_PROTO_PKG_LEN_SIZE;
		file_ext_name[FDFS_FILE_EXT_NAME_MAX_LEN] = '\0';

		if (meta_bytes > 0)
		{
			meta_buff = (char *)malloc(meta_bytes + 1);
			if (meta_buff == NULL)
			{
				resp.status = ENOMEM;

				logError("file: "__FILE__", line: %d, " \
					"malloc %d bytes fail", \
					__LINE__, meta_bytes + 1);
				break;
			}

			if ((resp.status=tcprecvdata(pClientInfo->sock, \
				meta_buff, meta_bytes, g_network_timeout)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"client ip:%s, recv data fail, " \
					"errno: %d, error info: %s.", \
					__LINE__, pClientInfo->ip_addr, \
					resp.status, strerror(resp.status));
				break;
			}

			*(meta_buff + meta_bytes) = '\0';
		}

		resp.status = storage_save_file(pClientInfo, file_bytes, \
			file_ext_name, meta_buff, meta_bytes, \
			filename, &filename_len);

		if (resp.status != 0)
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
			char meta_filename[64];
			sprintf(meta_filename, "%s"STORAGE_META_FILE_EXT, \
				filename);
			resp.status = storage_binlog_write(time(NULL), \
					STORAGE_OP_TYPE_SOURCE_CREATE_FILE, \
					meta_filename);
		}

		break;
	}

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

	if (meta_buff != NULL)
	{
		free(meta_buff);
	}

	if ((result=tcpsenddata(pClientInfo->sock, out_buff, \
		sizeof(resp) + out_len, g_network_timeout)) != 0)
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
	char filename[128];
	char full_filename[MAX_PATH_SIZE];
	int filename_len;
	int64_t file_bytes;
	int result;

	memset(&resp, 0, sizeof(resp));
	while (1)
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

		if ((resp.status=tcprecvdata(pClientInfo->sock, in_buff, \
			2*FDFS_PROTO_PKG_LEN_SIZE+4+FDFS_GROUP_NAME_MAX_LEN, \
			g_network_timeout)) != 0)
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
				filename_len,  sizeof(filename));
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

		if ((resp.status=tcprecvdata(pClientInfo->sock, filename, \
			filename_len, g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv filename fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}
		*(filename + filename_len) = '\0';
		if ((resp.status=fdfs_check_data_filename(filename, \
			filename_len)) != 0)
		{
			break;
		}

		snprintf(full_filename, sizeof(full_filename), \
				"%s/data/%s", g_base_path, filename);

		if ((proto_cmd == STORAGE_PROTO_CMD_SYNC_CREATE_FILE) && \
			fileExists(full_filename))
		{
			logWarning("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, data file: %s " \
				"already exists, ignore it", \
				__LINE__, \
				proto_cmd, \
				pClientInfo->ip_addr, full_filename);

			if ((resp.status=tcpdiscard(pClientInfo->sock, \
					file_bytes)) != 0)
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
		//else if ((resp.status=recv_file_serialized(pClientInfo->sock, 
		else if ((resp.status=tcprecvfile(pClientInfo->sock, 
				full_filename, file_bytes)) != 0)
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

		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	if ((result=tcpsenddata(pClientInfo->sock, \
		&resp, sizeof(resp), g_network_timeout)) != 0)
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
static int storage_get_metadata(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	int result;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 64];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)+64];
	char *file_buff;
	char *filename;
	int64_t file_bytes;

	memset(&resp, 0, sizeof(resp));
	file_buff = NULL;
	file_bytes = 0;
	while (1)
	{
		if (nInPackLen <= FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
				"expect length > %d", \
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
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is too large, " \
				"expect length should < %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata(pClientInfo->sock, in_buff, \
			nInPackLen, g_network_timeout)) != 0)
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
		if ((resp.status=fdfs_check_data_filename(filename, \
			nInPackLen - FDFS_GROUP_NAME_MAX_LEN)) != 0)
		{
			break;
		}

		sprintf(full_filename, "%s/data/%s", g_base_path, filename);
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
		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	long2buff(file_bytes, resp.pkg_len);
	if ((result=tcpsenddata(pClientInfo->sock, \
		&resp, sizeof(resp), g_network_timeout)) != 0)
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
		result = tcpsenddata(pClientInfo->sock, \
			file_buff, file_bytes, g_network_timeout);
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
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename
**/
static int storage_download_file(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	int result;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 64];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)+16];
	char *filename;
	int64_t file_bytes;
	struct stat stat_buf;

	memset(&resp, 0, sizeof(resp));
	file_bytes = 0;
	while (1)
	{
		if (nInPackLen <= FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
				"expect length > %d", \
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
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is too large, " \
				"expect length should < %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata(pClientInfo->sock, in_buff, \
				nInPackLen, g_network_timeout)) != 0)
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
		if ((resp.status=fdfs_check_data_filename(filename, \
			nInPackLen - FDFS_GROUP_NAME_MAX_LEN)) != 0)
		{
			break;
		}

		sprintf(full_filename, "%s/data/%s", g_base_path, filename);
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
			resp.status = errno != 0 ? errno : ENOENT;

			logError("file: "__FILE__", line: %d, " \
				"call stat fail, file: %s, "\
				"error no: %d, error info: %s", \
				__LINE__, full_filename, \
				errno, strerror(errno));
		}

		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	long2buff(file_bytes, resp.pkg_len);

	if ((result=tcpsenddata(pClientInfo->sock, \
		&resp, sizeof(resp), g_network_timeout)) != 0)
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

	result = tcpsendfile(pClientInfo->sock, \
		full_filename, file_bytes);
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
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 64];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)];
	char *filename;
	int result;

	memset(&resp, 0, sizeof(resp));
	while (1)
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
				"expect length should < %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_SYNC_DELETE_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata(pClientInfo->sock, in_buff, \
			nInPackLen, g_network_timeout)) != 0)
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
		if ((resp.status=fdfs_check_data_filename(filename, \
			nInPackLen - (4 + FDFS_GROUP_NAME_MAX_LEN))) != 0)
		{
			break;
		}

		sprintf(full_filename, "%s/data/%s", \
			g_base_path, filename);
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
		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;

	if ((result=tcpsenddata(pClientInfo->sock, \
		&resp, sizeof(resp), g_network_timeout)) != 0)
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
static int storage_delete_file(StorageClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 64];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)];
	char meta_filename[MAX_PATH_SIZE+sizeof(in_buff)];
	char *filename;
	int result;

	memset(&resp, 0, sizeof(resp));
	while (1)
	{
		if (nInPackLen <= FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
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
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is too large, " \
				"expect length should < %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata(pClientInfo->sock, in_buff, \
			nInPackLen, g_network_timeout)) != 0)
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
		if ((resp.status=fdfs_check_data_filename(filename, \
			nInPackLen - FDFS_GROUP_NAME_MAX_LEN)) != 0)
		{
			break;
		}

		sprintf(full_filename, "%s/data/%s", \
			g_base_path, filename);
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
		}

		sprintf(meta_filename, "%s"STORAGE_META_FILE_EXT, \
				filename);
		resp.status = storage_binlog_write(time(NULL), \
			STORAGE_OP_TYPE_SOURCE_DELETE_FILE, meta_filename);
		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;

	if ((result=tcpsenddata(pClientInfo->sock, \
		&resp, sizeof(resp), g_network_timeout)) != 0)
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

#define CHECK_AND_WRITE_TO_STAT_FILE  \
		if (++g_stat_change_count % STORAGE_SYNC_STAT_FILE_FREQ == 0) \
		{ \
			if (storage_write_to_stat_file() != 0) \
			{ \
				break; \
			} \
		}

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
	int server_sock;
	
	server_sock = (int)arg;

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
	client_info.sock = nbaccept(server_sock, 1 * 60, &result);
	if (pthread_mutex_unlock(&g_storage_thread_lock) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail", \
			__LINE__);
	}
	if(client_info.sock < 0) //error
	{
		if (result == ETIMEDOUT || result == EINTR || \
			result == EAGAIN)
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

	pSrcStorage = NULL;
	count = 0;
	while (g_continue_flag)
	{
		result = tcprecvdata_ex(client_info.sock, &header, \
			sizeof(header), g_network_timeout, &recv_bytes);
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
				log_level = LOG_WARNING;
			}
			else
			{
				log_level = LOG_ERR;
			}
			log_it(log_level, "file: "__FILE__", line: %d, " \
				"client ip: %s, recv data fail, " \
				"errno: %d, error info: %s", \
				__LINE__, client_info.ip_addr, \
				result, strerror(result));
			break;
		}

		nInPackLen = buff2long(header.pkg_len);

		if (header.cmd == STORAGE_PROTO_CMD_DOWNLOAD_FILE)
		{
			g_storage_stat.total_download_count++;
			if (storage_download_file(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
			g_storage_stat.success_download_count++;
			CHECK_AND_WRITE_TO_STAT_FILE
		}
		else if (header.cmd == STORAGE_PROTO_CMD_GET_METADATA)
		{
			g_storage_stat.total_get_meta_count++;
			if (storage_get_metadata(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
			g_storage_stat.success_get_meta_count++;
			CHECK_AND_WRITE_TO_STAT_FILE
		}
		else if (header.cmd == STORAGE_PROTO_CMD_UPLOAD_FILE)
		{
			g_storage_stat.total_upload_count++;
			if (storage_upload_file(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}

			g_storage_stat.success_upload_count++;
			g_storage_stat.last_source_update = time(NULL);
			CHECK_AND_WRITE_TO_STAT_FILE
		}
		else if (header.cmd == STORAGE_PROTO_CMD_DELETE_FILE)
		{
			g_storage_stat.total_delete_count++;
			if (storage_delete_file(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
			g_storage_stat.success_delete_count++;
			g_storage_stat.last_source_update = time(NULL);
			CHECK_AND_WRITE_TO_STAT_FILE
		}
		else if (header.cmd == STORAGE_PROTO_CMD_SYNC_CREATE_FILE)
		{
			if (storage_sync_copy_file(&client_info, \
				nInPackLen,header.cmd,&src_sync_timestamp) != 0)
			{
				break;
			}

			if (pSrcStorage == NULL)
			{
				pSrcStorage = get_storage_server( \
					client_info.ip_addr);
			}
			if (pSrcStorage != NULL)
			{
				pSrcStorage->last_sync_src_timestamp = \
						src_sync_timestamp;
				g_sync_change_count++;
			}

			g_storage_stat.last_sync_update = time(NULL);
			CHECK_AND_WRITE_TO_STAT_FILE
		}
		else if (header.cmd == STORAGE_PROTO_CMD_SYNC_DELETE_FILE)
		{
			if (storage_sync_delete_file(&client_info, \
				nInPackLen, &src_sync_timestamp) != 0)
			{
				break;
			}

			if (pSrcStorage == NULL)
			{
				pSrcStorage = get_storage_server( \
					client_info.ip_addr);
			}
			if (pSrcStorage != NULL)
			{
				pSrcStorage->last_sync_src_timestamp = \
						src_sync_timestamp;
				g_sync_change_count++;
			}

			g_storage_stat.last_sync_update = time(NULL);
			CHECK_AND_WRITE_TO_STAT_FILE
		}
		else if (header.cmd == STORAGE_PROTO_CMD_SYNC_UPDATE_FILE)
		{
			if (storage_sync_copy_file(&client_info, \
				nInPackLen,header.cmd,&src_sync_timestamp)!=0)
			{
				break;
			}

			if (pSrcStorage == NULL)
			{
				pSrcStorage = get_storage_server( \
					client_info.ip_addr);
			}
			if (pSrcStorage != NULL)
			{
				pSrcStorage->last_sync_src_timestamp = \
						src_sync_timestamp;
				g_sync_change_count++;
			}

			g_storage_stat.last_sync_update = time(NULL);
			CHECK_AND_WRITE_TO_STAT_FILE
		}
		else if (header.cmd == STORAGE_PROTO_CMD_SET_METADATA)
		{
			g_storage_stat.total_set_meta_count++;
			if (storage_set_metadata(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}

			g_storage_stat.success_set_meta_count++;
			g_storage_stat.last_source_update = time(NULL);
			CHECK_AND_WRITE_TO_STAT_FILE
		}
		else if (header.cmd == FDFS_PROTO_CMD_QUIT)
		{
			break;
		}
		else
		{
			logError("file: "__FILE__", line: %d, "   \
				"client ip: %s, unkown cmd: %d", \
				__LINE__, client_info.ip_addr, header.cmd);
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

	return NULL;
}

