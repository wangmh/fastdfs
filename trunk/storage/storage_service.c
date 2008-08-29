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

static int storage_gen_filename(StorageClientInfo *pClientInfo, \
			const int file_size, \
			char *filename, int *filename_len)
{
	//struct timeval tv;
	int current_time;
	int r;
	char buff[sizeof(int) * 3];
	char encoded[sizeof(int) * 4 + 1];
	int n;
	int len;

	/*
	if (gettimeofday(&tv, NULL) != 0)
	{
		logError("client ip: %s, call gettimeofday fail, " \
			 "errno=%d, error info: %s", \
			__LINE__, pClientInfo->ip_addr,  \
			 errno, strerror(errno));
		return errno != 0 ? errno : EPERM;
	}

	current_time = (int)(tv.tv_sec - (2008 - 1970) * 365 * 24 * 3600) * \
			100 + (int)(tv.tv_usec / 10000);
	*/

	r = rand();
	current_time = time(NULL);
	int2buff(current_time, buff);
	int2buff(file_size, buff+sizeof(int));
	int2buff(r, buff+sizeof(int)*2);

	base64_encode_ex(buff, sizeof(int) * 3, encoded, filename_len, false);
	n = PJWHash(encoded, *filename_len) % (1 << 16);
	len = sprintf(buff, STORAGE_DATA_DIR_FORMAT"/", (n >> 8) & 0xFF);
	len += sprintf(buff + len, STORAGE_DATA_DIR_FORMAT"/", n & 0xFF);

	memcpy(filename, buff, len);
	memcpy(filename+len, encoded, *filename_len+1);
	*filename_len += len;

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
	printf("meta_count=%d\n", meta_count);
	for (i=0; i<meta_count; i++)
	{
		printf("%d. %s=%s\n", i+1, meta_list[i].name, meta_list[i].value);
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
			const int file_size, \
			char *meta_buff, const int meta_size, \
			char *filename, int *filename_len)
{
	int result;
	int i;
	char full_filename[MAX_PATH_SIZE+32];

	for (i=0; i<1024; i++)
	{
		if ((result=storage_gen_filename(pClientInfo, file_size, \
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

	if ((result=tcprecvfile(pClientInfo->sock, full_filename, file_size)) != 0)
	{
		*filename = '\0';
		*filename_len = 0;
		return result;
	}

	if (meta_size > 0)
	{
		char meta_filename[MAX_PATH_SIZE+32];

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
	int file_bytes;
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
9 bytes: filename length
9 bytes: meta data size
1 bytes: operation flag, 
     'O' for overwrite all old metadata
     'M' for merge, insert when the meta item not exist, otherwise update it
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename
meta data bytes: each meta data seperated by \x01,
		 name and value seperated by \x02
**/
static int storage_set_metadata(StorageClientInfo *pClientInfo, \
				const int nInPackLen)
{
	TrackerHeader resp;
	char *in_buff;
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char filename[32];
	char meta_filename[32+sizeof(STORAGE_META_FILE_EXT)];
	char full_filename[MAX_PATH_SIZE + 32 + sizeof(STORAGE_META_FILE_EXT)];
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
		if (nInPackLen <= 2 * TRACKER_PROTO_PKG_LEN_SIZE + 1 + \
					FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size %d " \
				"is not correct, " \
				"expect length > %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_SET_METADATA, \
				pClientInfo->ip_addr,  \
				nInPackLen, 2 * TRACKER_PROTO_PKG_LEN_SIZE + 1 \
				+ FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		in_buff = (char *)malloc(nInPackLen + 1);
		if (in_buff == NULL)
		{
			resp.status = ENOMEM;

			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail", \
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
		filename_len = strtol(in_buff, NULL, 16);
		meta_bytes = strtol(in_buff + TRACKER_PROTO_PKG_LEN_SIZE, \
				NULL, 16);
		if (filename_len <= 0 || filename_len >= sizeof(filename))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, invalid filename length: %d", \
				__LINE__, pClientInfo->ip_addr, filename_len);
			resp.status = EINVAL;
			break;
		}

		op_flag = *(in_buff + 2 * TRACKER_PROTO_PKG_LEN_SIZE);
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
				(2 * TRACKER_PROTO_PKG_LEN_SIZE + 1 + \
				FDFS_GROUP_NAME_MAX_LEN + filename_len))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, invalid meta bytes: %d", \
				__LINE__, pClientInfo->ip_addr, meta_bytes);
			resp.status = EINVAL;
			break;
		}

		memcpy(group_name, in_buff + 2 * TRACKER_PROTO_PKG_LEN_SIZE + 1, \
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

		memcpy(filename, in_buff + 2 * TRACKER_PROTO_PKG_LEN_SIZE + 1 + \
			FDFS_GROUP_NAME_MAX_LEN, filename_len);
		*(filename + filename_len) = '\0';
		if ((resp.status=fdfs_check_data_filename(filename, \
			filename_len)) != 0)
		{
			break;
		}

		meta_buff = in_buff + 2 * TRACKER_PROTO_PKG_LEN_SIZE + 1 + \
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
					sync_flag, meta_filename);
		}

		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	sprintf(resp.pkg_len, "%x", 0);

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
9 bytes: meta data bytes
9 bytes: file size 
meta data bytes: each meta data seperated by \x01,
		 name and value seperated by \x02
file size bytes: file content
**/
static int storage_upload_file(StorageClientInfo *pClientInfo, \
				const int nInPackLen)
{
	TrackerHeader resp;
	int out_len;
	char in_buff[2 * TRACKER_PROTO_PKG_LEN_SIZE + 1];
	char *meta_buff;
	char out_buff[128];
	char filename[128];
	int meta_bytes;
	int file_bytes;
	int filename_len;
	int result;

	memset(&resp, 0, sizeof(resp));
	meta_buff = NULL;
	filename[0] = '\0';
	filename_len = 0;
	while (1)
	{
		if (nInPackLen < 2 * TRACKER_PROTO_PKG_LEN_SIZE)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size %d " \
				"is not correct, " \
				"expect length > %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_UPLOAD_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, 2 * TRACKER_PROTO_PKG_LEN_SIZE);
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata(pClientInfo->sock, in_buff, \
			2*TRACKER_PROTO_PKG_LEN_SIZE, g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		*(in_buff + 2 * TRACKER_PROTO_PKG_LEN_SIZE) = '\0';
		meta_bytes = strtol(in_buff, NULL, 16);
		file_bytes = strtol(in_buff + TRACKER_PROTO_PKG_LEN_SIZE, \
				NULL, 16);
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
			(2 * TRACKER_PROTO_PKG_LEN_SIZE + meta_bytes))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip:%s, pkg length is not correct, " \
				"invalid file bytes: %d", \
				__LINE__, pClientInfo->ip_addr, \
				file_bytes);
			resp.status = EINVAL;
			break;
		}

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

		resp.status = storage_save_file(pClientInfo,  \
			file_bytes, meta_buff, meta_bytes, \
			filename, &filename_len);

		if (resp.status != 0)
		{
			break;
		}

		resp.status = storage_binlog_write( \
					STORAGE_OP_TYPE_SOURCE_CREATE_FILE, \
					filename);
		if (resp.status != 0)
		{
			break;
		}

		if (meta_bytes > 0)
		{
			char meta_filename[64];
			sprintf(meta_filename, "%s"STORAGE_META_FILE_EXT, \
				filename);
			resp.status = storage_binlog_write( \
					STORAGE_OP_TYPE_SOURCE_CREATE_FILE, \
					meta_filename);
		}

		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	if (resp.status == 0)
	{
		out_len = FDFS_GROUP_NAME_MAX_LEN + filename_len;
		sprintf(resp.pkg_len, "%x", out_len);

		memcpy(out_buff, &resp, sizeof(resp));
		memcpy(out_buff+sizeof(resp), g_group_name, FDFS_GROUP_NAME_MAX_LEN);
		memcpy(out_buff+sizeof(resp)+FDFS_GROUP_NAME_MAX_LEN, filename, filename_len);
	}
	else
	{
		out_len = 0;
		sprintf(resp.pkg_len, "%x", out_len);
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
9 bytes: filename bytes
9 bytes: file size
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename bytes : filename
file size bytes: file content
**/
static int storage_sync_copy_file(StorageClientInfo *pClientInfo, \
			const int nInPackLen, const char proto_cmd)
{
	TrackerHeader resp;
	char in_buff[2 * TRACKER_PROTO_PKG_LEN_SIZE + \
			FDFS_GROUP_NAME_MAX_LEN + 1];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char filename[128];
	char full_filename[MAX_PATH_SIZE];
	int filename_len;
	int file_bytes;
	int result;

	memset(&resp, 0, sizeof(resp));
	while (1)
	{
		if (nInPackLen <= 2 * TRACKER_PROTO_PKG_LEN_SIZE + \
					FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size %d " \
				"is not correct, " \
				"expect length > %d", \
				__LINE__, \
				proto_cmd, \
				pClientInfo->ip_addr,  nInPackLen, \
				2 * TRACKER_PROTO_PKG_LEN_SIZE + \
					FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata(pClientInfo->sock, in_buff, \
			2*TRACKER_PROTO_PKG_LEN_SIZE+FDFS_GROUP_NAME_MAX_LEN, \
			g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv data fail, " \
				"expect pkg length: %d, " \
				"errno: %d, error info: %s", \
				__LINE__, \
				pClientInfo->ip_addr, nInPackLen, \
				resp.status, strerror(resp.status));
			break;
		}

		*(in_buff + 2 * TRACKER_PROTO_PKG_LEN_SIZE + \
			FDFS_GROUP_NAME_MAX_LEN) = '\0';
		filename_len = strtol(in_buff, NULL, 16);
		file_bytes = strtol(in_buff + TRACKER_PROTO_PKG_LEN_SIZE, \
					NULL, 16);

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
				"file size: %d is invalid, which < 0", \
				__LINE__, pClientInfo->ip_addr, file_bytes);
			resp.status = EPIPE;
			break;
		}

		memcpy(group_name, in_buff + 2 * TRACKER_PROTO_PKG_LEN_SIZE, \
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

		if (file_bytes != nInPackLen - (2*TRACKER_PROTO_PKG_LEN_SIZE + \
					FDFS_GROUP_NAME_MAX_LEN + filename_len))
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, in request pkg, " \
				"file size: %d != remain bytes: %d", \
				__LINE__, pClientInfo->ip_addr, file_bytes, \
				nInPackLen - (2*TRACKER_PROTO_PKG_LEN_SIZE + \
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
					"buff size: %d, " \
					"errno: %d, error info: %s.", \
					__LINE__, pClientInfo->ip_addr, \
					file_bytes, \
					resp.status, strerror(resp.status));
				break;
			}

			resp.status = EEXIST;
			break;
		}

		if ((resp.status=tcprecvfile(pClientInfo->sock, \
				full_filename, file_bytes)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv file buff fail, " \
				"file size: %d, errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				file_bytes, resp.status, strerror(resp.status));
			break;
		}

		if (proto_cmd == STORAGE_PROTO_CMD_SYNC_CREATE_FILE)
		{
			resp.status = storage_binlog_write( \
				STORAGE_OP_TYPE_REPLICA_CREATE_FILE, \
				filename);
		}
		else
		{
			resp.status = storage_binlog_write( \
				STORAGE_OP_TYPE_REPLICA_UPDATE_FILE, \
				filename);
		}

		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	resp.pkg_len[0] = '0';
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

	if (resp.status == EEXIST)
	{
		return 0;
	}
	else
	{
		return resp.status;
	}
}

/**
pkg format:
Header
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename
**/
static int storage_get_metadata(StorageClientInfo *pClientInfo, \
				const int nInPackLen)
{
	TrackerHeader resp;
	int result;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 32];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)+32];
	char *file_buff;
	char *filename;
	int file_bytes;

	memset(&resp, 0, sizeof(resp));
	file_buff = NULL;
	file_bytes = 0;
	while (1)
	{
		if (nInPackLen <= FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size %d " \
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
				"cmd=%d, client ip: %s, package size %d " \
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
	sprintf(resp.pkg_len, "%x", file_bytes);

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
				const int nInPackLen)
{
	TrackerHeader resp;
	int result;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 32];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)+16];
	char *filename;
	int file_bytes;
	struct stat stat_buf;

	memset(&resp, 0, sizeof(resp));
	file_bytes = 0;
	while (1)
	{
		if (nInPackLen <= FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size %d " \
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
				"cmd=%d, client ip: %s, package size %d " \
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
	sprintf(resp.pkg_len, "%x", file_bytes);

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
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename
**/
static int storage_sync_delete_file(StorageClientInfo *pClientInfo, \
				const int nInPackLen)
{
	TrackerHeader resp;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 32];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char full_filename[MAX_PATH_SIZE+sizeof(in_buff)];
	char *filename;
	int result;

	memset(&resp, 0, sizeof(resp));
	while (1)
	{
		if (nInPackLen <= FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size %d " \
				"is not correct, " \
				"expect length <= %d", \
				__LINE__, \
				STORAGE_PROTO_CMD_SYNC_DELETE_FILE, \
				pClientInfo->ip_addr,  \
				nInPackLen, FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if (nInPackLen >= sizeof(in_buff))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size %d " \
				"is too large, " \
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
			if (errno == ENOENT)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"cmd=%d, client ip: %s, file %s not exist, " \
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

		resp.status = storage_binlog_write( \
					STORAGE_OP_TYPE_REPLICA_DELETE_FILE, \
					filename);
		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	strcpy(resp.pkg_len, "0");

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
				const int nInPackLen)
{
	TrackerHeader resp;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 32];
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
				"cmd=%d, client ip: %s, package size %d " \
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
				"cmd=%d, client ip: %s, package size %d " \
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

		resp.status = storage_binlog_write( \
					STORAGE_OP_TYPE_SOURCE_DELETE_FILE, \
					filename);
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
		resp.status = storage_binlog_write( \
					STORAGE_OP_TYPE_SOURCE_DELETE_FILE, \
					meta_filename);
		break;
	}

	resp.cmd = STORAGE_PROTO_CMD_RESP;
	strcpy(resp.pkg_len, "0");

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
9 bytes length (hex string)
1 bytes cmd (char)
1 bytes status(char)
data buff (struct)
*/
	StorageClientInfo client_info;
	TrackerHeader header;
	int result;
	int nInPackLen;
	int count;
	int recv_bytes;
	int log_level;
	
	memset(&client_info, 0, sizeof(client_info));
	client_info.sock = (int)arg;
	
	getPeerIpaddr(client_info.sock, \
				client_info.ip_addr, FDFS_IPADDR_SIZE);

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

		header.pkg_len[sizeof(header.pkg_len)-1] = '\0';
		nInPackLen = strtol(header.pkg_len, NULL, 16);

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
				nInPackLen, header.cmd) != 0)
			{
				break;
			}
			g_storage_stat.last_sync_update = time(NULL);
			CHECK_AND_WRITE_TO_STAT_FILE
		}
		else if (header.cmd == STORAGE_PROTO_CMD_SYNC_DELETE_FILE)
		{
			if (storage_sync_delete_file(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
			g_storage_stat.last_sync_update = time(NULL);
			CHECK_AND_WRITE_TO_STAT_FILE
		}
		else if (header.cmd == STORAGE_PROTO_CMD_SYNC_UPDATE_FILE)
		{
			if (storage_sync_copy_file(&client_info, \
				nInPackLen, header.cmd) != 0)
			{
				break;
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

	if (pthread_mutex_lock(&g_storage_thread_lock) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info:%s.", \
			__LINE__, errno, strerror(errno));
	}
	g_storage_thread_count--;
	if (pthread_mutex_unlock(&g_storage_thread_lock) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
	}

	close(client_info.sock);
	return NULL;
}

