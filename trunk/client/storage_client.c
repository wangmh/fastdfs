/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/


#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include "fdfs_define.h"
#include "logger.h"
#include "fdfs_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "tracker_client.h"
#include "storage_client.h"
#include "client_global.h"
#include "fdfs_base64.h"

#define FDFS_SPLIT_GROUP_NAME_AND_FILENAME(file_id) \
	char new_file_id[FDFS_GROUP_NAME_MAX_LEN + 64]; \
	char *group_name; \
	char *filename; \
	char *pSeperator; \
	\
	snprintf(new_file_id, sizeof(new_file_id), "%s", file_id); \
	pSeperator = strchr(new_file_id, FDFS_FILE_ID_SEPERATOR); \
	if (pSeperator == NULL) \
	{ \
		return EINVAL; \
	} \
	\
	*pSeperator = '\0'; \
	group_name = new_file_id; \
	filename =  pSeperator + 1; \

static int storage_get_read_connection(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo **ppStorageServer, \
		const char *group_name, const char *filename, \
		TrackerServerInfo *pNewStorage, bool *new_connection)
{
	int result;
	if (*ppStorageServer == NULL)
	{
		if ((result=tracker_query_storage_fetch(pTrackerServer, \
		                pNewStorage, group_name, filename)) != 0)
		{
			return result;
		}

		if ((result=tracker_connect_server(pNewStorage)) != 0)
		{
			return result;
		}

		*ppStorageServer = pNewStorage;
		*new_connection = true;
	}
	else
	{
		if ((*ppStorageServer)->sock >= 0)
		{
			*new_connection = false;
		}
		else
		{
			if ((result=tracker_connect_server(*ppStorageServer)) != 0)
			{
				return result;
			}

			*new_connection = true;
		}
	}

	return 0;
}

static int storage_get_write_connection(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo **ppStorageServer, \
		TrackerServerInfo *pNewStorage, bool *new_connection)
{
	int result;
	if (*ppStorageServer == NULL)
	{
		if ((result=tracker_query_storage_store(pTrackerServer, \
		                pNewStorage)) != 0)
		{
			return result;
		}

		if ((result=tracker_connect_server(pNewStorage)) != 0)
		{
			return result;
		}

		*ppStorageServer = pNewStorage;
		*new_connection = true;
	}
	else
	{
		if ((*ppStorageServer)->sock >= 0)
		{
			*new_connection = false;
		}
		else
		{
			if ((result=tracker_connect_server(*ppStorageServer)) != 0)
			{
				return result;
			}

			*new_connection = true;
		}
	}

	return 0;
}

int storage_get_metadata1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer,  \
		const char *file_id, \
		FDFSMetaData **meta_list, int *meta_count)
{
	FDFS_SPLIT_GROUP_NAME_AND_FILENAME(file_id)

	return storage_get_metadata(pTrackerServer, pStorageServer, \
			group_name, filename, meta_list, meta_count);
}

int storage_get_metadata(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer,  \
			const char *group_name, const char *filename, \
			FDFSMetaData **meta_list, \
			int *meta_count)
{
	TrackerHeader header;
	int result;
	TrackerServerInfo storageServer;
	char out_buff[sizeof(TrackerHeader)+FDFS_GROUP_NAME_MAX_LEN+32];
	int64_t in_bytes;
	int filename_len;
	char *file_buff;
	int64_t file_size;
	bool new_connection;

	file_buff = NULL;
	*meta_list = NULL;
	*meta_count = 0;

	if ((result=storage_get_read_connection(pTrackerServer, \
		&pStorageServer, group_name, filename, \
		&storageServer, &new_connection)) != 0)
	{
		return result;
	}

	while (1)
	{
	/**
	send pkg format:
	FDFS_GROUP_NAME_MAX_LEN bytes: group_name
	remain bytes: filename
	**/

	memset(out_buff, 0, sizeof(out_buff));
	snprintf(out_buff + sizeof(TrackerHeader), sizeof(out_buff) - \
		sizeof(TrackerHeader),  "%s", group_name);
	filename_len = snprintf(out_buff + sizeof(TrackerHeader) + \
			FDFS_GROUP_NAME_MAX_LEN, \
			sizeof(out_buff) - sizeof(TrackerHeader) - \
			FDFS_GROUP_NAME_MAX_LEN,  "%s", filename);

	long2buff(FDFS_GROUP_NAME_MAX_LEN + filename_len, header.pkg_len);
	header.cmd = STORAGE_PROTO_CMD_GET_METADATA;
	header.status = 0;
	memcpy(out_buff, &header, sizeof(TrackerHeader));

	if ((result=tcpsenddata(pStorageServer->sock, out_buff, \
			sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN + \
			filename_len, g_network_timeout)) != 0)
	{
		logError("send data to storage server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pStorageServer->ip_addr, \
			pStorageServer->port, \
			result, strerror(result));

		break;
	}

	if ((result=fdfs_recv_response(pStorageServer, \
		&file_buff, 0, &in_bytes)) != 0)
	{
		break;
	}

	file_size = in_bytes;
	if (file_size == 0)
	{
		break;
	}

	file_buff[in_bytes] = '\0';
	*meta_list = fdfs_split_metadata(file_buff, meta_count, &result);
	break;
	}

	if (file_buff != NULL)
	{
		free(file_buff);
	}

	if (new_connection)
	{
		fdfs_quit(pStorageServer);
		tracker_disconnect_server(pStorageServer);
	}

	return result;
}

int storage_delete_file1(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *file_id)
{
	FDFS_SPLIT_GROUP_NAME_AND_FILENAME(file_id)

	return storage_delete_file(pTrackerServer, \
			pStorageServer, group_name, filename);
}

int storage_delete_file(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *group_name, const char *filename)
{
	TrackerHeader header;
	int result;
	TrackerServerInfo storageServer;
	char out_buff[sizeof(TrackerHeader)+FDFS_GROUP_NAME_MAX_LEN+32];
	char in_buff[1];
	char *pBuff;
	int64_t in_bytes;
	int filename_len;
	bool new_connection;

	if ((result=storage_get_read_connection(pTrackerServer, \
		&pStorageServer, group_name, filename, \
		&storageServer, &new_connection)) != 0)
	{
		return result;
	}

	while (1)
	{
	/**
	send pkg format:
	FDFS_GROUP_NAME_MAX_LEN bytes: group_name
	remain bytes: filename
	**/

	memset(out_buff, 0, sizeof(out_buff));
	snprintf(out_buff + sizeof(TrackerHeader), sizeof(out_buff) - \
		sizeof(TrackerHeader),  "%s", group_name);
	filename_len = snprintf(out_buff + sizeof(TrackerHeader) + \
			FDFS_GROUP_NAME_MAX_LEN, \
			sizeof(out_buff) - sizeof(TrackerHeader) - \
			FDFS_GROUP_NAME_MAX_LEN,  "%s", filename);

	long2buff(FDFS_GROUP_NAME_MAX_LEN + filename_len, header.pkg_len);
	header.cmd = STORAGE_PROTO_CMD_DELETE_FILE;
	header.status = 0;
	memcpy(out_buff, &header, sizeof(TrackerHeader));

	if ((result=tcpsenddata(pStorageServer->sock, out_buff, \
		sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN + \
		filename_len, g_network_timeout)) != 0)
	{
		logError("send data to storage server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pStorageServer->ip_addr, \
			pStorageServer->port, \
			result, strerror(result));
		break;
	}

	pBuff = in_buff;
	if ((result=fdfs_recv_response(pStorageServer, \
		&pBuff, 0, &in_bytes)) != 0)
	{
		break;
	}

	break;
	}

	if (new_connection)
	{
		fdfs_quit(pStorageServer);
		tracker_disconnect_server(pStorageServer);
	}

	return result;
}

int storage_do_download_file1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const int download_type, const char *file_id, \
		char **file_buff, void *arg, int64_t *file_size)
{
	FDFS_SPLIT_GROUP_NAME_AND_FILENAME(file_id)

	return storage_do_download_file(pTrackerServer, pStorageServer, \
		download_type, group_name, filename, \
		file_buff, arg, file_size);
}

int storage_do_download_file(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, const int download_type, \
		const char *group_name, const char *remote_filename, \
		char **file_buff, void *arg, int64_t *file_size)
{
	TrackerHeader header;
	int result;
	TrackerServerInfo storageServer;
	char out_buff[sizeof(TrackerHeader)+FDFS_GROUP_NAME_MAX_LEN+32];
	int64_t in_bytes;
	int filename_len;
	bool new_connection;

	*file_size = 0;
	if ((result=storage_get_read_connection(pTrackerServer, \
		&pStorageServer, group_name, remote_filename, \
		&storageServer, &new_connection)) != 0)
	{
		return result;
	}

	while (1)
	{
	/**
	send pkg format:
	FDFS_GROUP_NAME_MAX_LEN bytes: group_name
	remain bytes: filename
	**/

	memset(out_buff, 0, sizeof(out_buff));
	snprintf(out_buff + sizeof(TrackerHeader), sizeof(out_buff) - \
		sizeof(TrackerHeader),  "%s", group_name);
	filename_len = snprintf(out_buff + sizeof(TrackerHeader) + \
			FDFS_GROUP_NAME_MAX_LEN, \
			sizeof(out_buff) - sizeof(TrackerHeader) - \
			FDFS_GROUP_NAME_MAX_LEN,  "%s", remote_filename);

	long2buff(FDFS_GROUP_NAME_MAX_LEN + filename_len, header.pkg_len);
	header.cmd = STORAGE_PROTO_CMD_DOWNLOAD_FILE;
	header.status = 0;
	memcpy(out_buff, &header, sizeof(TrackerHeader));

	if ((result=tcpsenddata(pStorageServer->sock, out_buff, \
		sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN + \
		filename_len, g_network_timeout)) != 0)
	{
		logError("send data to storage server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pStorageServer->ip_addr, \
			pStorageServer->port, \
			result, strerror(result));
		break;
	}

	if (download_type == FDFS_DOWNLOAD_TO_FILE)
	{
		if ((result=fdfs_recv_header(pStorageServer, \
			&in_bytes)) != 0)
		{
			break;
		}

		if ((result=tcprecvfile(pStorageServer->sock, \
				*file_buff, in_bytes)) != 0)
		{
			break;
		}
	}
	else if (download_type == FDFS_DOWNLOAD_TO_BUFF)
	{
		*file_buff = NULL;
		if ((result=fdfs_recv_response(pStorageServer, \
			file_buff, 0, &in_bytes)) != 0)
		{
			break;
		}
	}
	else
	{
		DownloadCallback callback;
		char buff[2048];
		int recv_bytes;
		int64_t remain_bytes;

		if ((result=fdfs_recv_header(pStorageServer, \
			&in_bytes)) != 0)
		{
			break;
		}

		callback = (DownloadCallback)*file_buff;
		remain_bytes = in_bytes;
		while (remain_bytes > 0)
		{
			if (remain_bytes > sizeof(buff))
			{
				recv_bytes = sizeof(buff);
			}
			else
			{
				recv_bytes = remain_bytes;
			}

			if ((result=tcprecvdata(pStorageServer->sock, buff, \
				recv_bytes, g_network_timeout)) != 0)
			{
				logError("recv data from storage server " \
					"%s:%d fail, " \
					"errno: %d, error info: %s", \
					pStorageServer->ip_addr, \
					pStorageServer->port, \
					result, strerror(result));
				break;
			}

			result = callback(arg, in_bytes, buff, recv_bytes);
			if (result != 0)
			{
				logError("call callback function fail, " \
					"error code: %d", result);
				break;
			}

			remain_bytes -= recv_bytes;
		}

		if (remain_bytes != 0)
		{
			break;
		}
	}

	*file_size = in_bytes;
	break;
	}

	if (new_connection)
	{
		fdfs_quit(pStorageServer);
		tracker_disconnect_server(pStorageServer);
	}

	return result;
}

int storage_download_file_to_file1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *file_id, \
		const char *local_filename, int64_t *file_size)
{
	FDFS_SPLIT_GROUP_NAME_AND_FILENAME(file_id)

	return storage_download_file_to_file(pTrackerServer, \
			pStorageServer, group_name, filename, \
			local_filename, file_size);
}

int storage_download_file_to_file(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *group_name, const char *remote_filename, \
			const char *local_filename, int64_t *file_size)
{
	char *pLocalFilename;
	pLocalFilename = (char *)local_filename;
	return storage_do_download_file(pTrackerServer, pStorageServer, \
			FDFS_DOWNLOAD_TO_FILE, group_name, remote_filename, \
			&pLocalFilename, NULL, file_size);
}

int storage_upload_by_filename1(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *local_filename, \
			const FDFSMetaData *meta_list, \
			const int meta_count, \
			char *file_id)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char remote_filename[64];
	int result;

	result = storage_upload_by_filename(pTrackerServer, \
			pStorageServer, local_filename, \
			meta_list, meta_count, \
			group_name, remote_filename);
	if (result == 0)
	{
		sprintf(file_id, "%s%c%s", group_name, \
			FDFS_FILE_ID_SEPERATOR, remote_filename);
	}
	else
	{
		file_id[0] = '\0';
	}

	return result;
}

int storage_do_upload_file1(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const bool bFilename, \
			const char *file_buff, const int64_t file_size, \
			const FDFSMetaData *meta_list, \
			const int meta_count, \
			char *file_id)
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char remote_filename[64];
	int result;

	result = storage_do_upload_file(pTrackerServer, \
			pStorageServer, bFilename, \
			file_buff, file_size, \
			meta_list, meta_count, \
			group_name, remote_filename);
	if (result == 0)
	{
		sprintf(file_id, "%s%c%s", group_name, \
			FDFS_FILE_ID_SEPERATOR, remote_filename);
	}
	else
	{
		file_id[0] = '\0';
	}

	return result;
}

/**
8 bytes: meta data bytes
8 bytes: file size
meta data bytes: each meta data seperated by \x01,
                 name and value seperated by \x02
file size bytes: file content
**/
int storage_do_upload_file(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const bool bFilename, \
			const char *file_buff, const int64_t file_size, \
			const FDFSMetaData *meta_list, \
			const int meta_count, \
			char *group_name, \
			char *remote_filename)
{
#define MAX_STATIC_META_DATA_COUNT 32
	TrackerHeader header;
	int result;
	char meta_buff[2 * FDFS_PROTO_PKG_LEN_SIZE + \
			sizeof(FDFSMetaData) * MAX_STATIC_META_DATA_COUNT + 2];
	char *pMetaData;
	int meta_bytes;
	int64_t in_bytes;
	char in_buff[128];
	char *pInBuff;
	TrackerServerInfo storageServer;
	bool new_connection;

	group_name[0] = '\0';
	remote_filename[0] = '\0';

	if ((result=storage_get_write_connection(pTrackerServer, \
		&pStorageServer, &storageServer, &new_connection)) != 0)
	{
		return result;
	}

	/*
	//printf("upload to storage %s:%d\n", \
		pStorageServer->ip_addr, pStorageServer->port);
	*/

	while (1)
	{
	/**
	8 bytes: meta data bytes
	meta data bytes: each meta data seperated by \x01,
			 name and value seperated by \x02
	file size bytes: file content
	**/
	if (meta_count <= MAX_STATIC_META_DATA_COUNT)
	{
		pMetaData = meta_buff;
	}
	else
	{
		pMetaData = (char *)malloc(2 * FDFS_PROTO_PKG_LEN_SIZE + \
                        sizeof(FDFSMetaData) * meta_count + 2);
		if (pMetaData == NULL)
		{
			result= errno != 0 ? errno : ENOMEM;

			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail", __LINE__, \
				2 * FDFS_PROTO_PKG_LEN_SIZE + \
				sizeof(FDFSMetaData) * meta_count + 2);
			break;
		}
	}

	if (meta_count > 0)
	{
		fdfs_pack_metadata(meta_list, meta_count, pMetaData + \
			2 * FDFS_PROTO_PKG_LEN_SIZE, &meta_bytes);
	}
	else
	{
		meta_bytes = 0;
	}
	long2buff(meta_bytes, pMetaData);
	long2buff(file_size, pMetaData + FDFS_PROTO_PKG_LEN_SIZE);

	long2buff(2 * FDFS_PROTO_PKG_LEN_SIZE + \
			meta_bytes + file_size, header.pkg_len);
	header.cmd = STORAGE_PROTO_CMD_UPLOAD_FILE;
	header.status = 0;
	if ((result=tcpsenddata(pStorageServer->sock, &header, sizeof(header), \
				g_network_timeout)) != 0)
	{
		logError("send data to storage server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pStorageServer->ip_addr, \
			pStorageServer->port, \
			result, strerror(result));
		break;
	}

	if ((result=tcpsenddata(pStorageServer->sock, pMetaData, \
			2 * FDFS_PROTO_PKG_LEN_SIZE + meta_bytes, \
			g_network_timeout)) != 0)
	{
		logError("send data to storage server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pStorageServer->ip_addr, \
			pStorageServer->port, \
			result, strerror(result));
		break;
	}

	if (bFilename)
	{
		if ((result=tcpsendfile(pStorageServer->sock, file_buff, \
			file_size)) != 0)
		{
			break;
		}
	}
	else
	{
		if ((result=tcpsenddata(pStorageServer->sock, \
			(char *)file_buff, file_size, g_network_timeout)) != 0)
		{
			logError("send data to storage server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pStorageServer->ip_addr, \
				pStorageServer->port, \
				result, strerror(result));
			break;
		}
	}

	pInBuff = in_buff;
	if ((result=fdfs_recv_response(pStorageServer, \
		&pInBuff, sizeof(in_buff), &in_bytes)) != 0)
	{
		break;
	}

	if (in_bytes <= FDFS_GROUP_NAME_MAX_LEN)
	{
		logError("storage server %s:%d response data " \
			"length: "INT64_PRINTF_FORMAT" is invalid, should > %d.", \
			pStorageServer->ip_addr, pStorageServer->port, \
			in_bytes, FDFS_GROUP_NAME_MAX_LEN);
		result = EINVAL;
		break;
	}

	in_buff[in_bytes] = '\0';
	memcpy(group_name, in_buff, FDFS_GROUP_NAME_MAX_LEN);
	group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';

	memcpy(remote_filename, in_buff + FDFS_GROUP_NAME_MAX_LEN, \
		in_bytes - FDFS_GROUP_NAME_MAX_LEN + 1);

	break;
	}

	if (new_connection)
	{
		fdfs_quit(pStorageServer);
		tracker_disconnect_server(pStorageServer);
	}
	if (pMetaData != NULL && pMetaData != meta_buff)
	{
		free(pMetaData);
	}

	return result;
}

int storage_upload_by_filename(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *local_filename, \
			const FDFSMetaData *meta_list, \
			const int meta_count, \
			char *group_name, \
			char *remote_filename)

{
	struct stat stat_buf;

	if (stat(local_filename, &stat_buf) != 0)
	{
		group_name[0] = '\0';
		remote_filename[0] = '\0';
		return errno;
	}

	if (!S_ISREG(stat_buf.st_mode))
	{
		group_name[0] = '\0';
		remote_filename[0] = '\0';
		return EINVAL;
	}

	return storage_do_upload_file(pTrackerServer, \
			pStorageServer, true, local_filename, stat_buf.st_size, \
			meta_list, meta_count, \
			group_name, remote_filename);
}

int storage_set_metadata1(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *file_id, \
			FDFSMetaData *meta_list, const int meta_count, \
			const char op_flag)
{
	FDFS_SPLIT_GROUP_NAME_AND_FILENAME(file_id)

	return storage_set_metadata(pTrackerServer, pStorageServer, \
			group_name, filename, \
			meta_list, meta_count, op_flag);
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
int storage_set_metadata(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *group_name, const char *filename, \
			FDFSMetaData *meta_list, const int meta_count, \
			const char op_flag)
{
	TrackerHeader header;
	int result;
	TrackerServerInfo storageServer;
	char out_buff[sizeof(TrackerHeader)+2*FDFS_PROTO_PKG_LEN_SIZE+FDFS_GROUP_NAME_MAX_LEN+32];
	char in_buff[1];
	int64_t in_bytes;
	char *pBuff;
	int filename_len;
	char *meta_buff;
	int meta_bytes;
	char *p;
	char *pEnd;
	bool new_connection;

	if ((result=storage_get_read_connection(pTrackerServer, \
		&pStorageServer, group_name, filename, \
		&storageServer, &new_connection)) != 0)
	{
		return result;
	}

	meta_buff = NULL;
	while (1)
	{
	memset(out_buff, 0, sizeof(out_buff));
	filename_len = strlen(filename);

	if (meta_count > 0)
	{
		meta_buff = fdfs_pack_metadata(meta_list, meta_count, \
                        NULL, &meta_bytes);
		if (meta_buff == NULL)
		{
			result = ENOMEM;
			break;
		}
	}
	else
	{
		meta_bytes = 0;
	}

	pEnd = out_buff + sizeof(out_buff);
	p = out_buff + sizeof(TrackerHeader);

	long2buff(filename_len, p);
	p += FDFS_PROTO_PKG_LEN_SIZE;

	long2buff(meta_bytes, p);
	p += FDFS_PROTO_PKG_LEN_SIZE;

	*p++ = op_flag;

	snprintf(p, pEnd - p,  "%s", group_name);
	p += FDFS_GROUP_NAME_MAX_LEN;

	filename_len = snprintf(p, pEnd - p, "%s", filename);
	p += filename_len;

	long2buff((int)(p - (out_buff + sizeof(TrackerHeader))) + \
		meta_bytes, header.pkg_len);
	header.cmd = STORAGE_PROTO_CMD_SET_METADATA;
	header.status = 0;
	memcpy(out_buff, &header, sizeof(TrackerHeader));

	if ((result=tcpsenddata(pStorageServer->sock, out_buff, \
			p - out_buff, g_network_timeout)) != 0)
	{
		logError("send data to storage server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pStorageServer->ip_addr, \
			pStorageServer->port, \
			result, strerror(result));

		break;
	}

	if (meta_bytes > 0 && (result=tcpsenddata(pStorageServer->sock, \
			meta_buff, meta_bytes, g_network_timeout)) != 0)
	{
		logError("send data to storage server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pStorageServer->ip_addr, \
			pStorageServer->port, \
			result, strerror(result));

		break;
	}

	pBuff = in_buff;
	result = fdfs_recv_response(pStorageServer, \
		&pBuff, 0, &in_bytes);
	break;
	}

	if (meta_buff != NULL)
	{
		free(meta_buff);
	}

	if (new_connection)
	{
		fdfs_quit(pStorageServer);
		tracker_disconnect_server(pStorageServer);
	}

	return result;
}

int storage_download_file_ex1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *file_id, \
		DownloadCallback callback, void *arg, int64_t *file_size)
{
	FDFS_SPLIT_GROUP_NAME_AND_FILENAME(file_id)

	return storage_download_file_ex(pTrackerServer, pStorageServer, \
		group_name, filename, callback, arg, file_size);
}

int storage_download_file_ex(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *group_name, const char *remote_filename, \
		DownloadCallback callback, void *arg, int64_t *file_size)
{
	char *pCallback;
	pCallback = (char *)callback;
	return storage_do_download_file(pTrackerServer, pStorageServer, \
			FDFS_DOWNLOAD_TO_CALLBACK, group_name, remote_filename, \
			&pCallback, arg, file_size);
}

int tracker_query_storage_fetch1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *file_id)
{
	FDFS_SPLIT_GROUP_NAME_AND_FILENAME(file_id)

	return tracker_query_storage_fetch(pTrackerServer, \
		pStorageServer, group_name, filename);
}

