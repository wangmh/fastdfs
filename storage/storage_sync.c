/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_sync.c

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
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
#include "pthread_func.h"
#include "ini_file_reader.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "storage_global.h"
#include "storage_func.h"
#include "storage_sync.h"
#include "storage_ip_changed_dealer.h"
#include "tracker_client_thread.h"

#define SYNC_BINLOG_FILE_MAX_SIZE	1024 * 1024 * 1024
#define SYNC_BINLOG_FILE_PREFIX		"binlog"
#define SYNC_BINLOG_INDEX_FILENAME	SYNC_BINLOG_FILE_PREFIX".index"
#define SYNC_MARK_FILE_EXT		".mark"
#define SYNC_BINLOG_FILE_EXT_FMT	".%03d"
#define SYNC_DIR_NAME			"sync"
#define MARK_ITEM_BINLOG_FILE_INDEX	"binlog_index"
#define MARK_ITEM_BINLOG_FILE_OFFSET	"binlog_offset"
#define MARK_ITEM_NEED_SYNC_OLD		"need_sync_old"
#define MARK_ITEM_SYNC_OLD_DONE		"sync_old_done"
#define MARK_ITEM_UNTIL_TIMESTAMP	"until_timestamp"
#define MARK_ITEM_SCAN_ROW_COUNT	"scan_row_count"
#define MARK_ITEM_SYNC_ROW_COUNT	"sync_row_count"
#define SYNC_BINLOG_WRITE_BUFF_SIZE	16 * 1024

int g_binlog_fd = -1;
int g_binlog_index = 0;
static int64_t binlog_file_size = 0;

int g_storage_sync_thread_count = 0;
static pthread_mutex_t sync_thread_lock;
static char *binlog_write_cache_buff = NULL;
static int binlog_write_cache_len = 0;
static int binlog_write_version = 1;

/* save sync thread ids */
static pthread_t *sync_tids = NULL;

static int storage_write_to_mark_file(BinLogReader *pReader);
static int storage_binlog_reader_skip(BinLogReader *pReader);
static void storage_reader_destroy(BinLogReader *pReader);
static int storage_binlog_fsync(const bool bNeedLock);
static int storage_binlog_preread(BinLogReader *pReader);

/**
8 bytes: filename bytes
8 bytes: file size
4 bytes: source op timestamp
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
filename bytes : filename
file size bytes: file content
**/
static int storage_sync_copy_file(TrackerServerInfo *pStorageServer, \
			const BinLogRecord *pRecord, const char proto_cmd)
{
	TrackerHeader *pHeader;
	char *p;
	char *pBuff;
	char full_filename[MAX_PATH_SIZE];
	char out_buff[sizeof(TrackerHeader)+FDFS_GROUP_NAME_MAX_LEN+256];
	char in_buff[1];
	struct stat stat_buf;
	int64_t in_bytes;
	int result;

	snprintf(full_filename, sizeof(full_filename), \
		"%s/data/%s", pRecord->pBasePath, pRecord->true_filename);
	if (stat(full_filename, &stat_buf) != 0)
	{
		if (errno == ENOENT)
		{
			if(pRecord->op_type==STORAGE_OP_TYPE_SOURCE_CREATE_FILE)
			{
				logDebug("file: "__FILE__", line: %d, " \
					"sync data file, file: %s not exists, "\
					"maybe deleted later?", \
					__LINE__, full_filename);
			}
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"call stat fail, file: %s, "\
				"error no: %d, error info: %s", \
				__LINE__, full_filename, \
				errno, strerror(errno));
		}

		return 0;
	}

	//printf("sync create file: %s\n", pRecord->filename);
	do
	{
		pHeader = (TrackerHeader *)out_buff;
		memset(pHeader, 0, sizeof(TrackerHeader));
		long2buff(2 * FDFS_PROTO_PKG_LEN_SIZE + \
				4 + FDFS_GROUP_NAME_MAX_LEN + \
				pRecord->filename_len + stat_buf.st_size,\
				pHeader->pkg_len);
		pHeader->cmd = proto_cmd;

		p = out_buff + sizeof(TrackerHeader);

		long2buff(pRecord->filename_len, p);
		p += FDFS_PROTO_PKG_LEN_SIZE;

		long2buff(stat_buf.st_size, p);
		p += FDFS_PROTO_PKG_LEN_SIZE;

		int2buff(pRecord->timestamp, p);
		p += 4;

		sprintf(p, "%s", pStorageServer->group_name);
		p += FDFS_GROUP_NAME_MAX_LEN;
		memcpy(p, pRecord->filename, pRecord->filename_len);
		p += pRecord->filename_len;

		if((result=tcpsenddata_nb(pStorageServer->sock, out_buff, \
			p - out_buff, g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"sync data to storage server %s:%d fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pStorageServer->ip_addr, \
				pStorageServer->port, \
				result, strerror(result));

			break;
		}

		if((stat_buf.st_size > 0) && ((result=tcpsendfile( \
			pStorageServer->sock, full_filename, \
			stat_buf.st_size, g_network_timeout)) != 0))
		{
			logError("file: "__FILE__", line: %d, " \
				"sync data to storage server %s:%d fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pStorageServer->ip_addr, \
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

	} while (0);

	//printf("sync create file end!\n");
	if (result == EEXIST)
	{
		if (pRecord->op_type == STORAGE_OP_TYPE_SOURCE_CREATE_FILE)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"storage server ip: %s:%d, data file: %s " \
				"already exists, maybe some mistake?", \
				__LINE__, pStorageServer->ip_addr, \
				pStorageServer->port, pRecord->filename);
		}

		return 0;
	}
	else
	{
		return result;
	}
}

/**
send pkg format:
4 bytes: source delete timestamp
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
remain bytes: filename
**/
static int storage_sync_delete_file(TrackerServerInfo *pStorageServer, \
			const BinLogRecord *pRecord)
{
	TrackerHeader *pHeader;
	char full_filename[MAX_PATH_SIZE];
	char out_buff[sizeof(TrackerHeader)+FDFS_GROUP_NAME_MAX_LEN+256];
	char in_buff[1];
	char *pBuff;
	int64_t in_bytes;
	int result;

	snprintf(full_filename, sizeof(full_filename), \
		"%s/data/%s", pRecord->pBasePath, pRecord->true_filename);
	if (fileExists(full_filename))
	{
		if (pRecord->op_type == STORAGE_OP_TYPE_SOURCE_DELETE_FILE)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"sync data file, file: %s exists, " \
				"maybe created later?", \
				__LINE__, full_filename);
		}

		return 0;
	}

	memset(out_buff, 0, sizeof(out_buff));
	int2buff(pRecord->timestamp, out_buff + sizeof(TrackerHeader));
	snprintf(out_buff + sizeof(TrackerHeader) + 4, sizeof(out_buff) - \
		sizeof(TrackerHeader),  "%s", g_group_name);
	memcpy(out_buff + sizeof(TrackerHeader) + 4 + FDFS_GROUP_NAME_MAX_LEN, \
		pRecord->filename, pRecord->filename_len);

	pHeader = (TrackerHeader *)out_buff;
	long2buff(4 + FDFS_GROUP_NAME_MAX_LEN + pRecord->filename_len, \
			pHeader->pkg_len);
	pHeader->cmd = STORAGE_PROTO_CMD_SYNC_DELETE_FILE;

	if ((result=tcpsenddata_nb(pStorageServer->sock, out_buff, \
		sizeof(TrackerHeader) + 4 + FDFS_GROUP_NAME_MAX_LEN + \
		pRecord->filename_len, g_network_timeout)) != 0)
	{
		logError("FILE: "__FILE__", line: %d, " \
			"send data to storage server %s:%d fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pStorageServer->ip_addr, \
			pStorageServer->port, \
			result, strerror(result));
		return result;
	}

	pBuff = in_buff;
	result = fdfs_recv_response(pStorageServer, &pBuff, 0, &in_bytes);
	if (result == ENOENT)
	{
		result = 0;
	}
	
	return result;
}

/**
IP_ADDRESS_SIZE bytes: tracker client ip address
**/
static int storage_report_client_ip(TrackerServerInfo *pStorageServer)
{
	int result;
	TrackerHeader *pHeader;
	char out_buff[sizeof(TrackerHeader)+IP_ADDRESS_SIZE];
	char in_buff[1];
	char *pBuff;
	int64_t in_bytes;

	pHeader = (TrackerHeader *)out_buff;
	memset(out_buff, 0, sizeof(out_buff));
	
	long2buff(IP_ADDRESS_SIZE, pHeader->pkg_len);
	pHeader->cmd = STORAGE_PROTO_CMD_REPORT_CLIENT_IP;
	strcpy(out_buff + sizeof(TrackerHeader), g_tracker_client_ip);
	if ((result=tcpsenddata_nb(pStorageServer->sock, out_buff, \
		sizeof(TrackerHeader) + IP_ADDRESS_SIZE, \
		g_network_timeout)) != 0)
	{
		logError("FILE: "__FILE__", line: %d, " \
			"send data to storage server %s:%d fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pStorageServer->ip_addr, \
			pStorageServer->port, \
			result, strerror(result));
		return result;
	}

	pBuff = in_buff;
	return fdfs_recv_response(pStorageServer, &pBuff, 0, &in_bytes);
}

/**
8 bytes: dest(link) filename length
8 bytes: source filename length
4 bytes: source op timestamp
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
dest filename length: dest filename
source filename length: source filename
**/
static int storage_sync_link_file(TrackerServerInfo *pStorageServer, \
		const BinLogRecord *pRecord)
{
	TrackerHeader *pHeader;
	int result;
	char full_filename[MAX_PATH_SIZE];
	char out_buff[sizeof(TrackerHeader) + 2 * FDFS_PROTO_PKG_LEN_SIZE + \
			4 + FDFS_GROUP_NAME_MAX_LEN + 256];
	char in_buff[1];
	char src_full_filename[MAX_PATH_SIZE];
	char *pSrcFilename;
	char *p;
	int src_filename_len;
	int out_body_len;
	int64_t in_bytes;
	char *pBuff;
	struct stat stat_buf;
	int src_path_index;

	snprintf(full_filename, sizeof(full_filename), \
		"%s/data/%s", pRecord->pBasePath, pRecord->true_filename);
	if (lstat(full_filename, &stat_buf) != 0)
	{
		if (errno == ENOENT)
		{
		if (pRecord->op_type == STORAGE_OP_TYPE_SOURCE_CREATE_LINK)
		{
			logDebug("file: "__FILE__", line: %d, " \
				"sync data file, file: %s does not exist, " \
				"maybe delete later?", \
				__LINE__, full_filename);
		}
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"call stat fail, file: %s, "\
				"error no: %d, error info: %s", \
				__LINE__, full_filename, \
				errno, strerror(errno));
		}

		return 0;
	}

	if (!S_ISLNK(stat_buf.st_mode))
	{
		if (pRecord->op_type == STORAGE_OP_TYPE_SOURCE_CREATE_LINK)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"sync data file, file %s is not a symbol link,"\
				" maybe create later?", \
				__LINE__, full_filename);
		}

		return 0;
	}

	src_filename_len = readlink(full_filename, src_full_filename, \
				sizeof(src_full_filename) - 1);
	if (src_filename_len <= 0)
	{
		logWarning("file: "__FILE__", line: %d, " \
			"data file: %s, readlink fail, "\
			"errno: %d, error info: %s", \
			__LINE__, src_full_filename, errno, strerror(errno));
		return 0;
	}
	*(src_full_filename + src_filename_len) = '\0';

	pSrcFilename = strstr(src_full_filename, "/data/");
	if (pSrcFilename == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"source data file: %s is invalid", \
			__LINE__, src_full_filename);
		return EINVAL;
	}

	pSrcFilename += 6;
	p = strstr(pSrcFilename, "/data/");
	while (p != NULL)
	{
		pSrcFilename = p + 6;
		p = strstr(pSrcFilename, "/data/");
	}

	if (g_path_count == 1)
	{
		src_path_index = 0;
	}
	else
	{
		*(pSrcFilename - 6) = '\0';

		for (src_path_index=0; src_path_index<g_path_count; \
			src_path_index++)
		{
			if (strcmp(src_full_filename, \
				g_store_paths[src_path_index]) == 0)
			{
				break;
			}
		}

		if (src_path_index == g_path_count)
		{
			logError("file: "__FILE__", line: %d, " \
				"source data file: %s is invalid", \
				__LINE__, src_full_filename);
			return EINVAL;
		}
	}

	pSrcFilename -= 4;
	src_filename_len -= (pSrcFilename - src_full_filename);
	if (src_filename_len >= 128)
	{
		logError("file: "__FILE__", line: %d, " \
			"source data file: %s is invalid", \
			__LINE__, src_full_filename);
		return EINVAL;
	}

	sprintf(out_buff, "%c"STORAGE_DATA_DIR_FORMAT"/", \
			STORAGE_STORE_PATH_PREFIX_CHAR, src_path_index);
	memcpy(pSrcFilename, out_buff, 4);

	pHeader = (TrackerHeader *)out_buff;
	memset(out_buff, 0, sizeof(out_buff));
	long2buff(pRecord->filename_len, out_buff + sizeof(TrackerHeader));
	long2buff(src_filename_len, out_buff + sizeof(TrackerHeader) + \
			FDFS_PROTO_PKG_LEN_SIZE);
	int2buff(pRecord->timestamp, out_buff + sizeof(TrackerHeader) + \
			2 * FDFS_PROTO_PKG_LEN_SIZE);
	sprintf(out_buff + sizeof(TrackerHeader) + 2 * FDFS_PROTO_PKG_LEN_SIZE\
		 + 4, "%s", g_group_name);
	memcpy(out_buff + sizeof(TrackerHeader) + 2 * FDFS_PROTO_PKG_LEN_SIZE \
		+ 4 + FDFS_GROUP_NAME_MAX_LEN, \
		pRecord->filename, pRecord->filename_len);
	memcpy(out_buff + sizeof(TrackerHeader) + 2 * FDFS_PROTO_PKG_LEN_SIZE \
		+ 4 + FDFS_GROUP_NAME_MAX_LEN + pRecord->filename_len, \
		pSrcFilename, src_filename_len);

	out_body_len = 2 * FDFS_PROTO_PKG_LEN_SIZE + 4 + \
		FDFS_GROUP_NAME_MAX_LEN + pRecord->filename_len + \
		src_filename_len;
	long2buff(out_body_len, pHeader->pkg_len);
	pHeader->cmd = STORAGE_PROTO_CMD_SYNC_CREATE_LINK;

	if ((result=tcpsenddata_nb(pStorageServer->sock, out_buff, \
		sizeof(TrackerHeader) + out_body_len, g_network_timeout)) != 0)
	{
		logError("FILE: "__FILE__", line: %d, " \
			"send data to storage server %s:%d fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pStorageServer->ip_addr, \
			pStorageServer->port, \
			result, strerror(result));
		return result;
	}

	pBuff = in_buff;
	result = fdfs_recv_response(pStorageServer, &pBuff, 0, &in_bytes);
	if (result == ENOENT)
	{
		result = 0;
	}
	
	return result;
}

#define STARAGE_CHECK_IF_NEED_SYNC_OLD(pReader, pRecord) \
	if ((!pReader->need_sync_old) || pReader->sync_old_done || \
		(pRecord->timestamp > pReader->until_timestamp)) \
	{ \
		return 0; \
	} \

static int storage_sync_data(BinLogReader *pReader, \
			TrackerServerInfo *pStorageServer, \
			const BinLogRecord *pRecord)
{
	int result;
	switch(pRecord->op_type)
	{
		case STORAGE_OP_TYPE_SOURCE_CREATE_FILE:
			result = storage_sync_copy_file(pStorageServer, \
				pRecord, STORAGE_PROTO_CMD_SYNC_CREATE_FILE);
			break;
		case STORAGE_OP_TYPE_SOURCE_DELETE_FILE:
			result = storage_sync_delete_file( \
				pStorageServer, pRecord);
			break;
		case STORAGE_OP_TYPE_SOURCE_UPDATE_FILE:
			result = storage_sync_copy_file(pStorageServer, \
				pRecord, STORAGE_PROTO_CMD_SYNC_UPDATE_FILE);
			break;
		case STORAGE_OP_TYPE_SOURCE_CREATE_LINK:
			result = storage_sync_link_file(pStorageServer, \
				pRecord);
			break;
		case STORAGE_OP_TYPE_REPLICA_CREATE_FILE:
			STARAGE_CHECK_IF_NEED_SYNC_OLD(pReader, pRecord)
			result = storage_sync_copy_file(pStorageServer, \
				pRecord, STORAGE_PROTO_CMD_SYNC_CREATE_FILE);
			break;
		case STORAGE_OP_TYPE_REPLICA_DELETE_FILE:
			STARAGE_CHECK_IF_NEED_SYNC_OLD(pReader, pRecord)
			result = storage_sync_delete_file( \
				pStorageServer, pRecord);
			break;
		case STORAGE_OP_TYPE_REPLICA_UPDATE_FILE:
			STARAGE_CHECK_IF_NEED_SYNC_OLD(pReader, pRecord)
			result = storage_sync_copy_file(pStorageServer, \
				pRecord, STORAGE_PROTO_CMD_SYNC_UPDATE_FILE);
			break;
		case STORAGE_OP_TYPE_REPLICA_CREATE_LINK:
			STARAGE_CHECK_IF_NEED_SYNC_OLD(pReader, pRecord)
			result = storage_sync_link_file(pStorageServer, \
				pRecord);
			break;
		default:
			return EINVAL;
	}

	if (result == 0)
	{
		pReader->sync_row_count++;
	}

	return result;
}

static int write_to_binlog_index()
{
	char full_filename[MAX_PATH_SIZE];
	char buff[16];
	int fd;
	int len;

	snprintf(full_filename, sizeof(full_filename), \
			"%s/data/"SYNC_DIR_NAME"/%s", g_base_path, \
			SYNC_BINLOG_INDEX_FILENAME);
	if ((fd=open(full_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open file \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, full_filename, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	len = sprintf(buff, "%d", g_binlog_index);
	if (write(fd, buff, len) != len)
	{
		logError("file: "__FILE__", line: %d, " \
			"write to file \"%s\" fail, " \
			"errno: %d, error info: %s",  \
			__LINE__, full_filename, \
			errno, strerror(errno));
		close(fd);
		return errno != 0 ? errno : EIO;
	}

	close(fd);
	return 0;
}

static char *get_writable_binlog_filename(char *full_filename)
{
	static char buff[MAX_PATH_SIZE];

	if (full_filename == NULL)
	{
		full_filename = buff;
	}

	snprintf(full_filename, MAX_PATH_SIZE, \
			"%s/data/"SYNC_DIR_NAME"/"SYNC_BINLOG_FILE_PREFIX"" \
			SYNC_BINLOG_FILE_EXT_FMT, \
			g_base_path, g_binlog_index);
	return full_filename;
}

static int open_next_writable_binlog()
{
	char full_filename[MAX_PATH_SIZE];

	if (g_binlog_fd >= 0)
	{
		close(g_binlog_fd);
		g_binlog_fd = -1;
	}

	get_writable_binlog_filename(full_filename);
	if (fileExists(full_filename))
	{
		if (unlink(full_filename) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"unlink file \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, full_filename, \
				errno, strerror(errno));
			return errno != 0 ? errno : ENOENT;
		}

		logError("file: "__FILE__", line: %d, " \
			"binlog file \"%s\" already exists, truncate", \
			__LINE__, full_filename);
	}

	g_binlog_fd = open(full_filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
	if (g_binlog_fd < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open file \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, full_filename, \
			errno, strerror(errno));
		return errno != 0 ? errno : EACCES;
	}

	return 0;
}

int storage_sync_init()
{
	char data_path[MAX_PATH_SIZE];
	char sync_path[MAX_PATH_SIZE];
	char full_filename[MAX_PATH_SIZE];
	char file_buff[64];
	int bytes;
	int result;
	int fd;

	snprintf(data_path, sizeof(data_path), "%s/data", g_base_path);
	if (!fileExists(data_path))
	{
		if (mkdir(data_path, 0755) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"mkdir \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, data_path, \
				errno, strerror(errno));
			return errno != 0 ? errno : ENOENT;
		}
	}

	snprintf(sync_path, sizeof(sync_path), \
			"%s/"SYNC_DIR_NAME, data_path);
	if (!fileExists(sync_path))
	{
		if (mkdir(sync_path, 0755) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"mkdir \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, sync_path, \
				errno, strerror(errno));
			return errno != 0 ? errno : ENOENT;
		}
	}

	binlog_write_cache_buff = (char *)malloc(SYNC_BINLOG_WRITE_BUFF_SIZE);
	if (binlog_write_cache_buff == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, SYNC_BINLOG_WRITE_BUFF_SIZE, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	snprintf(full_filename, sizeof(full_filename), \
			"%s/%s", sync_path, SYNC_BINLOG_INDEX_FILENAME);
	if ((fd=open(full_filename, O_RDONLY)) >= 0)
	{
		bytes = read(fd, file_buff, sizeof(file_buff) - 1);
		close(fd);
		if (bytes <= 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"read file \"%s\" fail, bytes read: %d", \
				__LINE__, full_filename, bytes);
			return errno != 0 ? errno : EIO;
		}

		file_buff[bytes] = '\0';
		g_binlog_index = atoi(file_buff);
		if (g_binlog_index < 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"in file \"%s\", binlog_index: %d < 0", \
				__LINE__, full_filename, g_binlog_index);
			return EINVAL;
		}
	}
	else
	{
		g_binlog_index = 0;
		if ((result=write_to_binlog_index()) != 0)
		{
			return result;
		}
	}

	get_writable_binlog_filename(full_filename);
	g_binlog_fd = open(full_filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
	if (g_binlog_fd < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open file \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, full_filename, \
			errno, strerror(errno));
		return errno != 0 ? errno : EACCES;
	}

	binlog_file_size = lseek(g_binlog_fd, 0, SEEK_END);
	if (binlog_file_size < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"ftell file \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, full_filename, \
			errno, strerror(errno));
		storage_sync_destroy();
		return errno != 0 ? errno : EIO;
	}

	/*
	//printf("full_filename=%s, binlog_file_size=%d\n", \
			full_filename, binlog_file_size);
	*/
	
	if ((result=init_pthread_lock(&sync_thread_lock)) != 0)
	{
		return result;
	}

	load_local_host_ip_addrs();

	return 0;
}

int storage_sync_destroy()
{
	int result;

	if (g_binlog_fd >= 0)
	{
		storage_binlog_fsync(true);
		close(g_binlog_fd);
		g_binlog_fd = -1;
	}

	if ((result=pthread_mutex_destroy(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_destroy fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	if (binlog_write_cache_buff != NULL)
	{
		free(binlog_write_cache_buff);
		binlog_write_cache_buff = NULL;
	}


	return 0;
}

int kill_storage_sync_threads()
{
	int result;
	int kill_res;

	if (sync_tids == NULL)
	{
		return 0;
	}

	if ((result=pthread_mutex_lock(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	kill_res = kill_work_threads(sync_tids, g_storage_sync_thread_count);

	if ((result=pthread_mutex_unlock(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return kill_res;
}

int fdfs_binlog_sync_func(void *args)
{
	if (binlog_write_cache_len > 0)
	{
		return storage_binlog_fsync(true);
	}
	else
	{
		return 0;
	}
}

static int storage_binlog_fsync(const bool bNeedLock)
{
	int result;
	int write_ret;

	if (bNeedLock && (result=pthread_mutex_lock(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	if (binlog_write_cache_len == 0) //ignore
	{
		write_ret = 0;  //skip
	}
	else if (write(g_binlog_fd, binlog_write_cache_buff, \
		binlog_write_cache_len) != binlog_write_cache_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"write to binlog file \"%s\" fail, " \
			"errno: %d, error info: %s",  \
			__LINE__, get_writable_binlog_filename(NULL), \
			errno, strerror(errno));
		write_ret = errno != 0 ? errno : EIO;
	}
	else if (fsync(g_binlog_fd) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"sync to binlog file \"%s\" fail, " \
			"errno: %d, error info: %s",  \
			__LINE__, get_writable_binlog_filename(NULL), \
			errno, strerror(errno));
		write_ret = errno != 0 ? errno : EIO;
	}
	else
	{
		binlog_file_size += binlog_write_cache_len;
		if (binlog_file_size >= SYNC_BINLOG_FILE_MAX_SIZE)
		{
			g_binlog_index++;
			if ((write_ret=write_to_binlog_index()) == 0)
			{
				write_ret = open_next_writable_binlog();
			}

			binlog_file_size = 0;
			if (write_ret != 0)
			{
				g_continue_flag = false;
				logCrit("file: "__FILE__", line: %d, " \
					"open binlog file \"%s\" fail, " \
					"program exit!", \
					__LINE__, \
					get_writable_binlog_filename(NULL));
			}
		}
		else
		{
			write_ret = 0;
		}
	}

	binlog_write_version++;
	binlog_write_cache_len = 0;  //reset cache buff

	if (bNeedLock && (result=pthread_mutex_unlock(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return write_ret;
}

int storage_binlog_write(const int timestamp, const char op_type, \
		const char *filename)
{
	int result;
	int write_ret;

	if ((result=pthread_mutex_lock(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	binlog_write_cache_len += sprintf(binlog_write_cache_buff + \
				binlog_write_cache_len, "%d %c %s\n", \
				timestamp, op_type, filename);

	//check if buff full
	if (SYNC_BINLOG_WRITE_BUFF_SIZE - binlog_write_cache_len < 256)
	{
		write_ret = storage_binlog_fsync(false);  //sync to disk
	}
	else
	{
		write_ret = 0;
	}

	if ((result=pthread_mutex_unlock(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return write_ret;
}

static char *get_binlog_readable_filename(BinLogReader *pReader, \
		char *full_filename)
{
	static char buff[MAX_PATH_SIZE];

	if (full_filename == NULL)
	{
		full_filename = buff;
	}

	snprintf(full_filename, MAX_PATH_SIZE, \
			"%s/data/"SYNC_DIR_NAME"/"SYNC_BINLOG_FILE_PREFIX"" \
			SYNC_BINLOG_FILE_EXT_FMT, \
			g_base_path, pReader->binlog_index);
	return full_filename;
}

static int storage_open_readable_binlog(BinLogReader *pReader)
{
	char full_filename[MAX_PATH_SIZE];

	if (pReader->binlog_fd >= 0)
	{
		close(pReader->binlog_fd);
	}

	get_binlog_readable_filename(pReader, full_filename);
	pReader->binlog_fd = open(full_filename, O_RDONLY);
	if (pReader->binlog_fd < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open binlog file \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, full_filename, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (pReader->binlog_offset > 0 && \
	    lseek(pReader->binlog_fd, pReader->binlog_offset, SEEK_SET) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"seek binlog file \"%s\" fail, file offset=" \
			INT64_PRINTF_FORMAT", errno: %d, error info: %s", \
			__LINE__, full_filename, pReader->binlog_offset, \
			errno, strerror(errno));

		close(pReader->binlog_fd);
		pReader->binlog_fd = -1;
		return errno != 0 ? errno : ESPIPE;
	}

	return 0;
}

static char *get_mark_filename_by_reader(const void *pArg, \
			char *full_filename)
{
	const BinLogReader *pReader;
	static char buff[MAX_PATH_SIZE];

	pReader = (const BinLogReader *)pArg;
	if (full_filename == NULL)
	{
		full_filename = buff;
	}

	snprintf(full_filename, MAX_PATH_SIZE, \
			"%s/data/"SYNC_DIR_NAME"/%s_%d%s", g_base_path, \
			pReader->ip_addr, g_server_port, SYNC_MARK_FILE_EXT);
	return full_filename;
}

static char *get_mark_filename_by_ip(const char *ip_addr, char *full_filename, \
		const int filename_size)
{
	snprintf(full_filename, filename_size, \
			"%s/data/"SYNC_DIR_NAME"/%s_%d%s", g_base_path, \
			ip_addr, g_server_port, SYNC_MARK_FILE_EXT);
	return full_filename;
}

static int storage_report_storage_status(const char *ip_addr, \
			const char status)
{
	FDFSStorageBrief briefServer;
	TrackerServerInfo trackerServer;
	TrackerServerInfo *pGlobalServer;
	TrackerServerInfo *pTServer;
	TrackerServerInfo *pTServerEnd;
	int result;
	int i;

	strcpy(briefServer.ip_addr, ip_addr);
	briefServer.status = status;

	if (!g_sync_old_done)
	{
		while (g_continue_flag && !g_sync_old_done)
		{
			sleep(1);
		}

		if (!g_continue_flag)
		{
			return 0;
		}
	}

	result = 0;
	pTServer = &trackerServer;
	pTServerEnd = g_tracker_group.servers + g_tracker_group.server_count;
	for (pGlobalServer=g_tracker_group.servers; pGlobalServer<pTServerEnd; \
			pGlobalServer++)
	{
		memcpy(pTServer, pGlobalServer, sizeof(TrackerServerInfo));
		for (i=0; i < 3; i++)
		{
			pTServer->sock = socket(AF_INET, SOCK_STREAM, 0);
			if(pTServer->sock < 0)
			{
				result = errno != 0 ? errno : EPERM;
				logError("file: "__FILE__", line: %d, " \
					"socket create failed, errno: %d, " \
					"error info: %s.", \
					__LINE__, result, strerror(result));
				sleep(5);
				break;
			}

			if (g_client_bind_addr && *g_bind_addr != '\0')
			{
				socketBind(pTServer->sock, g_bind_addr, 0);
			}

			if ((result=connectserverbyip(pTServer->sock, \
				pTServer->ip_addr, pTServer->port)) == 0)
			{
				break;
			}

			close(pTServer->sock);
			pTServer->sock = -1;
			sleep(5);
		}

		if (pTServer->sock < 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"connect to tracker server %s:%d fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pTServer->ip_addr, pTServer->port, \
				result, strerror(result));

			continue;
		}

		if (tcpsetnonblockopt(pTServer->sock) != 0)
		{
			close(pTServer->sock);
			continue;
		}

		if ((result=tracker_report_storage_status(pTServer, \
			&briefServer)) != 0)
		{
		}

		fdfs_quit(pTServer);
		close(pTServer->sock);
	}

	return 0;
}

static int storage_reader_sync_init_req(BinLogReader *pReader)
{
	TrackerServerInfo *pTrackerServers;
	TrackerServerInfo *pTServer;
	TrackerServerInfo *pTServerEnd;
	char tracker_client_ip[IP_ADDRESS_SIZE];
	int result;
	int conn_ret;

	if (!g_sync_old_done)
	{
		while (g_continue_flag && !g_sync_old_done)
		{
			sleep(1);
		}

		if (!g_continue_flag)
		{
			return EINTR;
		}
	}

	pTrackerServers = (TrackerServerInfo *)malloc( \
		sizeof(TrackerServerInfo) * g_tracker_group.server_count);
	if (pTrackerServers == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", __LINE__, \
			(int)sizeof(TrackerServerInfo) * \
			g_tracker_group.server_count);
		return errno != 0 ? errno : ENOMEM;
	}

	memcpy(pTrackerServers, g_tracker_group.servers, \
		sizeof(TrackerServerInfo) * g_tracker_group.server_count);
	pTServerEnd = pTrackerServers + g_tracker_group.server_count;
	for (pTServer=pTrackerServers; pTServer<pTServerEnd; pTServer++)
	{
		pTServer->sock = -1;
	}

	result = EINTR;
	pTServer = pTrackerServers;
	do
	{
		while (g_continue_flag)
		{
			pTServer->sock = socket(AF_INET, SOCK_STREAM, 0);
			if(pTServer->sock < 0)
			{
				logCrit("file: "__FILE__", line: %d, " \
					"socket create failed, errno: %d, " \
					"error info: %s. program exit!", \
					__LINE__, errno, strerror(errno));
				g_continue_flag = false;
				result = errno != 0 ? errno : EPERM;
				break;
			}

			if (g_client_bind_addr && *g_bind_addr != '\0')
			{
				socketBind(pTServer->sock, g_bind_addr, 0);
			}

			if ((conn_ret=connectserverbyip(pTServer->sock, \
				pTServer->ip_addr, pTServer->port)) == 0)
			{
				break;
			}

			logError("file: "__FILE__", line: %d, " \
				"connect to tracker server %s:%d fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pTServer->ip_addr, pTServer->port, \
				conn_ret, strerror(conn_ret));

			close(pTServer->sock);

			pTServer++;
			if (pTServer >= pTServerEnd)
			{
				pTServer = pTrackerServers;
			}

			sleep(g_heart_beat_interval);
		}

		if (!g_continue_flag)
		{
			break;
		}

		if (tcpsetnonblockopt(pTServer->sock) != 0)
		{
			close(pTServer->sock);
			continue;
		}

		getSockIpaddr(pTServer->sock, \
				tracker_client_ip, IP_ADDRESS_SIZE);
		insert_into_local_host_ip(tracker_client_ip);

		if ((result=tracker_sync_src_req(pTServer, pReader)) != 0)
		{
			fdfs_quit(pTServer);
			close(pTServer->sock);
			sleep(g_heart_beat_interval);
			continue;
		}

		fdfs_quit(pTServer);
		close(pTServer->sock);

	} while (0);

	free(pTrackerServers);

	/*
	//printf("need_sync_old=%d, until_timestamp=%d\n", \
		pReader->need_sync_old, pReader->until_timestamp);
	*/

	return result;
}

static int storage_reader_init(FDFSStorageBrief *pStorage, \
			BinLogReader *pReader)
{
	char full_filename[MAX_PATH_SIZE];
	IniContext iniContext;
	int result;
	bool bFileExist;
	bool bNeedSyncOld;

	memset(pReader, 0, sizeof(BinLogReader));
	pReader->mark_fd = -1;
	pReader->binlog_fd = -1;

	pReader->binlog_buff.buffer = (char *)malloc( \
				STORAGE_BINLOG_BUFFER_SIZE);
	if (pReader->binlog_buff.buffer == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, STORAGE_BINLOG_BUFFER_SIZE, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	pReader->binlog_buff.current = pReader->binlog_buff.buffer;

	strcpy(pReader->ip_addr, pStorage->ip_addr);
	get_mark_filename_by_reader(pReader, full_filename);

	if (pStorage->status <= FDFS_STORAGE_STATUS_WAIT_SYNC)
	{
		bFileExist = false;
	}
	else
	{
		bFileExist = fileExists(full_filename);
	}

	if (!bFileExist)
	{
		if ((result=storage_reader_sync_init_req(pReader)) != 0)
		{
			return result;
		}
	}

	if (bFileExist)
	{
		memset(&iniContext, 0, sizeof(IniContext));
		if ((result=iniLoadFromFile(full_filename, &iniContext)) \
			 != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"load from mark file \"%s\" fail, " \
				"error code: %d", \
				__LINE__, full_filename, result);
			return result;
		}

		if (iniContext.global.count < 7)
		{
			iniFreeContext(&iniContext);
			logError("file: "__FILE__", line: %d, " \
				"in mark file \"%s\", item count: %d < 7", \
				__LINE__, full_filename, iniContext.global.count);
			return ENOENT;
		}

		bNeedSyncOld = iniGetBoolValue(NULL,  \
				MARK_ITEM_NEED_SYNC_OLD, \
				&iniContext, false);
		if (pStorage->status == FDFS_STORAGE_STATUS_SYNCING)
		{
			if ((result=storage_reader_sync_init_req(pReader)) != 0)
			{
				iniFreeContext(&iniContext);
				return result;
			}

			if (pReader->need_sync_old && !bNeedSyncOld)
			{
				bFileExist = false;  //re-sync
			}
			else
			{
				pReader->need_sync_old = bNeedSyncOld;
			}
		}
		else
		{
			pReader->need_sync_old = bNeedSyncOld;
		}

		if (bFileExist)
		{
			pReader->binlog_index = iniGetIntValue(NULL, \
					MARK_ITEM_BINLOG_FILE_INDEX, \
					&iniContext, -1);
			pReader->binlog_offset = iniGetInt64Value(NULL, \
					MARK_ITEM_BINLOG_FILE_OFFSET, \
					&iniContext, -1);
			pReader->sync_old_done = iniGetBoolValue(NULL,  \
					MARK_ITEM_SYNC_OLD_DONE, \
					&iniContext, false);
			pReader->until_timestamp = iniGetIntValue(NULL, \
					MARK_ITEM_UNTIL_TIMESTAMP, \
					&iniContext, -1);
			pReader->scan_row_count = iniGetInt64Value(NULL, \
					MARK_ITEM_SCAN_ROW_COUNT, \
					&iniContext, 0);
			pReader->sync_row_count = iniGetInt64Value(NULL, \
					MARK_ITEM_SYNC_ROW_COUNT, \
					&iniContext, 0);

			if (pReader->binlog_index < 0)
			{
				iniFreeContext(&iniContext);
				logError("file: "__FILE__", line: %d, " \
					"in mark file \"%s\", " \
					"binlog_index: %d < 0", \
					__LINE__, full_filename, \
					pReader->binlog_index);
				return EINVAL;
			}
			if (pReader->binlog_offset < 0)
			{
				iniFreeContext(&iniContext);
				logError("file: "__FILE__", line: %d, " \
					"in mark file \"%s\", binlog_offset: "\
					INT64_PRINTF_FORMAT" < 0", \
					__LINE__, full_filename, \
					pReader->binlog_offset);
				return EINVAL;
			}
		}

		iniFreeContext(&iniContext);
	}

	pReader->last_write_row_count = pReader->scan_row_count;

	pReader->mark_fd = open(full_filename, O_WRONLY | O_CREAT, 0644);
	if (pReader->mark_fd < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open mark file \"%s\" fail, " \
			"error no: %d, error info: %s", \
			__LINE__, full_filename, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if ((result=storage_open_readable_binlog(pReader)) != 0)
	{
		close(pReader->mark_fd);
		pReader->mark_fd = -1;
		return result;
	}

	if (!bFileExist)
	{
        	if (!pReader->need_sync_old && pReader->until_timestamp > 0)
		{
			if ((result=storage_binlog_reader_skip(pReader)) != 0)
			{
				storage_reader_destroy(pReader);
				return result;
			}
		}

		if ((result=storage_write_to_mark_file(pReader)) != 0)
		{
			storage_reader_destroy(pReader);
			return result;
		}
	}

	result = storage_binlog_preread(pReader);
	if (result != 0 && result != ENOENT)
	{
		storage_reader_destroy(pReader);
		return result;
	}

	return 0;
}

static void storage_reader_destroy(BinLogReader *pReader)
{
	if (pReader->mark_fd >= 0)
	{
		close(pReader->mark_fd);
		pReader->mark_fd = -1;
	}

	if (pReader->binlog_fd >= 0)
	{
		close(pReader->binlog_fd);
		pReader->binlog_fd = -1;
	}

	if (pReader->binlog_buff.buffer != NULL)
	{
		free(pReader->binlog_buff.buffer);
		pReader->binlog_buff.buffer = NULL;
		pReader->binlog_buff.current = NULL;
		pReader->binlog_buff.length = 0;
	}
}

static int storage_write_to_mark_file(BinLogReader *pReader)
{
	char buff[256];
	int len;
	int result;

	len = sprintf(buff, \
		"%s=%d\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s=%d\n"  \
		"%s=%d\n"  \
		"%s=%d\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n", \
		MARK_ITEM_BINLOG_FILE_INDEX, pReader->binlog_index, \
		MARK_ITEM_BINLOG_FILE_OFFSET, pReader->binlog_offset, \
		MARK_ITEM_NEED_SYNC_OLD, pReader->need_sync_old, \
		MARK_ITEM_SYNC_OLD_DONE, pReader->sync_old_done, \
		MARK_ITEM_UNTIL_TIMESTAMP, (int)pReader->until_timestamp, \
		MARK_ITEM_SCAN_ROW_COUNT, pReader->scan_row_count, \
		MARK_ITEM_SYNC_ROW_COUNT, pReader->sync_row_count);

	if ((result=storage_write_to_fd(pReader->mark_fd, \
		get_mark_filename_by_reader, pReader, buff, len)) == 0)
	{
		pReader->last_write_row_count = pReader->scan_row_count;
	}

	return result;
}

static int rewind_to_prev_rec_end(BinLogReader *pReader)
{
	if (lseek(pReader->binlog_fd, pReader->binlog_offset, SEEK_SET) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"seek binlog file \"%s\"fail, " \
			"file offset: "INT64_PRINTF_FORMAT", " \
			"errno: %d, error info: %s", \
			__LINE__, get_binlog_readable_filename(pReader, NULL), \
			pReader->binlog_offset, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	pReader->binlog_buff.current = pReader->binlog_buff.buffer;
	pReader->binlog_buff.length = 0;

	return 0;
}

static int storage_binlog_preread(BinLogReader *pReader)
{
	int bytes_read;
	int saved_binlog_write_version;

	if (pReader->binlog_buff.version == binlog_write_version && \
		pReader->binlog_buff.length == 0)
	{
		return ENOENT;
	}

	saved_binlog_write_version = binlog_write_version;
	if (pReader->binlog_buff.current != pReader->binlog_buff.buffer)
	{
		if (pReader->binlog_buff.length > 0)
		{
			memcpy(pReader->binlog_buff.buffer, \
				pReader->binlog_buff.current, \
				pReader->binlog_buff.length);
		}

		pReader->binlog_buff.current = pReader->binlog_buff.buffer;
	}

	bytes_read = read(pReader->binlog_fd, pReader->binlog_buff.buffer \
		+ pReader->binlog_buff.length, \
		STORAGE_BINLOG_BUFFER_SIZE - pReader->binlog_buff.length);
	if (bytes_read < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"read from binlog file \"%s\" fail, " \
			"file offset: "INT64_PRINTF_FORMAT", " \
			"error no: %d, error info: %s", __LINE__, \
			get_binlog_readable_filename(pReader, NULL), \
			pReader->binlog_offset + pReader->binlog_buff.length, \
			errno, strerror(errno));
		return errno != 0 ? errno : EIO;
	}
	else if (bytes_read == 0) //end of binlog file
	{
		pReader->binlog_buff.version = saved_binlog_write_version;
		return ENOENT;
	}

	pReader->binlog_buff.length += bytes_read;
	return 0;
}

static int storage_binlog_do_line_read(BinLogReader *pReader, \
		char *line, const int line_size, int *line_length)
{
	char *pLineEnd;

	if (pReader->binlog_buff.length == 0)
	{
		return ENOENT;
	}

	pLineEnd = (char *)memchr(pReader->binlog_buff.current, '\n', \
			pReader->binlog_buff.length);
	if (pLineEnd == NULL)
	{
		return ENOENT;
	}

	*line_length = (pLineEnd - pReader->binlog_buff.current) + 1;
	if (*line_length >= line_size)
	{
		logError("file: "__FILE__", line: %d, " \
			"read from binlog file \"%s\" fail, " \
			"file offset: "INT64_PRINTF_FORMAT", " \
			"line buffer size: %d is too small! " \
			"<= line length: %d", __LINE__, \
			get_binlog_readable_filename(pReader, NULL), \
			pReader->binlog_offset, line_size, *line_length);
		return ENOSPC;
	}

	memcpy(line, pReader->binlog_buff.current, *line_length);
	*(line + *line_length) = '\0';

	pReader->binlog_buff.current = pLineEnd + 1;
	pReader->binlog_buff.length -= *line_length;

	return 0;
}

static int storage_binlog_read_line(BinLogReader *pReader, \
		char *line, const int line_size, int *line_length)
{
	int result;

	result = storage_binlog_do_line_read(pReader, line, \
			line_size, line_length);
	if (result != ENOENT)
	{
		return result;
	}

	result = storage_binlog_preread(pReader);
	if (result != 0)
	{
		return result;
	}

	return storage_binlog_do_line_read(pReader, line, \
			line_size, line_length);
}

static int storage_binlog_read(BinLogReader *pReader, \
			BinLogRecord *pRecord, int *record_length)
{
	char line[256];
	char *cols[3];
	int result;

	while (1)
	{
		result = storage_binlog_read_line(pReader, line, \
				sizeof(line), record_length);
		if (result == 0)
		{
			break;
		}
		else if (result != ENOENT)
		{
			return result;
		}

		if (pReader->binlog_index >= g_binlog_index)
		{
			return ENOENT;
		}

		//rotate
		if (pReader->binlog_buff.length != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"binlog file \"%s\" not ended by \\n, " \
				"file offset: "INT64_PRINTF_FORMAT, __LINE__, \
				get_binlog_readable_filename(pReader, NULL), \
				pReader->binlog_offset);
			return EINVAL;
		}

		pReader->binlog_index++;
		pReader->binlog_offset = 0;
		if ((result=storage_open_readable_binlog(pReader)) != 0)
		{
			return result;
		}

		if ((result=storage_write_to_mark_file(pReader)) != 0)
		{
			return result;
		}
	}

	if ((result=splitEx(line, ' ', cols, 3)) < 3)
	{
		logError("file: "__FILE__", line: %d, " \
			"read data from binlog file \"%s\" fail, " \
			"file offset: "INT64_PRINTF_FORMAT", " \
			"read item count: %d < 3", \
			__LINE__, get_binlog_readable_filename(pReader, NULL), \
			pReader->binlog_offset, result);
		return ENOENT;
	}

	pRecord->timestamp = atoi(cols[0]);
	pRecord->op_type = *(cols[1]);
	pRecord->filename_len = strlen(cols[2]) - 1; //need trim new line \n
	if (pRecord->filename_len > sizeof(pRecord->filename)-1)
	{
		logError("file: "__FILE__", line: %d, " \
			"item \"filename\" in binlog " \
			"file \"%s\" is invalid, file offset: " \
			INT64_PRINTF_FORMAT", filename length: %d > %d", \
			__LINE__, get_binlog_readable_filename(pReader, NULL), \
			pReader->binlog_offset, \
			pRecord->filename_len, (int)sizeof(pRecord->filename)-1);
		return EINVAL;
	}

	memcpy(pRecord->filename, cols[2], pRecord->filename_len);
	*(pRecord->filename + pRecord->filename_len) = '\0';
	pRecord->true_filename_len = pRecord->filename_len;
	if ((result=storage_split_filename(pRecord->filename, \
			&pRecord->true_filename_len, pRecord->true_filename, \
			&pRecord->pBasePath)) != 0)
	{
		return result;
	}

	/*
	//printf("timestamp=%d, op_type=%c, filename=%s(%d), line length=%d, " \
		"offset=%d\n", \
		pRecord->timestamp, pRecord->op_type, \
		pRecord->filename, strlen(pRecord->filename), \
		*record_length, pReader->binlog_offset);
	*/

	return 0;
}

static int storage_binlog_reader_skip(BinLogReader *pReader)
{
	BinLogRecord record;
	int result;
	int record_len;

	while (1)
	{
		result = storage_binlog_read(pReader, \
				&record, &record_len);
		if (result != 0)
		{
			if (result == ENOENT)
			{
				return 0;
			}

			return result;
		}

		if (record.timestamp >= pReader->until_timestamp)
		{
			result = rewind_to_prev_rec_end(pReader);
			break;
		}

		pReader->binlog_offset += record_len;
	}

	return result;
}

int storage_unlink_mark_file(const char *ip_addr)
{
	char old_filename[MAX_PATH_SIZE];
	char new_filename[MAX_PATH_SIZE];
	time_t t;
	struct tm tm;

	t = time(NULL);
	localtime_r(&t, &tm);

	get_mark_filename_by_ip(ip_addr, old_filename, sizeof(old_filename));
	if (!fileExists(old_filename))
	{
		return ENOENT;
	}

	snprintf(new_filename, sizeof(new_filename), \
		"%s.%04d%02d%02d%02d%02d%02d", old_filename, \
		tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, \
		tm.tm_hour, tm.tm_min, tm.tm_sec);
	if (rename(old_filename, new_filename) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"rename file %s to %s fail" \
			", errno: %d, error info: %s", \
			__LINE__, old_filename, new_filename, \
			errno, strerror(errno));
		return errno != 0 ? errno : EACCES;
	}

	return 0;
}

int storage_rename_mark_file(const char *old_ip_addr, const char *new_ip_addr)
{
	char old_filename[MAX_PATH_SIZE];
	char new_filename[MAX_PATH_SIZE];

	get_mark_filename_by_ip(old_ip_addr, old_filename,sizeof(old_filename));
	if (!fileExists(old_filename))
	{
		return ENOENT;
	}

	get_mark_filename_by_ip(new_ip_addr, new_filename,sizeof(new_filename));
	if (fileExists(new_filename))
	{
		logError("file: "__FILE__", line: %d, " \
			"mark file %s already exists, " \
			"ignore rename file %s to %s", \
			__LINE__, new_filename, old_filename, new_filename);
		return EEXIST;
	}

	get_mark_filename_by_ip(new_ip_addr, new_filename,sizeof(new_filename));
	if (rename(old_filename, new_filename) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"rename file %s to %s fail" \
			", errno: %d, error info: %s", \
			__LINE__, old_filename, new_filename, \
			errno, strerror(errno));
		return errno != 0 ? errno : EACCES;
	}

	return 0;
}

static void storage_sync_get_start_end_times(time_t current_time, \
	const TimeInfo *pStartTime, const TimeInfo *pEndTime, \
	time_t *start_time, time_t *end_time)
{
	struct tm tm_time;
	//char buff[32];

	localtime_r(&current_time, &tm_time);
	tm_time.tm_sec = 0;

	/*
	strftime(buff, sizeof(buff), "%Y-%m-%d %H:%M:%S", &tm_time);
	//printf("current time: %s\n", buff);
	*/

	tm_time.tm_hour = pStartTime->hour;
	tm_time.tm_min = pStartTime->minute;
	*start_time = mktime(&tm_time);

	//end time < start time
	if (pEndTime->hour < pStartTime->hour || (pEndTime->hour == \
		pStartTime->hour && pEndTime->minute < pStartTime->minute))
	{
		current_time += 24 * 3600;
		localtime_r(&current_time, &tm_time);
		tm_time.tm_sec = 0;
	}

	tm_time.tm_hour = pEndTime->hour;
	tm_time.tm_min = pEndTime->minute;
	*end_time = mktime(&tm_time);
}

static void storage_sync_thread_exit(TrackerServerInfo *pStorage)
{
	int result;
	int i;
	pthread_t tid;

	if ((result=pthread_mutex_lock(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	tid = pthread_self();
	for (i=0; i<g_storage_sync_thread_count; i++)
	{
		if (pthread_equal(sync_tids[i], tid))
		{
			break;
		}
	}

	while (i < g_storage_sync_thread_count - 1)
	{
		sync_tids[i] = sync_tids[i + 1];
		i++;
	}
	
	g_storage_sync_thread_count--;

	if ((result=pthread_mutex_unlock(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	logDebug("file: "__FILE__", line: %d, " \
		"sync thread to storage server %s:%d exit", 
		__LINE__, pStorage->ip_addr, pStorage->port);
}

static void* storage_sync_thread_entrance(void* arg)
{
	FDFSStorageBrief *pStorage;
	BinLogReader reader;
	BinLogRecord record;
	TrackerServerInfo storage_server;
	char local_ip_addr[IP_ADDRESS_SIZE];
	int read_result;
	int sync_result;
	int conn_result;
	int result;
	int record_len;
	int previousCode;
	int nContinuousFail;
	time_t current_time;
	time_t start_time;
	time_t end_time;
	
	memset(local_ip_addr, 0, sizeof(local_ip_addr));
	memset(&reader, 0, sizeof(reader));

	current_time =  time(NULL);
	start_time = 0;
	end_time = 0;

	pStorage = (FDFSStorageBrief *)arg;

	strcpy(storage_server.ip_addr, pStorage->ip_addr);
	strcpy(storage_server.group_name, g_group_name);
	storage_server.port = g_server_port;
	storage_server.sock = -1;

	logDebug("file: "__FILE__", line: %d, " \
		"sync thread to storage server %s:%d started", \
		__LINE__, storage_server.ip_addr, storage_server.port);

	while (g_continue_flag && \
		pStorage->status != FDFS_STORAGE_STATUS_DELETED && \
		pStorage->status != FDFS_STORAGE_STATUS_IP_CHANGED && \
		pStorage->status != FDFS_STORAGE_STATUS_NONE)
	{
		while (g_continue_flag && \
			(pStorage->status == FDFS_STORAGE_STATUS_INIT ||
			 pStorage->status == FDFS_STORAGE_STATUS_OFFLINE ||
			 pStorage->status == FDFS_STORAGE_STATUS_ONLINE))
		{
			sleep(1);
		}

		if ((!g_continue_flag) ||
			pStorage->status == FDFS_STORAGE_STATUS_DELETED || \
			pStorage->status == FDFS_STORAGE_STATUS_IP_CHANGED || \
			pStorage->status == FDFS_STORAGE_STATUS_NONE)
		{
			break;
		}

		if (g_sync_part_time)
		{
			current_time = time(NULL);
			storage_sync_get_start_end_times(current_time, \
				&g_sync_end_time, &g_sync_start_time, \
				&start_time, &end_time);
			start_time += 60;
			end_time -= 60;
			while (g_continue_flag && (current_time >= start_time \
					&& current_time <= end_time))
			{
				current_time = time(NULL);
				sleep(1);
			}
		}

		previousCode = 0;
		nContinuousFail = 0;
		conn_result = 0;
		while (g_continue_flag && \
			pStorage->status != FDFS_STORAGE_STATUS_DELETED && \
			pStorage->status != FDFS_STORAGE_STATUS_IP_CHANGED && \
			pStorage->status != FDFS_STORAGE_STATUS_NONE)
		{
			storage_server.sock = \
				socket(AF_INET, SOCK_STREAM, 0);
			if(storage_server.sock < 0)
			{
				logCrit("file: "__FILE__", line: %d," \
					" socket create fail, " \
					"errno: %d, error info: %s. " \
					"program exit!", __LINE__, \
					errno, strerror(errno));
				g_continue_flag = false;
				break;
			}

			if (g_client_bind_addr && *g_bind_addr != '\0')
			{
				socketBind(storage_server.sock, g_bind_addr, 0);
			}

			if ((conn_result=connectserverbyip(storage_server.sock,\
				storage_server.ip_addr, g_server_port)) == 0)
			{
				char szFailPrompt[64];
				if (nContinuousFail == 0)
				{
					*szFailPrompt = '\0';
				}
				else
				{
					sprintf(szFailPrompt, \
						", continuous fail count: %d", \
						nContinuousFail);
				}
				logInfo("file: "__FILE__", line: %d, " \
					"successfully connect to " \
					"storage server %s:%d%s", __LINE__, \
					storage_server.ip_addr, \
					g_server_port, szFailPrompt);
				nContinuousFail = 0;
				break;
			}

			if (previousCode != conn_result)
			{
				logError("file: "__FILE__", line: %d, " \
					"connect to storage server %s:%d fail" \
					", errno: %d, error info: %s", \
					__LINE__, \
					storage_server.ip_addr, g_server_port, \
					conn_result, strerror(conn_result));
				previousCode = conn_result;
			}

			nContinuousFail++;
			close(storage_server.sock);
			storage_server.sock = -1;

			if (!g_continue_flag)
			{
				break;
			}

			sleep(1);
		}

		if (nContinuousFail > 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"connect to storage server %s:%d fail, " \
				"try count: %d, errno: %d, error info: %s", \
				__LINE__, storage_server.ip_addr, \
				g_server_port, nContinuousFail, \
				conn_result, strerror(conn_result));
		}

		if ((!g_continue_flag) ||
			pStorage->status == FDFS_STORAGE_STATUS_DELETED || \
			pStorage->status == FDFS_STORAGE_STATUS_IP_CHANGED || \
			pStorage->status == FDFS_STORAGE_STATUS_NONE)
		{
			break;
		}

		if (pStorage->status != FDFS_STORAGE_STATUS_ACTIVE && \
			pStorage->status != FDFS_STORAGE_STATUS_WAIT_SYNC && \
			pStorage->status != FDFS_STORAGE_STATUS_SYNCING)
		{
			close(storage_server.sock);
			continue;
		}

		if ((result=storage_reader_init(pStorage, &reader)) != 0)
		{
			logCrit("file: "__FILE__", line: %d, " \
				"storage_reader_init fail, errno=%d, " \
				"program exit!", \
				__LINE__, result);
			g_continue_flag = false;
			break;
		}

		if (!reader.need_sync_old)
		{
			while (g_continue_flag && \
			(pStorage->status != FDFS_STORAGE_STATUS_ACTIVE && \
			 pStorage->status != FDFS_STORAGE_STATUS_DELETED && \
			 pStorage->status != FDFS_STORAGE_STATUS_IP_CHANGED && \
			 pStorage->status != FDFS_STORAGE_STATUS_NONE))
			{
				sleep(1);
			}

			if (pStorage->status != FDFS_STORAGE_STATUS_ACTIVE)
			{
				close(storage_server.sock);
				storage_reader_destroy(&reader);
				continue;
			}
		}

		if (tcpsetnonblockopt(storage_server.sock) != 0)
		{
			close(storage_server.sock);
			storage_reader_destroy(&reader);
			continue;
		}

		getSockIpaddr(storage_server.sock, \
			local_ip_addr, IP_ADDRESS_SIZE);

		/*
		//printf("file: "__FILE__", line: %d, " \
			"storage_server.ip_addr=%s, " \
			"local_ip_addr: %s\n", \
			__LINE__, storage_server.ip_addr, local_ip_addr);
		*/

		if (strcmp(local_ip_addr, storage_server.ip_addr) == 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"ip_addr %s belong to the local host," \
				" sync thread exit.", \
				__LINE__, storage_server.ip_addr);
			fdfs_quit(&storage_server);
			close(storage_server.sock);
			break;
		}

		if (*g_tracker_client_ip != '\0' && \
			strcmp(local_ip_addr, g_tracker_client_ip) != 0)
		{
			if (storage_report_client_ip(&storage_server) != 0)
			{
				close(storage_server.sock);
				storage_reader_destroy(&reader);
				continue;
			}
		}

		if (pStorage->status == FDFS_STORAGE_STATUS_WAIT_SYNC)
		{
			pStorage->status = FDFS_STORAGE_STATUS_SYNCING;
			storage_report_storage_status(pStorage->ip_addr, \
				pStorage->status);
		}

		if (pStorage->status == FDFS_STORAGE_STATUS_SYNCING)
		{
			if (reader.need_sync_old && reader.sync_old_done)
			{
				pStorage->status = FDFS_STORAGE_STATUS_OFFLINE;
				storage_report_storage_status(  \
					pStorage->ip_addr, \
					pStorage->status);
			}
		}

		if (g_sync_part_time)
		{
			current_time = time(NULL);
			storage_sync_get_start_end_times(current_time, \
				&g_sync_start_time, &g_sync_end_time, \
				&start_time, &end_time);
		}

		sync_result = 0;
		while (g_continue_flag && (!g_sync_part_time || \
			(current_time >= start_time && \
			current_time <= end_time)) && \
			(pStorage->status == FDFS_STORAGE_STATUS_ACTIVE || \
			pStorage->status == FDFS_STORAGE_STATUS_SYNCING))
		{
			if (g_sync_part_time)
			{
				current_time = time(NULL);
			}

			read_result = storage_binlog_read(&reader, \
					&record, &record_len);
			if (read_result == ENOENT)
			{
				if (reader.need_sync_old && \
					!reader.sync_old_done)
				{
				reader.sync_old_done = true;
				if (storage_write_to_mark_file(&reader) != 0)
				{
					logCrit("file: "__FILE__", line: %d, " \
						"storage_write_to_mark_file " \
						"fail, program exit!", \
						__LINE__);
					g_continue_flag = false;
					break;
				}

				if (pStorage->status == \
					FDFS_STORAGE_STATUS_SYNCING)
				{
					pStorage->status = \
						FDFS_STORAGE_STATUS_OFFLINE;
					storage_report_storage_status(  \
						pStorage->ip_addr, \
						pStorage->status);
				}
				}

				usleep(g_sync_wait_usec);
				continue;
			}
			else if (read_result != 0)
			{
				sleep(5);
				continue;
			}

			if ((sync_result=storage_sync_data(&reader, \
				&storage_server, &record)) != 0)
			{
				if (rewind_to_prev_rec_end(&reader) != 0)
				{
					logCrit("file: "__FILE__", line: %d, " \
						"rewind_to_prev_rec_end fail, "\
						"program exit!", __LINE__);
					g_continue_flag = false;
				}

				break;
			}

			reader.binlog_offset += record_len;
			reader.scan_row_count++;
			if (reader.scan_row_count % 2000 == 0)
			{
				if (storage_write_to_mark_file(&reader) != 0)
				{
					logCrit("file: "__FILE__", line: %d, " \
						"storage_write_to_mark_file " \
						"fail, program exit!", \
						__LINE__);
					g_continue_flag = false;
					break;
				}
			}

			if (g_sync_interval > 0)
			{
				usleep(g_sync_interval);
			}
		}

		if (reader.last_write_row_count != reader.scan_row_count)
		{
			if (storage_write_to_mark_file(&reader) != 0)
			{
				logCrit("file: "__FILE__", line: %d, " \
					"storage_write_to_mark_file fail, " \
					"program exit!", __LINE__);
				g_continue_flag = false;
				break;
			}
		}

		close(storage_server.sock);
		storage_server.sock = -1;
		storage_reader_destroy(&reader);

		if (!g_continue_flag)
		{
			break;
		}

		if (!(sync_result == ENOTCONN || sync_result == EIO))
		{
			sleep(1);
		}
	}

	if (storage_server.sock >= 0)
	{
		close(storage_server.sock);
	}
	storage_reader_destroy(&reader);

	if (pStorage->status == FDFS_STORAGE_STATUS_DELETED
	 || pStorage->status == FDFS_STORAGE_STATUS_IP_CHANGED)
	{
		storage_changelog_req();
		sleep(2 * g_heart_beat_interval + 1);
		pStorage->status = FDFS_STORAGE_STATUS_NONE;
	}

	storage_sync_thread_exit(&storage_server);

	return NULL;
}

int storage_sync_thread_start(const FDFSStorageBrief *pStorage)
{
	int result;
	pthread_attr_t pattr;
	pthread_t tid;

	if (pStorage->status == FDFS_STORAGE_STATUS_DELETED || \
	    pStorage->status == FDFS_STORAGE_STATUS_IP_CHANGED || \
	    pStorage->status == FDFS_STORAGE_STATUS_NONE)
	{
		return 0;
	}

	if (is_local_host_ip(pStorage->ip_addr)) //can't self sync to self
	{
		return 0;
	}

	if ((result=init_pthread_attr(&pattr, g_thread_stack_size)) != 0)
	{
		return result;
	}

	/*
	//printf("start storage ip_addr: %s, g_storage_sync_thread_count=%d\n", 
			pStorage->ip_addr, g_storage_sync_thread_count);
	*/

	if ((result=pthread_create(&tid, &pattr, storage_sync_thread_entrance, \
		(void *)pStorage)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"create thread failed, errno: %d, " \
			"error info: %s", \
			__LINE__, result, strerror(result));

		pthread_attr_destroy(&pattr);
		return result;
	}

	if ((result=pthread_mutex_lock(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	g_storage_sync_thread_count++;
	sync_tids = (pthread_t *)realloc(sync_tids, sizeof(pthread_t) * \
					g_storage_sync_thread_count);
	if (sync_tids == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, (int)sizeof(pthread_t) * \
			g_storage_sync_thread_count, \
			errno, strerror(errno));
	}
	else
	{
		sync_tids[g_storage_sync_thread_count - 1] = tid;
	}

	if ((result=pthread_mutex_unlock(&sync_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	pthread_attr_destroy(&pattr);

	return 0;
}

