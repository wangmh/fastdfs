/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_func.c

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
#include "fdfs_define.h"
#include "logger.h"
#include "fdfs_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "ini_file_reader.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "storage_global.h"
#include "storage_func.h"
#include "fdht_func.h"
#include "fdht_client.h"

#define DATA_DIR_INITED_FILENAME	".data_init_flag"
#define STORAGE_STAT_FILENAME		"storage_stat.dat"

#define INIT_ITEM_STORAGE_JOIN_TIME	"storage_join_time"
#define INIT_ITEM_SYNC_OLD_DONE		"sync_old_done"
#define INIT_ITEM_SYNC_SRC_SERVER	"sync_src_server"
#define INIT_ITEM_SYNC_UNTIL_TIMESTAMP	"sync_until_timestamp"

#define STAT_ITEM_TOTAL_UPLOAD		"total_upload_count"
#define STAT_ITEM_SUCCESS_UPLOAD	"success_upload_count"
#define STAT_ITEM_TOTAL_DOWNLOAD	"total_download_count"
#define STAT_ITEM_SUCCESS_DOWNLOAD	"success_download_count"
#define STAT_ITEM_LAST_SOURCE_UPD	"last_source_update"
#define STAT_ITEM_LAST_SYNC_UPD		"last_sync_update"
#define STAT_ITEM_TOTAL_SET_META	"total_set_meta_count"
#define STAT_ITEM_SUCCESS_SET_META	"success_set_meta_count"
#define STAT_ITEM_TOTAL_DELETE		"total_delete_count"
#define STAT_ITEM_SUCCESS_DELETE	"success_delete_count"
#define STAT_ITEM_TOTAL_GET_META	"total_get_meta_count"
#define STAT_ITEM_SUCCESS_GET_META	"success_get_meta_count"
#define STAT_ITEM_TOTAL_CREATE_LINK	"total_create_link_count"
#define STAT_ITEM_SUCCESS_CREATE_LINK	"success_create_link_count"
#define STAT_ITEM_TOTAL_DELETE_LINK	"total_delete_link_count"
#define STAT_ITEM_SUCCESS_DELETE_LINK	"success_delete_link_count"

#define STAT_ITEM_DIST_PATH_INDEX_HIGH	"dist_path_index_high"
#define STAT_ITEM_DIST_PATH_INDEX_LOW	"dist_path_index_low"
#define STAT_ITEM_DIST_WRITE_FILE_COUNT	"dist_write_file_count"

static int storage_stat_fd = -1;

/*
static pthread_mutex_t fsync_thread_mutex;
static pthread_cond_t fsync_thread_cond;
static int fsync_thread_count = 0;
*/

static int storage_open_storage_stat();
static int storage_close_storage_stat();
static int storage_make_data_dirs(const char *pBasePath);
static int storage_check_and_make_data_dirs();

static char *get_storage_stat_filename(const void *pArg, char *full_filename)
{
	static char buff[MAX_PATH_SIZE];

	if (full_filename == NULL)
	{
		full_filename = buff;
	}

	snprintf(full_filename, MAX_PATH_SIZE, \
			"%s/data/%s", g_base_path, STORAGE_STAT_FILENAME);
	return full_filename;
}

int storage_write_to_fd(int fd, get_filename_func filename_func, \
		const void *pArg, const char *buff, const int len)
{
	if (ftruncate(fd, 0) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"truncate file \"%s\" to empty fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (lseek(fd, 0, SEEK_SET) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"rewind file \"%s\" to start fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (write(fd, buff, len) != len)
	{
		logError("file: "__FILE__", line: %d, " \
			"write to file \"%s\" fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (fsync(fd) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"sync file \"%s\" to disk fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	return 0;
}

static int storage_open_storage_stat()
{
	char full_filename[MAX_PATH_SIZE];
	IniItemInfo *items;
	int nItemCount;
	int result;

	get_storage_stat_filename(NULL, full_filename);
	if (fileExists(full_filename))
	{
		if ((result=iniLoadItems(full_filename, &items, &nItemCount)) \
			 != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"load from stat file \"%s\" fail, " \
				"error code: %d", \
				__LINE__, full_filename, result);
			return result;
		}

		if (nItemCount < 12)
		{
			iniFreeItems(items);
			logError("file: "__FILE__", line: %d, " \
				"in stat file \"%s\", item count: %d < 12", \
				__LINE__, full_filename, nItemCount);
			return ENOENT;
		}

		g_storage_stat.total_upload_count = iniGetInt64Value( \
				STAT_ITEM_TOTAL_UPLOAD, \
				items, nItemCount, 0);
		g_storage_stat.success_upload_count = iniGetInt64Value( \
				STAT_ITEM_SUCCESS_UPLOAD, \
				items, nItemCount, 0);
		g_storage_stat.total_download_count = iniGetInt64Value( \
				STAT_ITEM_TOTAL_DOWNLOAD, \
				items, nItemCount, 0);
		g_storage_stat.success_download_count = iniGetInt64Value( \
				STAT_ITEM_SUCCESS_DOWNLOAD, \
				items, nItemCount, 0);
		g_storage_stat.last_source_update = iniGetIntValue( \
				STAT_ITEM_LAST_SOURCE_UPD, \
				items, nItemCount, 0);
		g_storage_stat.last_sync_update = iniGetIntValue( \
				STAT_ITEM_LAST_SYNC_UPD, \
				items, nItemCount, 0);
		g_storage_stat.total_set_meta_count = iniGetInt64Value( \
				STAT_ITEM_TOTAL_SET_META, \
				items, nItemCount, 0);
		g_storage_stat.success_set_meta_count = iniGetInt64Value( \
				STAT_ITEM_SUCCESS_SET_META, \
				items, nItemCount, 0);
		g_storage_stat.total_delete_count = iniGetInt64Value( \
				STAT_ITEM_TOTAL_DELETE, \
				items, nItemCount, 0);
		g_storage_stat.success_delete_count = iniGetInt64Value( \
				STAT_ITEM_SUCCESS_DELETE, \
				items, nItemCount, 0);
		g_storage_stat.total_get_meta_count = iniGetInt64Value( \
				STAT_ITEM_TOTAL_GET_META, \
				items, nItemCount, 0);
		g_storage_stat.success_get_meta_count = iniGetInt64Value( \
				STAT_ITEM_SUCCESS_GET_META, \
				items, nItemCount, 0);
		g_storage_stat.total_create_link_count = iniGetInt64Value( \
				STAT_ITEM_TOTAL_CREATE_LINK, \
				items, nItemCount, 0);
		g_storage_stat.success_create_link_count = iniGetInt64Value( \
				STAT_ITEM_SUCCESS_CREATE_LINK, \
				items, nItemCount, 0);
		g_storage_stat.total_delete_link_count = iniGetInt64Value( \
				STAT_ITEM_TOTAL_DELETE_LINK, \
				items, nItemCount, 0);
		g_storage_stat.success_delete_link_count = iniGetInt64Value( \
				STAT_ITEM_SUCCESS_DELETE_LINK, \
				items, nItemCount, 0);

		g_dist_path_index_high = iniGetIntValue( \
				STAT_ITEM_DIST_PATH_INDEX_HIGH, \
				items, nItemCount, 0);
		g_dist_path_index_low = iniGetIntValue( \
				STAT_ITEM_DIST_PATH_INDEX_LOW, \
				items, nItemCount, 0);
		g_dist_write_file_count = iniGetIntValue( \
				STAT_ITEM_DIST_WRITE_FILE_COUNT, \
				items, nItemCount, 0);

		iniFreeItems(items);
	}
	else
	{
		memset(&g_storage_stat, 0, sizeof(g_storage_stat));
	}

	storage_stat_fd = open(full_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (storage_stat_fd < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open stat file \"%s\" fail, " \
			"error no: %d, error info: %s", \
			__LINE__, full_filename, errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	return storage_write_to_stat_file();
}

static int storage_close_storage_stat()
{
	int result;

	result = 0;
	if (storage_stat_fd >= 0)
	{
		result = storage_write_to_stat_file();
		if (close(storage_stat_fd) != 0)
		{
			result += errno != 0 ? errno : ENOENT;
		}
		storage_stat_fd = -1;
	}

	return result;
}

int storage_write_to_stat_file()
{
	char buff[1024];
	int len;

	len = sprintf(buff, 
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s=%d\n"  \
		"%s=%d\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s=%d\n"  \
		"%s=%d\n"  \
		"%s=%d\n", \
		STAT_ITEM_TOTAL_UPLOAD, g_storage_stat.total_upload_count, \
		STAT_ITEM_SUCCESS_UPLOAD, g_storage_stat.success_upload_count, \
		STAT_ITEM_TOTAL_DOWNLOAD, g_storage_stat.total_download_count, \
		STAT_ITEM_SUCCESS_DOWNLOAD, \
		g_storage_stat.success_download_count, \
		STAT_ITEM_LAST_SOURCE_UPD, \
		(int)g_storage_stat.last_source_update, \
		STAT_ITEM_LAST_SYNC_UPD, (int)g_storage_stat.last_sync_update,\
		STAT_ITEM_TOTAL_SET_META, g_storage_stat.total_set_meta_count, \
		STAT_ITEM_SUCCESS_SET_META, \
		g_storage_stat.success_set_meta_count, \
		STAT_ITEM_TOTAL_DELETE, g_storage_stat.total_delete_count, \
		STAT_ITEM_SUCCESS_DELETE, g_storage_stat.success_delete_count, \
		STAT_ITEM_TOTAL_GET_META, g_storage_stat.total_get_meta_count, \
		STAT_ITEM_SUCCESS_GET_META, \
		g_storage_stat.success_get_meta_count,  \
		STAT_ITEM_TOTAL_CREATE_LINK, \
		g_storage_stat.total_create_link_count,  \
		STAT_ITEM_SUCCESS_CREATE_LINK, \
		g_storage_stat.success_create_link_count,  \
		STAT_ITEM_TOTAL_DELETE_LINK, \
		g_storage_stat.total_delete_link_count,  \
		STAT_ITEM_SUCCESS_DELETE_LINK, \
		g_storage_stat.success_delete_link_count,  \
		STAT_ITEM_DIST_PATH_INDEX_HIGH, g_dist_path_index_high, \
		STAT_ITEM_DIST_PATH_INDEX_LOW, g_dist_path_index_low, \
		STAT_ITEM_DIST_WRITE_FILE_COUNT, g_dist_write_file_count
	    );

	return storage_write_to_fd(storage_stat_fd, \
			get_storage_stat_filename, NULL, buff, len);
}

int storage_write_to_sync_ini_file()
{
	char full_filename[MAX_PATH_SIZE];
	char buff[256];
	int fd;
	int len;

	snprintf(full_filename, sizeof(full_filename), \
		"%s/data/%s", g_base_path, DATA_DIR_INITED_FILENAME);
	if ((fd=open(full_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open file \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, full_filename, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	len = sprintf(buff, "%s=%d\n" \
		"%s=%d\n"  \
		"%s=%s\n"  \
		"%s=%d\n", \
		INIT_ITEM_STORAGE_JOIN_TIME, g_storage_join_time, \
		INIT_ITEM_SYNC_OLD_DONE, g_sync_old_done, \
		INIT_ITEM_SYNC_SRC_SERVER, g_sync_src_ip_addr, \
		INIT_ITEM_SYNC_UNTIL_TIMESTAMP, g_sync_until_timestamp \
	    );
	if (write(fd, buff, len) != len)
	{
		logError("file: "__FILE__", line: %d, " \
			"write to file \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, full_filename, \
			errno, strerror(errno));
		close(fd);
		return errno != 0 ? errno : EIO;
	}

	close(fd);
	return 0;
}

static int storage_check_and_make_data_dirs()
{
	int result;
	int i;
	char data_path[MAX_PATH_SIZE];
	char full_filename[MAX_PATH_SIZE];

	snprintf(data_path, sizeof(data_path), "%s/data", \
			g_base_path);
	snprintf(full_filename, sizeof(full_filename), "%s/%s", \
			data_path, DATA_DIR_INITED_FILENAME);
	if (fileExists(full_filename))
	{
		IniItemInfo *items;
		int nItemCount;
		char *pValue;

		if ((result=iniLoadItems(full_filename, \
				&items, &nItemCount)) \
			 != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"load from file \"%s/%s\" fail, " \
				"error code: %d", \
				__LINE__, data_path, \
				full_filename, result);
			return result;
		}
		
		pValue = iniGetStrValue(INIT_ITEM_STORAGE_JOIN_TIME, \
				items, nItemCount);
		if (pValue == NULL)
		{
			iniFreeItems(items);
			logError("file: "__FILE__", line: %d, " \
				"in file \"%s/%s\", item \"%s\" not exists", \
				__LINE__, data_path, full_filename, \
				INIT_ITEM_STORAGE_JOIN_TIME);
			return ENOENT;
		}
		g_storage_join_time = atoi(pValue);

		pValue = iniGetStrValue(INIT_ITEM_SYNC_OLD_DONE, \
				items, nItemCount);
		if (pValue == NULL)
		{
			iniFreeItems(items);
			logError("file: "__FILE__", line: %d, " \
				"in file \"%s/%s\", item \"%s\" not exists", \
				__LINE__, data_path, full_filename, \
				INIT_ITEM_SYNC_OLD_DONE);
			return ENOENT;
		}
		g_sync_old_done = atoi(pValue);

		pValue = iniGetStrValue(INIT_ITEM_SYNC_SRC_SERVER, \
				items, nItemCount);
		if (pValue == NULL)
		{
			iniFreeItems(items);
			logError("file: "__FILE__", line: %d, " \
				"in file \"%s/%s\", item \"%s\" not exists", \
				__LINE__, data_path, full_filename, \
				INIT_ITEM_SYNC_SRC_SERVER);
			return ENOENT;
		}
		snprintf(g_sync_src_ip_addr, sizeof(g_sync_src_ip_addr), \
				"%s", pValue);

		g_sync_until_timestamp = iniGetIntValue( \
				INIT_ITEM_SYNC_UNTIL_TIMESTAMP, \
				items, nItemCount, 0);
		iniFreeItems(items);

		//printf("g_sync_old_done = %d\n", g_sync_old_done);
		//printf("g_sync_src_ip_addr = %s\n", g_sync_src_ip_addr);
		//printf("g_sync_until_timestamp = %d\n", g_sync_until_timestamp);
	}
	else
	{
		if (!fileExists(data_path))
		{
			if (mkdir(data_path, 0755) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"mkdir \"%s\" fail, " \
					"errno: %d, error info: %s", \
					__LINE__, data_path, \
					errno, strerror(errno));
				return errno != 0 ? errno : EPERM;
			}
		}

		g_storage_join_time = time(NULL);
		if ((result=storage_write_to_sync_ini_file()) != 0)
		{
			return result;
		}
	}

	for (i=0; i<g_path_count; i++)
	{
		if ((result=storage_make_data_dirs(g_store_paths[i])) != 0)
		{
			return result;
		}
	}

	return 0;
}

static int storage_make_data_dirs(const char *pBasePath)
{
	char data_path[MAX_PATH_SIZE];
	char dir_name[9];
	char sub_name[9];
	char min_sub_path[16];
	char max_sub_path[16];
	int i, k;

	snprintf(data_path, sizeof(data_path), "%s/data", pBasePath);
	if (!fileExists(data_path))
	{
		if (mkdir(data_path, 0755) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"mkdir \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, data_path, errno, strerror(errno));
			return errno != 0 ? errno : EPERM;
		}
	}

	if (chdir(data_path) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"chdir \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, data_path, errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	sprintf(min_sub_path, STORAGE_DATA_DIR_FORMAT"/"STORAGE_DATA_DIR_FORMAT,
			0, 0);
	sprintf(max_sub_path, STORAGE_DATA_DIR_FORMAT"/"STORAGE_DATA_DIR_FORMAT,
			g_subdir_count_per_path-1, g_subdir_count_per_path-1);
	if (fileExists(min_sub_path) && fileExists(max_sub_path))
	{
		return 0;
	}

	fprintf(stderr, "data path: %s, mkdir sub dir...\n", data_path);
	for (i=0; i<g_subdir_count_per_path; i++)
	{
		sprintf(dir_name, STORAGE_DATA_DIR_FORMAT, i);

		fprintf(stderr, "mkdir data path: %s ...\n", dir_name);
		if (mkdir(dir_name, 0755) != 0)
		{
			if (!(errno == EEXIST && isDir(dir_name)))
			{
				logError("file: "__FILE__", line: %d, " \
					"mkdir \"%s/%s\" fail, " \
					"errno: %d, error info: %s", \
					__LINE__, data_path, dir_name, \
					errno, strerror(errno));
				return errno != 0 ? errno : ENOENT;
			}
		}

		if (chdir(dir_name) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"chdir \"%s/%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, data_path, dir_name, \
				errno, strerror(errno));
			return errno != 0 ? errno : ENOENT;
		}

		for (k=0; k<g_subdir_count_per_path; k++)
		{
			sprintf(sub_name, STORAGE_DATA_DIR_FORMAT, k);
			if (mkdir(sub_name, 0755) != 0)
			{
				if (!(errno == EEXIST && isDir(sub_name)))
				{
					logError("file: "__FILE__", line: %d," \
						" mkdir \"%s/%s/%s\" fail, " \
						"errno: %d, error info: %s", \
						__LINE__, data_path, \
						dir_name, sub_name, \
						errno, strerror(errno));
					return errno != 0 ? errno : ENOENT;
				}
			}
		}

		if (chdir("..") != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"chdir \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, data_path, \
				errno, strerror(errno));
			return errno != 0 ? errno : ENOENT;
		}
	}

	fprintf(stderr, "data path: %s, mkdir sub dir done.\n", data_path);

	return 0;
}

static int storage_cmp_by_ip_and_port(const void *p1, const void *p2)
{
	int res;

	res = strcmp(((TrackerServerInfo *)p1)->ip_addr, \
			((TrackerServerInfo *)p2)->ip_addr);
	if (res != 0)
	{
		return res;
	}

	return ((TrackerServerInfo *)p1)->port - \
			((TrackerServerInfo *)p2)->port;
}

static void insert_into_sorted_servers(TrackerServerInfo *pInsertedServer)
{
	TrackerServerInfo *pDestServer;
	for (pDestServer=g_tracker_servers+g_tracker_server_count; \
		pDestServer>g_tracker_servers; pDestServer--)
	{
		if (storage_cmp_by_ip_and_port(pInsertedServer, \
			pDestServer-1) > 0)
		{
			memcpy(pDestServer, pInsertedServer, \
				sizeof(TrackerServerInfo));
			return;
		}

		memcpy(pDestServer, pDestServer-1, sizeof(TrackerServerInfo));
	}

	memcpy(pDestServer, pInsertedServer, sizeof(TrackerServerInfo));
}

static int copy_tracker_servers(const char *filename, char **ppTrackerServers)
{
	char **ppSrc;
	char **ppEnd;
	TrackerServerInfo destServer;
	char *pSeperator;
	char szHost[128];
	int nHostLen;

	memset(&destServer, 0, sizeof(TrackerServerInfo));
	ppEnd = ppTrackerServers + g_tracker_server_count;

	g_tracker_server_count = 0;
	for (ppSrc=ppTrackerServers; ppSrc<ppEnd; ppSrc++)
	{
		if ((pSeperator=strchr(*ppSrc, ':')) == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\", " \
				"tracker_server \"%s\" is invalid, " \
				"correct format is host:port", \
				__LINE__, filename, *ppSrc);
			return EINVAL;
		}

		nHostLen = pSeperator - (*ppSrc);
		if (nHostLen >= sizeof(szHost))
		{
			nHostLen = sizeof(szHost) - 1;
		}
		memcpy(szHost, *ppSrc, nHostLen);
		szHost[nHostLen] = '\0';

		if (getIpaddrByName(szHost, destServer.ip_addr, \
			sizeof(destServer.ip_addr)) == INADDR_NONE)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\", " \
				"host \"%s\" is invalid", \
				__LINE__, filename, szHost);
			return EINVAL;
		}
		if (strcmp(destServer.ip_addr, "127.0.0.1") == 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\", " \
				"host \"%s\" is invalid, " \
				"ip addr can't be 127.0.0.1", \
				__LINE__, filename, szHost);
			return EINVAL;
		}

		destServer.port = atoi(pSeperator+1);
		if (destServer.port <= 0)
		{
			destServer.port = FDFS_TRACKER_SERVER_DEF_PORT;
		}

		if (bsearch(&destServer, g_tracker_servers, \
			g_tracker_server_count, \
			sizeof(TrackerServerInfo), \
			storage_cmp_by_ip_and_port) == NULL)
		{
			insert_into_sorted_servers(&destServer);
			g_tracker_server_count++;
		}
	}

	/*
	{
	TrackerServerInfo *pServer;
	for (pServer=g_tracker_servers; pServer<g_tracker_servers+ \
		g_tracker_server_count;	pServer++)
	{
		//printf("server=%s:%d\n", \
			pServer->ip_addr, pServer->port);
	}
	}
	*/

	return 0;
}

/*
static int init_fsync_pthread_cond()
{
	int result;
	pthread_condattr_t thread_condattr;
	if ((result=pthread_condattr_init(&thread_condattr)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_condattr_init failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	if ((result=pthread_cond_init(&fsync_thread_cond, &thread_condattr)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_cond_init failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	pthread_condattr_destroy(&thread_condattr);
	return 0;
}
*/

int storage_load_paths(IniItemInfo *items, const int nItemCount)
{
	char item_name[64];
	char *pPath;
	int i;

	pPath = iniGetStrValue("base_path", items, nItemCount);
	if (pPath == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"conf file must have item \"base_path\"!", __LINE__);
		return ENOENT;
	}

	snprintf(g_base_path, sizeof(g_base_path), "%s", pPath);
	chopPath(g_base_path);
	if (!fileExists(g_base_path))
	{
		logError("file: "__FILE__", line: %d, " \
			"\"%s\" can't be accessed, error info: %s", \
			__LINE__, strerror(errno), g_base_path);
		return errno != 0 ? errno : ENOENT;
	}
	if (!isDir(g_base_path))
	{
		logError("file: "__FILE__", line: %d, " \
			"\"%s\" is not a directory!", \
			__LINE__, g_base_path);
		return ENOTDIR;
	}

	g_path_count = iniGetIntValue("store_path_count", \
			items, nItemCount, 1);
	if (g_path_count <= 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"store_path_count: %d is invalid!", \
			__LINE__, g_path_count);
		return EINVAL;
	}

	g_store_paths = (char **)malloc(sizeof(char *) *g_path_count);
	if (g_store_paths == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s", \
			__LINE__, sizeof(char *) *g_path_count, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	memset(g_store_paths, 0, sizeof(char *) *g_path_count);

	pPath = iniGetStrValue("store_path0", items, nItemCount);
	if (pPath == NULL)
	{
		pPath = g_base_path;
	}
	g_store_paths[0] = strdup(pPath);
	if (g_store_paths[0] == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s", \
			__LINE__, strlen(pPath), errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	for (i=1; i<g_path_count; i++)
	{
		sprintf(item_name, "store_path%d", i);
		pPath = iniGetStrValue(item_name, items, nItemCount);
		if (pPath == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file must have item \"%s\"!", \
				__LINE__, item_name);
			return ENOENT;
		}

		chopPath(pPath);
		if (!fileExists(pPath))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" can't be accessed, error info: %s", \
				__LINE__, strerror(errno), pPath);
			return errno != 0 ? errno : ENOENT;
		}
		if (!isDir(pPath))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" is not a directory!", \
				__LINE__, pPath);
			return ENOTDIR;
		}

		g_store_paths[i] = strdup(pPath);
		if (g_store_paths[i] == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", __LINE__, \
				strlen(pPath), errno, strerror(errno));
			return errno != 0 ? errno : ENOMEM;
		}
	}

	return 0;
}

int storage_func_init(const char *filename, \
		char *bind_addr, const int addr_size)
{
	char *pBindAddr;
	char *pGroupName;
	char *pRunByGroup;
	char *pRunByUser;
	char *pFsyncAfterWrittenBytes;
	char *ppTrackerServers[FDFS_MAX_TRACKERS];
	IniItemInfo *items;
	int nItemCount;
	int result;
	int64_t fsync_after_written_bytes;

	/*
	while (nThreadCount > 0)
	{
		sleep(1);
	}
	*/

	if ((result=iniLoadItems(filename, &items, &nItemCount)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"load conf file \"%s\" fail, ret code: %d", \
			__LINE__, filename, result);
		return result;
	}

	while (1)
	{
		if (iniGetBoolValue("disabled", items, nItemCount, false))
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\" disabled=true, exit", \
				__LINE__, filename);
			result = ECANCELED;
			break;
		}

		g_subdir_count_per_path=iniGetIntValue("subdir_count_per_path",
			 items, nItemCount, DEFAULT_DATA_DIR_COUNT_PER_PATH);
		if (g_subdir_count_per_path <= 0 || \
		    g_subdir_count_per_path > 256)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\", invalid subdir_count: %d", \
				__LINE__, filename, g_subdir_count_per_path);
			result = EINVAL;
			break;
		}

		if ((result=storage_load_paths(items, nItemCount)) != 0)
		{
			break;
		}

		load_log_level(items, nItemCount);
		if ((result=log_init(STORAGE_ERROR_LOG_FILENAME)) != 0)
		{
			break;
		}

		g_network_timeout = iniGetIntValue("network_timeout", \
				items, nItemCount, DEFAULT_NETWORK_TIMEOUT);
		if (g_network_timeout <= 0)
		{
			g_network_timeout = DEFAULT_NETWORK_TIMEOUT;
		}

		g_server_port = iniGetIntValue("port", items, nItemCount, \
					FDFS_STORAGE_SERVER_DEF_PORT);
		if (g_server_port <= 0)
		{
			g_server_port = FDFS_STORAGE_SERVER_DEF_PORT;
		}

		g_heart_beat_interval = iniGetIntValue("heart_beat_interval", \
			items, nItemCount, STORAGE_BEAT_DEF_INTERVAL);
		if (g_heart_beat_interval <= 0)
		{
			g_heart_beat_interval = STORAGE_BEAT_DEF_INTERVAL;
		}

		g_stat_report_interval = iniGetIntValue("stat_report_interval",\
			 items, nItemCount, STORAGE_REPORT_DEF_INTERVAL);
		if (g_stat_report_interval <= 0)
		{
			g_stat_report_interval = STORAGE_REPORT_DEF_INTERVAL;
		}

		pBindAddr = iniGetStrValue("bind_addr", items, nItemCount);
		if (pBindAddr == NULL)
		{
			bind_addr[0] = '\0';
		}
		else
		{
			snprintf(bind_addr, addr_size, "%s", pBindAddr);
		}

		pGroupName = iniGetStrValue("group_name", items, nItemCount);
		if (pGroupName == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\" must have item " \
				"\"group_name\"!", \
				__LINE__, filename);
			result = ENOENT;
			break;
		}
		if (pGroupName[0] == '\0')
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\", " \
				"group_name is empty!", \
				__LINE__, filename);
			result = EINVAL;
			break;
		}

		snprintf(g_group_name, sizeof(g_group_name), "%s", pGroupName);
		if ((result=fdfs_validate_group_name(g_group_name)) != 0) \
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\", " \
				"the group name \"%s\" is invalid!", \
				__LINE__, filename, g_group_name);
			result = EINVAL;
			break;
		}

		if ((g_tracker_server_count=iniGetValues("tracker_server", \
			items, nItemCount, ppTrackerServers, \
			FDFS_MAX_TRACKERS)) <= 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\", " \
				"get item \"tracker_server\" fail", \
				__LINE__, filename);
			result = ENOENT;
			break;
		}

		g_tracker_servers = (TrackerServerInfo *)malloc( \
			sizeof(TrackerServerInfo) * g_tracker_server_count);
		if (g_tracker_servers == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail", __LINE__, \
				sizeof(TrackerServerInfo)*g_tracker_server_count);

			result = errno != 0 ? errno : ENOMEM;
			break;
		}

		memset(g_tracker_servers, 0, \
			sizeof(TrackerServerInfo) * g_tracker_server_count); 
		if ((result=copy_tracker_servers(filename, \
				ppTrackerServers)) != 0)
		{
			free(g_tracker_servers);
			g_tracker_servers = NULL;
			break;
		}

		g_sync_wait_usec = iniGetIntValue("sync_wait_msec",\
			 items, nItemCount, STORAGE_DEF_SYNC_WAIT_MSEC);
		if (g_sync_wait_usec <= 0)
		{
			g_sync_wait_usec = STORAGE_DEF_SYNC_WAIT_MSEC;
		}
		g_sync_wait_usec *= 1000;

		g_sync_interval = iniGetIntValue("sync_interval",\
			 items, nItemCount, 0);
		if (g_sync_interval < 0)
		{
			g_sync_interval = 0;
		}
		g_sync_interval *= 1000;

		if ((result=get_time_item_from_conf(items, nItemCount, \
			"sync_start_time", &g_sync_start_time, 0, 0)) != 0)
		{
			break;
		}
		if ((result=get_time_item_from_conf(items, nItemCount, \
			"sync_end_time", &g_sync_end_time, 23, 59)) != 0)
		{
			break;
		}

		g_sync_part_time = !((g_sync_start_time.hour == 0 && \
				g_sync_start_time.minute == 0) && \
				(g_sync_end_time.hour == 23 && \
				g_sync_end_time.minute == 59));

		g_max_connections = iniGetIntValue("max_connections", \
				items, nItemCount, DEFAULT_MAX_CONNECTONS);
		if (g_max_connections <= 0)
		{
			g_max_connections = DEFAULT_MAX_CONNECTONS;
		}
		if ((result=set_rlimit(RLIMIT_NOFILE, g_max_connections)) != 0)
		{
			break;
		}
	
		pRunByGroup = iniGetStrValue("run_by_group", \
						items, nItemCount);
		pRunByUser = iniGetStrValue("run_by_user", \
						items, nItemCount);
		if ((result=set_run_by(pRunByGroup, pRunByUser)) != 0)
		{
			return result;
		}

		if ((result=load_allow_hosts(items, nItemCount, \
                	 &g_allow_ip_addrs, &g_allow_ip_count)) != 0)
		{
			return result;
		}

		g_file_distribute_path_mode = iniGetIntValue( \
			"file_distribute_path_mode", items, nItemCount, \
			FDFS_FILE_DIST_PATH_ROUND_ROBIN);
		g_file_distribute_rotate_count = iniGetIntValue( \
			"file_distribute_rotate_count", items, nItemCount, \
			FDFS_FILE_DIST_DEFAULT_ROTATE_COUNT);
		if (g_file_distribute_rotate_count <= 0)
		{
			g_file_distribute_rotate_count = \
				FDFS_FILE_DIST_DEFAULT_ROTATE_COUNT;
		}

		pFsyncAfterWrittenBytes = iniGetStrValue( \
			"fsync_after_written_bytes", items, nItemCount);
		if (pFsyncAfterWrittenBytes == NULL)
		{
			fsync_after_written_bytes = 0;
		}
		else if ((result=parse_bytes(pFsyncAfterWrittenBytes, 1, \
				&fsync_after_written_bytes)) != 0)
		{
			return result;
		}
		g_fsync_after_written_bytes = fsync_after_written_bytes;

		g_sync_log_buff_interval = iniGetIntValue( \
				"sync_log_buff_interval", items, nItemCount, \
				SYNC_LOG_BUFF_DEF_INTERVAL);
		if (g_sync_log_buff_interval <= 0)
		{
			g_sync_log_buff_interval = SYNC_LOG_BUFF_DEF_INTERVAL;
		}

		g_check_file_duplicate = iniGetBoolValue("check_file_duplicate",
					items, nItemCount, false);
		if (g_check_file_duplicate)
		{
			char *pKeyNamespace;
			pKeyNamespace = iniGetStrValue( \
				"key_namespace", items, nItemCount);
			if (pKeyNamespace == NULL || *pKeyNamespace == '\0')
			{
				logError("file: "__FILE__", line: %d, " \
					"item \"key_namespace\" does not " \
					"exist or is empty", __LINE__);
				result = EINVAL;
				break;
			}

			g_namespace_len = strlen(pKeyNamespace);
			if (g_namespace_len >= sizeof(g_key_namespace))
			{
				g_namespace_len = sizeof(g_key_namespace) - 1;
			}
			memcpy(g_key_namespace, pKeyNamespace, g_namespace_len);
			*(g_key_namespace + g_namespace_len) = '\0';

			if ((result=fdht_load_groups(items, nItemCount, \
					&g_group_array)) != 0)
			{
				break;
			}
		}
 
		logInfo("FastDFS v%d.%d, base_path=%s, store_path_count=%d, " \
			"subdir_count_per_path=%d, group_name=%s, " \
			"network_timeout=%ds, "\
			"port=%d, bind_addr=%s, " \
			"max_connections=%d, "    \
			"heart_beat_interval=%ds, " \
			"stat_report_interval=%ds, tracker_server_count=%d, " \
			"sync_wait_msec=%dms, sync_interval=%dms, " \
			"sync_start_time=%02d:%02d, sync_end_time: %02d:%02d, "\
			"allow_ip_count=%d, " \
			"file_distribute_path_mode=%d, " \
			"file_distribute_rotate_count=%d, " \
			"fsync_after_written_bytes=%d, " \
			"sync_log_buff_interval=%ds, " \
			"check_file_duplicate=%d, FDHT group count=%d, " \
			"key_namespace=%s", \
			g_version.major, g_version.minor, \
			g_base_path, g_path_count, g_subdir_count_per_path, \
			g_group_name, g_network_timeout, \
			g_server_port, bind_addr, g_max_connections, \
			g_heart_beat_interval, g_stat_report_interval, \
			g_tracker_server_count, g_sync_wait_usec / 1000, \
			g_sync_interval / 1000, \
			g_sync_start_time.hour, g_sync_start_time.minute, \
			g_sync_end_time.hour, g_sync_end_time.minute, \
			g_allow_ip_count, g_file_distribute_path_mode, \
			g_file_distribute_rotate_count, \
			g_fsync_after_written_bytes, g_sync_log_buff_interval, \
			g_check_file_duplicate, g_group_array.count, g_key_namespace);

		break;
	}

	/*
	if ((result=init_pthread_lock(&fsync_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, program exit!", __LINE__);
		return result;
	}

	if ((result=init_fsync_pthread_cond()) != 0)
	{
		return result;
	}
	*/

	iniFreeItems(items);

	if (result == 0)
	{
		if ((result=storage_check_and_make_data_dirs()) != 0)
		{
			logCrit("file: "__FILE__", line: %d, " \
				"storage_check_and_make_data_dirs fail, " \
				"program exit!", __LINE__);
			return result;
		}

		return storage_open_storage_stat();
	}
	else
	{
		return result;
	}
}

int storage_func_destroy()
{
	int i;

	if (g_store_paths != NULL)
	{
		for (i=0; i<g_path_count; i++)
		{
			if (g_store_paths[i] != NULL)
			{
				free(g_store_paths[i]);
				g_store_paths[i] = NULL;
			}
		}

		g_store_paths = NULL;
	}

	if (g_tracker_servers != NULL)
	{
		free(g_tracker_servers);
		g_tracker_servers = NULL;
	}

	return storage_close_storage_stat();
}

#define SPLIT_FILENAME_BODY(logic_filename, \
		filename_len, true_filename, store_path_index) \
	char buff[3]; \
	char *pEnd; \
 \
	while (1) \
	{ \
	if (*filename_len <= FDFS_FILE_PATH_LEN) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"filename_len: %d is invalid, <= %d", \
			__LINE__, *filename_len, FDFS_FILE_PATH_LEN); \
		return EINVAL; \
	} \
 \
	if (*logic_filename != STORAGE_STORE_PATH_PREFIX_CHAR) \
	{ /*version < V1.12 */ \
		store_path_index = 0; \
		memcpy(true_filename, logic_filename, (*filename_len)+1); \
		break; \
	} \
 \
	if (*(logic_filename + 3) != '/') \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"filename: %s is invalid", \
			__LINE__, logic_filename); \
		return EINVAL; \
	} \
 \
	*buff = *(logic_filename+1); \
	*(buff+1) = *(logic_filename+2); \
	*(buff+2) = '\0'; \
 \
	pEnd = NULL; \
	store_path_index = strtol(buff, &pEnd, 16); \
	if (pEnd != NULL && *pEnd != '\0') \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"filename: %s is invalid", \
			__LINE__, logic_filename); \
		return EINVAL; \
	} \
 \
	if (store_path_index < 0 || store_path_index >= g_path_count) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"filename: %s is invalid, " \
			"invalid store path index: %d", \
			__LINE__, logic_filename, store_path_index); \
		return EINVAL; \
	} \
 \
	*filename_len -= 4; \
	memcpy(true_filename, logic_filename + 4, (*filename_len) + 1); \
 \
 	break; \
	}


int storage_split_filename(const char *logic_filename, \
		int *filename_len, char *true_filename, char **ppStorePath)
{
	int store_path_index;

	SPLIT_FILENAME_BODY(logic_filename, \
		filename_len, true_filename, store_path_index)

	*ppStorePath = g_store_paths[store_path_index];

	return 0;
}

int storage_split_filename_ex(const char *logic_filename, \
		int *filename_len, char *true_filename, int *store_path_index)
{
	SPLIT_FILENAME_BODY(logic_filename, \
		filename_len, true_filename, *store_path_index)

	return 0;
}

/*
int write_serialized(int fd, const char *buff, size_t count, const bool bSync)
{
	int result;
	int fsync_ret;

	if ((result=pthread_mutex_lock(&fsync_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	while (fsync_thread_count >= g_max_write_thread_count)
	{
		if ((result=pthread_cond_wait(&fsync_thread_cond, \
				&fsync_thread_mutex)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"pthread_cond_wait failed, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			return result;
		}
	}

	fsync_thread_count++;

	if ((result=pthread_mutex_unlock(&fsync_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	if (write(fd, buff, count) == count)
	{
		if (bSync && fsync(fd) != 0)
		{
			fsync_ret = errno != 0 ? errno : EIO;
			logError("file: "__FILE__", line: %d, " \
				"call fsync fail, " \
				"errno: %d, error info: %s", \
				__LINE__, fsync_ret, strerror(fsync_ret));
		}
		else
		{
			fsync_ret = 0;
		}
	}
	else
	{
		fsync_ret = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"call write fail, " \
			"errno: %d, error info: %s", \
			__LINE__, fsync_ret, strerror(fsync_ret));
	}

	if ((result=pthread_mutex_lock(&fsync_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	fsync_thread_count--;

	if ((result=pthread_mutex_unlock(&fsync_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	if ((result=pthread_cond_signal(&fsync_thread_cond)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_cond_signal failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return fsync_ret;
}

int fsync_serialized(int fd)
{
	int result;
	int fsync_ret;

	if ((result=pthread_mutex_lock(&fsync_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	while (fsync_thread_count >= g_max_write_thread_count)
	{
		if ((result=pthread_cond_wait(&fsync_thread_cond, \
				&fsync_thread_mutex)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"pthread_cond_wait failed, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			return result;
		}
	}

	fsync_thread_count++;

	if ((result=pthread_mutex_unlock(&fsync_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	if (fsync(fd) == 0)
	{
		fsync_ret = 0;
	}
	else
	{
		fsync_ret = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"call fsync fail, " \
			"errno: %d, error info: %s", \
			__LINE__, fsync_ret, strerror(fsync_ret));
	}

	if ((result=pthread_mutex_lock(&fsync_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	fsync_thread_count--;

	if ((result=pthread_mutex_unlock(&fsync_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	if ((result=pthread_cond_signal(&fsync_thread_cond)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_cond_signal failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return fsync_ret;
}

int recv_file_serialized(int sock, const char *filename, \
		const int64_t file_bytes)
{
	int fd;
	char buff[FDFS_WRITE_BUFF_SIZE];
	int64_t remain_bytes;
	int recv_bytes;
	int result;

	fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0)
	{
		return errno != 0 ? errno : EACCES;
	}

	remain_bytes = file_bytes;
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

		if ((result=tcprecvdata(sock, buff, recv_bytes, \
				g_network_timeout)) != 0)
		{
			close(fd);
			unlink(filename);
			return result;
		}

		if (recv_bytes == remain_bytes)  //last buff
		{
			if (write_serialized(fd, buff, recv_bytes, true) != 0)
			{
				result = errno != 0 ? errno : EIO;
				close(fd);
				unlink(filename);
				return result;
			}
		}
		else
		{
			if (write_serialized(fd, buff, recv_bytes, false) != 0)
			{
				result = errno != 0 ? errno : EIO;
				close(fd);
				unlink(filename);
				return result;
			}

			if ((result=fsync_serialized(fd)) != 0)
			{
				close(fd);
				unlink(filename);
				return result;
			}
		}

		remain_bytes -= recv_bytes;
	}

	close(fd);
	return 0;
}
*/
