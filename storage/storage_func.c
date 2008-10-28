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

#define DATA_DIR_INITED_FILENAME	".data_init_flag"
#define STORAGE_STAT_FILENAME		"storage_stat.dat"
#define DATA_DIR_COUNT_PER_PATH		256

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

static int storage_stat_fd = -1;

static char *get_storage_stat_filename(const void *pArg, char *full_filename)
{
	static char buff[MAX_PATH_SIZE];

	if (full_filename == NULL)
	{
		full_filename = buff;
	}

	snprintf(full_filename, MAX_PATH_SIZE, \
			"%s/data/%s", g_base_path, \
			STORAGE_STAT_FILENAME);
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

int storage_open_storage_stat()
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

int storage_close_storage_stat()
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
	char buff[512];
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
		"%s="INT64_PRINTF_FORMAT"\n", \
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
		g_storage_stat.success_get_meta_count \
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

int storage_check_and_make_data_dirs()
{
	char data_path[MAX_PATH_SIZE];
	char dir_name[9];
	char sub_name[9];
	int i, k;
	int result;

	snprintf(data_path, sizeof(data_path), "%s/data", g_base_path);
	if (!fileExists(data_path))
	{
		if (mkdir(data_path, 0755) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"mkdir \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, data_path, errno, strerror(errno));
			return errno != 0 ? errno : ENOENT;
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

	if (fileExists(DATA_DIR_INITED_FILENAME))
	{
		IniItemInfo *items;
		int nItemCount;
		char *pValue;

		if ((result=iniLoadItems(DATA_DIR_INITED_FILENAME, \
				&items, &nItemCount)) \
			 != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"load from file \"%s/%s\" fail, " \
				"error code: %d", \
				__LINE__, data_path, \
				DATA_DIR_INITED_FILENAME, result);
			return result;
		}
		
		pValue = iniGetStrValue(INIT_ITEM_STORAGE_JOIN_TIME, \
				items, nItemCount);
		if (pValue == NULL)
		{
			iniFreeItems(items);
			logError("file: "__FILE__", line: %d, " \
				"in file \"%s/%s\", item \"%s\" not exists", \
				__LINE__, data_path, DATA_DIR_INITED_FILENAME, \
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
				__LINE__, data_path, DATA_DIR_INITED_FILENAME, \
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
				__LINE__, data_path, DATA_DIR_INITED_FILENAME, \
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

		return 0;
	}

	g_storage_join_time = time(NULL);
	for (i=0; i<DATA_DIR_COUNT_PER_PATH; i++)
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

		for (k=0; k<DATA_DIR_COUNT_PER_PATH; k++)
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

	fprintf(stderr, "mkdir data path done.\n");

	result = storage_write_to_sync_ini_file();

	return result;
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

int storage_load_from_conf_file(const char *filename, \
		char *bind_addr, const int addr_size)
{
	char *pBasePath;
	char *pBindAddr;
	char *pGroupName;
	char *pRunByGroup;
	char *pRunByUser;
	char *ppTrackerServers[FDFS_MAX_TRACKERS];
	IniItemInfo *items;
	int nItemCount;
	int result;

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
		if (iniGetBoolValue("disabled", items, nItemCount))
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\" disabled=true, exit", \
				__LINE__, filename);
			result = ECANCELED;
			break;
		}

		pBasePath = iniGetStrValue("base_path", items, nItemCount);
		if (pBasePath == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\" must have item " \
				"\"base_path\"!", \
				__LINE__, filename);
			result = ENOENT;
			break;
		}

		snprintf(g_base_path, sizeof(g_base_path), "%s", pBasePath);
		chopPath(g_base_path);
		if (!fileExists(g_base_path))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" can't be accessed, error info: %s", \
				__LINE__, strerror(errno), g_base_path);
			result = errno != 0 ? errno : ENOENT;
			break;
		}
		if (!isDir(g_base_path))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" is not a directory!", \
				__LINE__, g_base_path);
			result = ENOTDIR;
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

		logInfo("FastDFS v%d.%d, base_path=%s, " \
			"group_name=%s, " \
			"network_timeout=%ds, "\
			"port=%d, bind_addr=%s, " \
			"max_connections=%d, "    \
			"heart_beat_interval=%ds, " \
			"stat_report_interval=%ds, tracker_server_count=%d, " \
			"sync_wait_msec=%dms, allow_ip_count=%d", \
			g_version.major, g_version.minor, \
			g_base_path, g_group_name, \
			g_network_timeout, \
			g_server_port, bind_addr, g_max_connections, \
			g_heart_beat_interval, g_stat_report_interval, \
			g_tracker_server_count, g_sync_wait_usec / 1000, \
			g_allow_ip_count);

		break;
	}

	iniFreeItems(items);

	return result;
}

