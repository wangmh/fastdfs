/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/


#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/statvfs.h>
#include <sys/param.h>
#include "fdfs_define.h"
#include "logger.h"
#include "fdfs_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "storage_global.h"
#include "storage_func.h"
#include "storage_sync.h"
#include "tracker_client.h"
#include "storage_disk_recovery.h"

#define RECOVERY_BINLOG_FILENAME	".binlog.recovery"
#define RECOVERY_MARK_FILENAME		".recovery.mark"
#define MARK_ITEM_BINLOG_OFFSET    	"binlog_offset"
#define MARK_ITEM_FETCH_BINLOG_DONE    	"fetch_binlog_done"

/*
static int storage_do_changelog_req(TrackerServerInfo *pTrackerServer)
{
	char out_buff[sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN];
	TrackerHeader *pHeader;
	int result;

	memset(out_buff, 0, sizeof(out_buff));
	pHeader = (TrackerHeader *)out_buff;

	long2buff(FDFS_GROUP_NAME_MAX_LEN, pHeader->pkg_len);
	pHeader->cmd = TRACKER_PROTO_CMD_STORAGE_CHANGELOG_REQ;
	strcpy(out_buff + sizeof(TrackerHeader), g_group_name);
	if((result=tcpsenddata_nb(pTrackerServer->sock, out_buff, \
		sizeof(TrackerHeader) + FDFS_GROUP_NAME_MAX_LEN, \
		g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, STRERROR(result));
		return result;
	}

	return tracker_deal_changelog_response(pTrackerServer);
}
*/

static int recovery_get_src_storage_server(TrackerServerInfo *pSrcStorage)
{
	int result;
	int status;
	int storage_count;
	TrackerServerInfo trackerServer;
	FDFSGroupStat groupStat;
	FDFSStorageInfo storageStats[FDFS_MAX_SERVERS_EACH_GROUP];
	FDFSStorageInfo *pStorageStat;
	FDFSStorageInfo *pStorageEnd;

	memset(pSrcStorage, 0, sizeof(TrackerServerInfo));
	pSrcStorage->sock = -1;

	logDebug("file: "__FILE__", line: %d, " \
		"disk recovery: get source storage server", \
		__LINE__);
	while (g_continue_flag)
	{
		result = tracker_get_storage_status(&g_tracker_group, \
                	g_group_name, g_tracker_client_ip, &status);
		if (result == ENOENT)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"current storage: %s does not exist " \
				"in tracker server", __LINE__, \
				g_tracker_client_ip);
			return ENOENT;
		}

		if (result == 0)
		{
			if (status == FDFS_STORAGE_STATUS_INIT)
			{
				logInfo("file: "__FILE__", line: %d, " \
					"current storage: %s 's status is %d" \
					", does not need recovery", __LINE__, \
					g_tracker_client_ip);
				return ENOENT;
			}

			if (status == FDFS_STORAGE_STATUS_IP_CHANGED || \
			    status == FDFS_STORAGE_STATUS_DELETED)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"current storage: %s 's status is %d" \
					", does not need recovery", __LINE__, \
					g_tracker_client_ip, status);
				return ENOENT;
			}

			break;
		}

		sleep(1);
	}

	while (g_continue_flag)
	{
		if ((result=tracker_get_connection_r(&trackerServer)) != 0)
		{
			sleep(1);
			continue;
		}

		result = tracker_list_one_group(&trackerServer, \
				g_group_name, &groupStat);
		if (result != 0)
		{
			close(trackerServer.sock);
			sleep(1);
			continue;
		}

		if (groupStat.count <= 0)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"storage server count: %d in the group <= 0!",\
				__LINE__, groupStat.count);

			close(trackerServer.sock);
			sleep(1);
			continue;
		}

		if (groupStat.count == 1)
		{
			logInfo("file: "__FILE__", line: %d, " \
				"storage server count in the group = 1, " \
				"does not need recovery", __LINE__);

			close(trackerServer.sock);
			return ENOENT;
		}

		if (groupStat.active_count <= 0)
		{
			close(trackerServer.sock);
			sleep(5);
			continue;
		}

		result = tracker_list_servers(&trackerServer, \
                		g_group_name, NULL, storageStats, \
				FDFS_MAX_SERVERS_EACH_GROUP, &storage_count);
		close(trackerServer.sock);
		if (result != 0)
		{
			sleep(5);
			continue;
		}

		if (storage_count <= 1)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"storage server count: %d in the group <= 1!",\
				__LINE__, storage_count);

			sleep(5);
			continue;
		}

		pStorageEnd = storageStats + storage_count;
		for (pStorageStat=storageStats; pStorageStat<pStorageEnd; \
			pStorageStat++)
		{
			if (strcmp(pStorageStat->ip_addr, \
				g_tracker_client_ip) == 0)
			{
				continue;
			}

			if (pStorageStat->status == FDFS_STORAGE_STATUS_ACTIVE)
			{
				strcpy(pSrcStorage->ip_addr, \
					pStorageStat->ip_addr);
				pSrcStorage->port = pStorageStat->storage_port;
				break;
			}
		}

		if (pStorageStat < pStorageEnd)  //found src storage server
		{
			break;
		}

		sleep(5);
	}

	if (!g_continue_flag)
	{
		return EINTR;
	}

	return 0;
}

static char *recovery_get_full_filename(const void *pArg, \
		const char *filename, char *full_filename)
{
	const char *pBasePath;
	static char buff[MAX_PATH_SIZE];

	pBasePath = (const char *)pArg;
	if (full_filename == NULL)
	{
		full_filename = buff;
	}

	snprintf(full_filename, MAX_PATH_SIZE, \
		"%s/data/%s", pBasePath, filename);

	return full_filename;
}

static char *recovery_get_binlog_filename(const void *pArg, \
                        char *full_filename)
{
	return recovery_get_full_filename(pArg, \
			RECOVERY_BINLOG_FILENAME, full_filename);
}

static char *recovery_get_mark_filename(const void *pArg, \
                        char *full_filename)
{
	return recovery_get_full_filename(pArg, \
			RECOVERY_MARK_FILENAME, full_filename);
}

static int storage_disk_recovery_finish(const char *pBasePath)
{
	char full_filename[MAX_PATH_SIZE];

	recovery_get_binlog_filename(pBasePath, full_filename);
	if (fileExists(full_filename))
	{
		if (unlink(full_filename) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"delete recovery binlog file: %s fail, " \
				"errno: %d, error info: %s", \
				 __LINE__, full_filename, \
				errno, STRERROR(errno));
			return errno != 0 ? errno : EPERM;
		}
	}

	recovery_get_mark_filename(pBasePath, full_filename);
	if (fileExists(full_filename))
	{
		if (unlink(full_filename) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"delete recovery mark file: %s fail, " \
				"errno: %d, error info: %s", \
				 __LINE__, full_filename, \
				errno, STRERROR(errno));
			return errno != 0 ? errno : EPERM;
		}
	}

	return 0;
}

static int recovery_write_to_mark_file(const char *pBasePath, \
			BinLogReader *pReader)
{
	char buff[128];
	int len;

	len = sprintf(buff, \
		"%s="INT64_PRINTF_FORMAT"\n"  \
		"%s=1\n",  \
		MARK_ITEM_BINLOG_OFFSET, pReader->binlog_offset, \
		MARK_ITEM_FETCH_BINLOG_DONE);

	return storage_write_to_fd(pReader->mark_fd, \
		recovery_get_mark_filename, pBasePath, buff, len);
}

static int recovery_init_mark_file(const char *pBasePath, \
		const bool fetch_binlog_done)
{
	char full_filename[MAX_PATH_SIZE];
	char buff[128];
	int len;

	recovery_get_mark_filename(pBasePath, full_filename);

	len = sprintf(buff, \
		"%s=0\n" \
		"%s=%d\n", \
		MARK_ITEM_BINLOG_OFFSET, \
		MARK_ITEM_FETCH_BINLOG_DONE, fetch_binlog_done);
	return writeToFile(full_filename, buff, len);
}

static int recovery_reader_init(const char *pBasePath, \
                        BinLogReader *pReader)
{
	char full_mark_filename[MAX_PATH_SIZE];
	IniContext iniContext;
	int result;

	memset(pReader, 0, sizeof(BinLogReader));
	pReader->mark_fd = -1;
	pReader->binlog_fd = -1;
	pReader->binlog_index = g_binlog_index + 1;

	pReader->binlog_buff.buffer = (char *)malloc( \
			STORAGE_BINLOG_BUFFER_SIZE);
	if (pReader->binlog_buff.buffer == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, STORAGE_BINLOG_BUFFER_SIZE, \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	pReader->binlog_buff.current = pReader->binlog_buff.buffer;

	recovery_get_mark_filename(pBasePath, full_mark_filename);
	memset(&iniContext, 0, sizeof(IniContext));
	if ((result=iniLoadFromFile(full_mark_filename, &iniContext)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"load from mark file \"%s\" fail, " \
			"error code: %d", __LINE__, \
			full_mark_filename, result);
		return result;
	}

	if (!iniGetBoolValue(NULL, MARK_ITEM_FETCH_BINLOG_DONE, \
			&iniContext, false))
	{
		iniFreeContext(&iniContext);

		logInfo("file: "__FILE__", line: %d, " \
			"mark file \"%s\", %s=0, " \
			"need to fetch binlog again", __LINE__, \
			full_mark_filename, MARK_ITEM_FETCH_BINLOG_DONE);
		return EAGAIN;
	}

	pReader->binlog_offset = iniGetInt64Value(NULL, \
			MARK_ITEM_BINLOG_OFFSET, &iniContext, -1);
	if (pReader->binlog_offset < 0)
	{
		iniFreeContext(&iniContext);

		logError("file: "__FILE__", line: %d, " \
			"in mark file \"%s\", binlog_offset: "\
			INT64_PRINTF_FORMAT" < 0", __LINE__, \
			full_mark_filename, pReader->binlog_offset);
		return EINVAL;
	}

	iniFreeContext(&iniContext);

	pReader->mark_fd = open(full_mark_filename, O_WRONLY | O_CREAT, 0644);
	if (pReader->mark_fd < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open mark file \"%s\" fail, " \
			"error no: %d, error info: %s", \
			__LINE__, full_mark_filename, \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if ((result=storage_open_readable_binlog(pReader, \
			recovery_get_binlog_filename, pBasePath)) != 0)
	{
		return result;
	}

	return 0;
}

int storage_disk_recovery_restore(const char *pBasePath)
{
	char full_binlog_filename[MAX_PATH_SIZE];
	char full_mark_filename[MAX_PATH_SIZE];
	TrackerServerInfo srcStorage;
	int result;
	BinLogReader reader;

	recovery_get_binlog_filename(pBasePath, full_binlog_filename);
	recovery_get_mark_filename(pBasePath, full_mark_filename);

	if (!(fileExists(full_mark_filename) && \
	      fileExists(full_binlog_filename)))
	{
		return 0;
	}

	if ((result=recovery_get_src_storage_server(&srcStorage)) != 0)
	{
		if (result == ENOENT)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"no source storage server, " \
				"disk recovery finished!", __LINE__);
			return storage_disk_recovery_finish(pBasePath);
		}
		else
		{
			return result;
		}
	}

	if ((result=recovery_reader_init(pBasePath, &reader)) != 0)
	{
		storage_reader_destroy(&reader);
		return result;
	}

	recovery_write_to_mark_file(pBasePath, &reader);
	storage_reader_destroy(&reader);
	return 0;
}

int storage_disk_recovery_start(const char *pBasePath)
{
	char full_binlog_filename[MAX_PATH_SIZE];
	TrackerServerInfo srcStorage;
	int result;

	if ((result=storage_disk_recovery_finish(pBasePath)) != 0)
	{
		return result;
	}

	if ((result=recovery_get_src_storage_server(&srcStorage)) != 0)
	{
		return result == ENOENT ? 0 : result;
	}

	if ((result=recovery_init_mark_file(pBasePath, false)) != 0)
	{
		return result;
	}

	recovery_get_binlog_filename(pBasePath, full_binlog_filename);

	//set fetch binlog done
	if ((result=recovery_init_mark_file(pBasePath, true)) != 0)
	{
		return result;
	}

	return 0;
}

