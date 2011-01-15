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
#include "storage_disk_recovery.h"

int storage_disk_recovery(const char *pBasePath)
{
	return 0;
}

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

int storage_changelog_req()
{
	TrackerServerInfo *pGlobalServer;
	TrackerServerInfo *pTServer;
	TrackerServerInfo *pTServerEnd;
	TrackerServerInfo trackerServer;
	int success_count;
	int result;
	int i;

	result = 0;
	success_count = 0;
	pTServer = &trackerServer;
	pTServerEnd = g_tracker_group.servers + g_tracker_group.server_count;

	while (success_count == 0 && g_continue_flag)
	{
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
					__LINE__, result, STRERROR(result));
				sleep(5);
				break;
			}

			if (g_client_bind_addr && *g_bind_addr != '\0')
			{
				socketBind(pTServer->sock, g_bind_addr, 0);
			}

			if (tcpsetnonblockopt(pTServer->sock) != 0)
			{
				close(pTServer->sock);
				pTServer->sock = -1;
				sleep(1);
				continue;
			}

			if ((result=connectserverbyip_nb(pTServer->sock, \
				pTServer->ip_addr, pTServer->port, \
				g_fdfs_connect_timeout)) == 0)
			{
				break;
			}

			close(pTServer->sock);
			pTServer->sock = -1;
			sleep(1);
		}

		if (pTServer->sock < 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"connect to tracker server %s:%d fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pTServer->ip_addr, pTServer->port, \
				result, STRERROR(result));

			continue;
		}

		result = storage_do_changelog_req(pTServer);
		if (result == 0 || result == ENOENT)
		{
			success_count++;
		}
		else
		{
			sleep(1);
		}

		fdfs_quit(pTServer);
		close(pTServer->sock);
	}
	}

	if (!g_continue_flag)
	{
		return EINTR;
	}

	return 0;
}

int storage_check_ip_changed()
{
	int result;

	if (!g_storage_ip_changed_auto_adjust)
	{
		return 0;
	}

	if ((result=storage_report_storage_ip_addr()) != 0)
	{
		return result;
	}

	if (*g_last_storage_ip == '\0') //first run
	{
		return 0;
	}

	return storage_changelog_req();
}

