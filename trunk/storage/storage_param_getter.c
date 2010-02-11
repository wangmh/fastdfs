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
#include "storage_param_getter.h"

static int storage_do_parameter_req(TrackerServerInfo *pTrackerServer, \
	char *buff, const int buff_size)
{
	char out_buff[sizeof(TrackerHeader)];
	TrackerHeader *pHeader;
	int64_t in_bytes;
	int result;

	memset(out_buff, 0, sizeof(out_buff));
	pHeader = (TrackerHeader *)out_buff;
	pHeader->cmd = TRACKER_PROTO_CMD_STORAGE_PARAMETER_REQ;
	if((result=tcpsenddata_nb(pTrackerServer->sock, out_buff, \
		sizeof(TrackerHeader), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, strerror(result));
		return result;
	}

	result = fdfs_recv_response(pTrackerServer, &buff, buff_size, &in_bytes);
	if (result != 0)
	{
		return result;
	}

	if (in_bytes >= buff_size)
	{
		logError("file: "__FILE__", line: %d, " \
			"server: %s:%d, recv body bytes: " \
			INT64_PRINTF_FORMAT" exceed max: %d", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, in_bytes, buff_size);
		return ENOSPC;
	}

	*(buff + in_bytes) = '\0';
	return 0;
}

static int storage_do_get_params_from_tracker( \
		IniContext *iniContext, int *count)
{
	TrackerServerInfo *pGlobalServer;
	TrackerServerInfo *pTServer;
	TrackerServerInfo *pTServerEnd;
	TrackerServerInfo trackerServer;
	char in_buff[1024];
	int result;
	int i;

	*count = 0;
	pTServer = &trackerServer;
	pTServerEnd = g_tracker_group.servers + g_tracker_group.server_count;

	while (*count == 0 && g_continue_flag)
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
			sleep(1);
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

		result = storage_do_parameter_req(pTServer, in_buff, \
						sizeof(in_buff));
		if (result == 0)
		{
			result = iniLoadFromBuffer(in_buff, \
					iniContext + (*count));
			if (result != 0)
			{
				close(pTServer->sock);
				return result;
			}

			(*count)++;
		}

		fdfs_quit(pTServer);
		close(pTServer->sock);
		sleep(1);
	}
	}

	if (!g_continue_flag)
	{
		return EINTR;
	}

	return 0;
}

int storage_get_params_from_tracker()
{
	IniContext *iniContext;
	int context_count;
	int bytes;
	int i;
	int result;

	bytes = sizeof(IniContext) * g_tracker_group.server_count;
	iniContext = (IniContext *)malloc(bytes);
	if (iniContext == NULL)
	{
		result = errno != 0 ? errno : ENOMEM;
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes failed, errno: %d, " \
			"error info: %s", __LINE__, \
			bytes, result, strerror(result));
		return result;
	}

	memset(iniContext, 0, bytes);
	result = storage_do_get_params_from_tracker( \
                	iniContext, &context_count);
	if (result != 0)
	{
		free(iniContext);
		return result;
	}

	if (context_count == 0)
	{
		free(iniContext);
		return ENOENT;
	}

	for (i=0; i<context_count; i++)
	{
		g_storage_ip_changed_auto_adjust = iniGetBoolValue(NULL, \
				"storage_ip_changed_auto_adjust", \
				iniContext + i, false);
		if (g_storage_ip_changed_auto_adjust)
		{
			break;
		}
	}

	for (i=0; i<context_count; i++)
	{
		iniFreeContext(iniContext + i);
	}
	free(iniContext);

	logInfo("file: "__FILE__", line: %d, " \
		"storage_ip_changed_auto_adjust=%d", __LINE__, \
		g_storage_ip_changed_auto_adjust);

	return 0;
}

