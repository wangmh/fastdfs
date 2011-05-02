/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <pthread.h>
#include "fdfs_define.h"
#include "logger.h"
#include "sockopt.h"
#include "fdfs_global.h"
#include "shared_func.h"
#include "pthread_func.h"
#include "tracker_global.h"
#include "tracker_proto.h"
#include "tracker_mem.h"

bool g_if_leader_self = false;  //if I am leader

static int relationship_cmp_tracker_status(const void *p1, const void *p2)
{
	TrackerRunningStatus *pStatus1;
	TrackerRunningStatus *pStatus2;
	TrackerServerInfo *pTrackerServer1;
	TrackerServerInfo *pTrackerServer2;
	int sub;

	pStatus1 = (TrackerRunningStatus *)p1;
	pStatus2 = (TrackerRunningStatus *)p2;

	sub = pStatus1->if_leader - pStatus2->if_leader;
	if (sub != 0)
	{
		return sub;
	}

	sub = pStatus1->running_time - pStatus2->running_time;
	if (sub != 0)
	{
		return sub;
	}

	sub = pStatus2->restart_interval - pStatus1->restart_interval;
	if (sub != 0)
	{
		return sub;
	}

	pTrackerServer1 = pStatus1->pTrackerServer;
	pTrackerServer2 = pStatus2->pTrackerServer;
	sub = strcmp(pTrackerServer1->ip_addr, pTrackerServer2->ip_addr);
	if (sub != 0)
	{
		return sub;
	}

	return pTrackerServer1->port - pTrackerServer2->port;
}

static int relationship_get_tracker_leader(TrackerRunningStatus *pTrackerStatus)
{
	TrackerServerInfo *pTrackerServer;
	TrackerServerInfo *pTrackerEnd;
	TrackerRunningStatus *pStatus;
	TrackerRunningStatus trackerStatus[FDFS_MAX_TRACKERS];
	int count;
	int result;
	int r;
	int i;

	memset(pTrackerStatus, 0, sizeof(TrackerRunningStatus));
	pStatus = trackerStatus;
	result = 0;
	pTrackerEnd = g_tracker_servers.servers + g_tracker_servers.server_count;
	for (pTrackerServer=g_tracker_servers.servers; \
		pTrackerServer<pTrackerEnd; pTrackerServer++)
	{
		pStatus->pTrackerServer = pTrackerServer;
		r = tracker_mem_get_status(pTrackerServer, pStatus);
		if (r == 0)
		{
			pStatus++;
		}
		else if (r != ENOENT)
		{
			result = r;
		}
	}

	count = pStatus - trackerStatus;
	if (count == 0)
	{
		return result == 0 ? ENOENT : result;
	}

	qsort(trackerStatus, count, sizeof(TrackerRunningStatus), \
		relationship_cmp_tracker_status);

	for (i=0; i<count; i++)
	{
		logDebug("file: "__FILE__", line: %d, " \
			"%s:%d if_leader: %d, running time: %d, " \
			"restart interval: %d", __LINE__, \
			trackerStatus[i].pTrackerServer->ip_addr, \
			trackerStatus[i].pTrackerServer->port, \
			trackerStatus[i].running_time, \
			trackerStatus[i].restart_interval);
	}

	memcpy(pTrackerStatus, trackerStatus + (count - 1), \
			sizeof(TrackerRunningStatus));
	return 0;
}

#define relationship_notify_next_leader(pTrackerServer, pLeader, bConnectFail) \
	do_notify_leader_changed(pTrackerServer, pLeader, \
		TRACKER_PROTO_CMD_TRACKER_NOTIFY_NEXT_LEADER, bConnectFail)

#define relationship_commit_next_leader(pTrackerServer, pLeader, bConnectFail) \
	do_notify_leader_changed(pTrackerServer, pLeader, \
		TRACKER_PROTO_CMD_TRACKER_COMMIT_NEXT_LEADER, bConnectFail)

static int do_notify_leader_changed(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pLeader, const char cmd, bool *bConnectFail)
{
	char out_buff[sizeof(TrackerHeader) + FDFS_PROTO_IP_PORT_SIZE];
	char in_buff[1];
	TrackerHeader *pHeader;
	char *pInBuff;
	int64_t in_bytes;
	int result;

	pTrackerServer->sock = -1;
	if ((result=tracker_connect_server(pTrackerServer)) != 0)
	{
		*bConnectFail = true;
		return result;
	}
	*bConnectFail = false;

	do
	{
	memset(out_buff, 0, sizeof(out_buff));
	pHeader = (TrackerHeader *)out_buff;
	pHeader->cmd = cmd;
	sprintf(out_buff + sizeof(TrackerHeader), "%s:%d", \
			pLeader->ip_addr, pLeader->port);
	long2buff(FDFS_PROTO_IP_PORT_SIZE, pHeader->pkg_len);
	if ((result=tcpsenddata_nb(pTrackerServer->sock, out_buff, \
			sizeof(out_buff), g_fdfs_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"send data to tracker server %s:%d fail, " \
			"errno: %d, error info: %s", __LINE__, \
			pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, STRERROR(result));

		result = (result == ENOENT ? EACCES : result);
		break;
	}

	pInBuff = in_buff;
	result = fdfs_recv_response(pTrackerServer, &pInBuff, \
				0, &in_bytes);
	if (result != 0)
	{
		break;
	}

	if (in_bytes != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d response data " \
			"length: "INT64_PRINTF_FORMAT" is invalid, " \
			"expect length: %d.", __LINE__, \
			pTrackerServer->ip_addr, pTrackerServer->port, \
			in_bytes, 0);
		result = EINVAL;
		break;
	}
	} while (0);

	close(pTrackerServer->sock);
	pTrackerServer->sock = -1;

	return result;
}

static int relationship_notify_leader_changed(TrackerServerInfo *pLeader)
{
	TrackerServerInfo *pTrackerServer;
	TrackerServerInfo *pTrackerEnd;
	int result;
	bool bConnectFail;
	int success_count;

	result = ENOENT;
	pTrackerEnd = g_tracker_servers.servers + g_tracker_servers.server_count;
	success_count = 0;
	for (pTrackerServer=g_tracker_servers.servers; \
		pTrackerServer<pTrackerEnd; pTrackerServer++)
	{
		if (pTrackerServer->port == g_server_port && \
			is_local_host_ip(pTrackerServer->ip_addr))
		{
			continue;
		}

		if ((result=relationship_notify_next_leader(pTrackerServer, \
				pLeader, &bConnectFail)) != 0)
		{
			if (!bConnectFail)
			{
				return result;
			}
		}
		else
		{
			success_count++;
		}
	}

	if (success_count == 0)
	{
		return result;
	}

	success_count = 0;
	for (pTrackerServer=g_tracker_servers.servers; \
		pTrackerServer<pTrackerEnd; pTrackerServer++)
	{
		if (pTrackerServer->port == g_server_port && \
			is_local_host_ip(pTrackerServer->ip_addr))
		{
			continue;
		}

		if ((result=relationship_commit_next_leader(pTrackerServer, \
				pLeader, &bConnectFail)) != 0)
		{
			if (!bConnectFail)
			{
				return result;
			}
		}
		else
		{
			success_count++;
		}
	}
	if (success_count == 0)
	{
		return result;
	}

	return 0;
}

static int relationship_select_leader()
{
	int result;
	TrackerRunningStatus trackerStatus;

	if (g_tracker_servers.server_count <= 0)
	{
		return 0;
	}

	if ((result=relationship_get_tracker_leader(&trackerStatus)) != 0)
	{
		return result;
	}

	if (trackerStatus.if_leader)  //leader not changed
	{
		return 0;
	}

	if (trackerStatus.pTrackerServer->port == g_server_port && \
		is_local_host_ip(trackerStatus.pTrackerServer->ip_addr))
	{
		if ((result=relationship_notify_leader_changed( \
				trackerStatus.pTrackerServer)) != 0)
		{
			return result;
		}

		g_tracker_servers.leader_index = trackerStatus.pTrackerServer \
						 - g_tracker_servers.servers;
		g_if_leader_self = true;
	}

	return 0;
}

static int relationship_ping_leader()
{
	if (g_if_leader_self)
	{
		return 0;
	}

	return 0;
}

static void *relationship_thread_entrance(void* arg)
{
	while (g_continue_flag)
	{
		if (g_tracker_servers.servers != NULL)
		{
			if (g_tracker_servers.leader_index < 0)
			{
				relationship_select_leader();
			}
			else
			{
				relationship_ping_leader();
			}
		}

		if (g_last_tracker_servers != NULL)
		{
			tracker_mem_file_lock();

			free(g_last_tracker_servers);
			g_last_tracker_servers = NULL;

			tracker_mem_file_unlock();
		}

		sleep(1);
	}

	return NULL;
}

int tracker_relationship_init()
{
	int result;
	pthread_t tid;
	pthread_attr_t thread_attr;

	if ((result=init_pthread_attr(&thread_attr, g_thread_stack_size)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_attr fail, program exit!", __LINE__);
		return result;
	}

	if ((result=pthread_create(&tid, &thread_attr, \
			relationship_thread_entrance, NULL)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"create thread failed, errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	pthread_attr_destroy(&thread_attr);

	return 0;
}

int tracker_relationship_destroy()
{
	return 0;
}

