/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <stdlib.h>
#include <string.h>
#include "client_global.h"

int g_tracker_server_http_port = 80;
TrackerServerGroup g_tracker_group = {0, 0, -1, NULL};

bool g_anti_steal_token = false;
BufferInfo g_anti_steal_secret_key = {0};

int fdfs_set_tracker_leader_ex(TrackerServerGroup *pServerGroup, \
		const char *leaderIp, const int leaderPort)
{
	TrackerServerInfo *pServer;
	TrackerServerInfo *pEnd;

	if (pServerGroup->server_count == 0)
	{
		return ENOENT;
	}

	pEnd = pServerGroup->servers + pServerGroup->server_count;
	for (pServer=pServerGroup->servers; pServer<pEnd; pServer++)
	{
		if (strcmp(pServer->ip_addr, leaderIp) == 0 && \
			pServer->port == leaderPort)
		{
			pServerGroup->leader_index = \
					pServer - pServerGroup->servers;
			return 0;
		}
	}

	return ENOENT;
}

