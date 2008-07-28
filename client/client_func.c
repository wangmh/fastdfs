/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//client_func.c

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
#include "fdfs_define.h"
#include "logger.h"
#include "fdfs_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "ini_file_reader.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "client_global.h"
#include "client_func.h"

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
			logError( \
				"conf file \"%s\", " \
				"tracker_server \"%s\" is invalid, " \
				"correct format is host:port", \
				filename, *ppSrc);
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
			logError( \
				"conf file \"%s\", " \
				"host \"%s\" is invalid", \
				filename, szHost);
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

int fdfs_client_init(const char *filename)
{
	char *pBasePath;
	char *ppTrackerServers[FDFS_MAX_TRACKERS];
	IniItemInfo *items;
	int nItemCount;
	int result;

	if ((result=iniLoadItems(filename, &items, &nItemCount)) != 0)
	{
		logError( \
			"load conf file \"%s\" fail, ret code: %d", \
			filename, result);
		return result;
	}

	while (1)
	{
		pBasePath = iniGetStrValue("base_path", items, nItemCount);
		if (pBasePath == NULL)
		{
			logError( \
				"conf file \"%s\" must have item " \
				"\"base_path\"!", filename);
			result = ENOENT;
			break;
		}

		snprintf(g_base_path, sizeof(g_base_path), "%s", pBasePath);
		chopPath(g_base_path);
		if (!fileExists(g_base_path))
		{
			logError( \
				"\"%s\" can't be accessed, error info: %s", \
				strerror(errno), g_base_path);
			result = errno != 0 ? errno : ENOENT;
			break;
		}
		if (!isDir(g_base_path))
		{
			logError( \
				"\"%s\" is not a directory!", g_base_path);
			result = ENOTDIR;
			break;
		}

		g_network_timeout = iniGetIntValue("network_timeout", \
				items, nItemCount, DEFAULT_NETWORK_TIMEOUT);
		if (g_network_timeout <= 0)
		{
			g_network_timeout = DEFAULT_NETWORK_TIMEOUT;
		}

		if ((g_tracker_server_count=iniGetValues("tracker_server", \
			items, nItemCount, ppTrackerServers, \
			FDFS_MAX_TRACKERS)) <= 0)
		{
			logError( \
				"conf file \"%s\", " \
				"get item \"tracker_server\" fail", \
				filename);
			result = ENOENT;
			break;
		}

		g_tracker_servers = (TrackerServerInfo *)malloc( \
			sizeof(TrackerServerInfo) * g_tracker_server_count);
		if (g_tracker_servers == NULL)
		{
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

#ifdef __DEBUG__
		fprintf(stderr, "base_path=%s, " \
			"network_timeout=%d, "\
			"tracker_server_count=%d\n", \
			g_base_path, g_network_timeout, \
			g_tracker_server_count);
#endif

		break;
	}

	iniFreeItems(items);

	return result;
}

void fdfs_client_destroy()
{
	if (g_tracker_servers != NULL)
	{
		free(g_tracker_servers);
		g_tracker_servers = NULL;
	}
}

