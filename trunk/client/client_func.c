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
#include "fdfs_base64.h"
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

static void insert_into_sorted_servers(TrackerServerGroup *pTrackerGroup, \
		TrackerServerInfo *pInsertedServer)
{
	TrackerServerInfo *pDestServer;
	for (pDestServer=pTrackerGroup->servers+pTrackerGroup->server_count; \
		pDestServer>pTrackerGroup->servers; pDestServer--)
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

static int copy_tracker_servers(TrackerServerGroup *pTrackerGroup, \
		const char *filename, char **ppTrackerServers)
{
	char **ppSrc;
	char **ppEnd;
	TrackerServerInfo destServer;
	char *pSeperator;
	char szHost[128];
	int nHostLen;

	memset(&destServer, 0, sizeof(TrackerServerInfo));
	ppEnd = ppTrackerServers + pTrackerGroup->server_count;

	pTrackerGroup->server_count = 0;
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

		if (bsearch(&destServer, pTrackerGroup->servers, \
			pTrackerGroup->server_count, \
			sizeof(TrackerServerInfo), \
			storage_cmp_by_ip_and_port) == NULL)
		{
			insert_into_sorted_servers(pTrackerGroup, &destServer);
			pTrackerGroup->server_count++;
		}
	}

	/*
	{
	TrackerServerInfo *pServer;
	for (pServer=pTrackerGroup->servers; pServer<pTrackerGroup->servers+ \
		pTrackerGroup->server_count;	pServer++)
	{
		//printf("server=%s:%d\n", \
			pServer->ip_addr, pServer->port);
	}
	}
	*/

	return 0;
}

int fdfs_load_tracker_group_ex(TrackerServerGroup *pTrackerGroup, \
		const char *conf_filename, IniItemContext *pItemContext)
{
	int result;
	char *ppTrackerServers[FDFS_MAX_TRACKERS];

	if ((pTrackerGroup->server_count=iniGetValues("tracker_server", \
		pItemContext, ppTrackerServers, FDFS_MAX_TRACKERS)) <= 0)
	{
		logError("conf file \"%s\", " \
			"get item \"tracker_server\" fail", conf_filename);
		return ENOENT;
	}

	pTrackerGroup->servers = (TrackerServerInfo *)malloc( \
		sizeof(TrackerServerInfo) * pTrackerGroup->server_count);
	if (pTrackerGroup->servers == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", __LINE__, \
			(int)sizeof(TrackerServerInfo) * \
			pTrackerGroup->server_count);
		pTrackerGroup->server_count = 0;
		return errno != 0 ? errno : ENOMEM;
	}

	memset(pTrackerGroup->servers, 0, \
		sizeof(TrackerServerInfo) * pTrackerGroup->server_count);
	if ((result=copy_tracker_servers(pTrackerGroup, conf_filename, \
			ppTrackerServers)) != 0)
	{
		pTrackerGroup->server_count = 0;
		free(pTrackerGroup->servers);
		pTrackerGroup->servers = NULL;
		return result;
	}

	return 0;
}

int fdfs_load_tracker_group(TrackerServerGroup *pTrackerGroup, \
		const char *conf_filename)
{
	IniItemContext itemContext;
	int result;

	if ((result=iniLoadItems(conf_filename, &itemContext)) != 0)
	{
		logError("load conf file \"%s\" fail, ret code: %d", \
			conf_filename, result);
		return result;
	}

	result = fdfs_load_tracker_group_ex(pTrackerGroup, conf_filename, \
			&itemContext);
	iniFreeItems(&itemContext);

	return result;
}

int fdfs_client_init_ex(TrackerServerGroup *pTrackerGroup, \
		const char *conf_filename)
{
	char *pBasePath;
	IniItemContext itemContext;
	int result;

	if ((result=iniLoadItems(conf_filename, &itemContext)) != 0)
	{
		logError("load conf file \"%s\" fail, ret code: %d", \
			conf_filename, result);
		return result;
	}

	do
	{
		pBasePath = iniGetStrValue("base_path", &itemContext);
		if (pBasePath == NULL)
		{
			logError("conf file \"%s\" must have item " \
				"\"base_path\"!", conf_filename);
			result = ENOENT;
			break;
		}

		snprintf(g_base_path, sizeof(g_base_path), "%s", pBasePath);
		chopPath(g_base_path);
		if (!fileExists(g_base_path))
		{
			logError("\"%s\" can't be accessed, error info: %s", \
				g_base_path, strerror(errno));
			result = errno != 0 ? errno : ENOENT;
			break;
		}
		if (!isDir(g_base_path))
		{
			logError("\"%s\" is not a directory!", g_base_path);
			result = ENOTDIR;
			break;
		}

		g_network_timeout = iniGetIntValue("network_timeout", \
				&itemContext, DEFAULT_NETWORK_TIMEOUT);
		if (g_network_timeout <= 0)
		{
			g_network_timeout = DEFAULT_NETWORK_TIMEOUT;
		}

		result = fdfs_load_tracker_group_ex(pTrackerGroup, \
			conf_filename, &itemContext);
		if (result != 0)
		{
			break;
		}

		g_anti_steal_token = iniGetBoolValue( \
				"http.anti_steal.check_token", \
				&itemContext, false);
		if (g_anti_steal_token)
		{
			char *anti_steal_secret_key;

			anti_steal_secret_key = iniGetStrValue( \
					"http.anti_steal.secret_key", \
					&itemContext);
			if (anti_steal_secret_key == NULL || \
				*anti_steal_secret_key == '\0')
			{
				logError("file: "__FILE__", line: %d, " \
					"param \"http.anti_steal.secret_key\""\
					" not exist or is empty", __LINE__);
				result = EINVAL;
				break;
			}

			buffer_strcpy(&g_anti_steal_secret_key, \
				anti_steal_secret_key);
		}

		g_tracker_server_http_port = iniGetIntValue( \
				"http.tracker_server_port", \
				&itemContext, 80);
		if (g_tracker_server_http_port <= 0)
		{
			g_tracker_server_http_port = 80;
		}

#ifdef DEBUG_FLAG
		fprintf(stderr, "base_path=%s, " \
			"network_timeout=%d, "\
			"tracker_server_count=%d, " \
			"anti_steal_token=%d, " \
			"anti_steal_secret_key length=%d\n", \
			g_base_path, g_network_timeout, \
			pTrackerGroup->server_count, g_anti_steal_token, \
			g_anti_steal_secret_key.length);
#endif

	} while (0);

	iniFreeItems(&itemContext);

	return result;
}

int fdfs_copy_tracker_group(TrackerServerGroup *pDestTrackerGroup, \
		TrackerServerGroup *pSrcTrackerGroup)
{
	int bytes;
	TrackerServerInfo *pDestServer;
	TrackerServerInfo *pDestServerEnd;

	bytes = sizeof(TrackerServerInfo) * pSrcTrackerGroup->server_count;
	pDestTrackerGroup->servers = (TrackerServerInfo *)malloc(bytes);
	if (pDestTrackerGroup->servers == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", __LINE__, bytes);
		return errno != 0 ? errno : ENOMEM;
	}

	pDestTrackerGroup->server_index = 0;
	pDestTrackerGroup->server_count = pSrcTrackerGroup->server_count;
	memcpy(pDestTrackerGroup->servers, pSrcTrackerGroup->servers, bytes);

	pDestServerEnd = pDestTrackerGroup->servers + \
			pDestTrackerGroup->server_count;
	for (pDestServer=pDestTrackerGroup->servers; \
		pDestServer<pDestServerEnd; pDestServer++)
	{
		pDestServer->sock = -1;
	}

	return 0;
}

void fdfs_client_destroy_ex(TrackerServerGroup *pTrackerGroup)
{
	if (pTrackerGroup->servers != NULL)
	{
		free(pTrackerGroup->servers);
		pTrackerGroup->servers = NULL;

		pTrackerGroup->server_count = 0;
		pTrackerGroup->server_index = 0;
	}
}


