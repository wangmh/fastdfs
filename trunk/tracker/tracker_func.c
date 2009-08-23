/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_func.c

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
#include "tracker_global.h"
#include "tracker_func.h"
#include "tracker_mem.h"

#ifdef WITH_HTTPD
#include "fdfs_http_shared.h"
#endif

static int tracker_load_store_lookup(const char *filename, \
		IniItemInfo *items, const int nItemCount)
{
	char *pGroupName;
	g_groups.store_lookup = iniGetIntValue("store_lookup", \
			items, nItemCount, FDFS_STORE_LOOKUP_ROUND_ROBIN);
	if (g_groups.store_lookup == FDFS_STORE_LOOKUP_ROUND_ROBIN)
	{
		g_groups.store_group[0] = '\0';
		return 0;
	}

	if (g_groups.store_lookup == FDFS_STORE_LOOKUP_LOAD_BALANCE)
	{
		g_groups.store_group[0] = '\0';
		return 0;
	}

	if (g_groups.store_lookup != FDFS_STORE_LOOKUP_SPEC_GROUP)
	{
		logError("file: "__FILE__", line: %d, " \
			"conf file \"%s\", the value of \"store_lookup\" is " \
			"invalid, value=%d!", \
			__LINE__, filename, g_groups.store_lookup);
		return EINVAL;
	}

	pGroupName = iniGetStrValue("store_group", items, nItemCount);
	if (pGroupName == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"conf file \"%s\" must have item " \
			"\"store_group\"!", __LINE__, filename);
		return ENOENT;
	}
	if (pGroupName[0] == '\0')
	{
		logError("file: "__FILE__", line: %d, " \
			"conf file \"%s\", " \
			"store_group is empty!", __LINE__, filename);
		return EINVAL;
	}

	snprintf(g_groups.store_group, sizeof(g_groups.store_group), \
			"%s", pGroupName);
	if (fdfs_validate_group_name(g_groups.store_group) != 0) \
	{
		logError("file: "__FILE__", line: %d, " \
			"conf file \"%s\", " \
			"the group name \"%s\" is invalid!", \
			__LINE__, filename, g_groups.store_group);
		return EINVAL;
	}

	return 0;
}

int tracker_load_from_conf_file(const char *filename, \
		char *bind_addr, const int addr_size)
{
	char *pBasePath;
	char *pBindAddr;
	char *pStorageReserved;
	int64_t storage_reserved;
	char *pRunByGroup;
	char *pRunByUser;
	IniItemInfo *items;
	int nItemCount;
	int result;

	if ((result=iniLoadItems(filename, &items, &nItemCount)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"load conf file \"%s\" fail, ret code: %d", \
			__LINE__, filename, result);
		return result;
	}

	do
	{
		if (iniGetBoolValue("disabled", items, nItemCount, false))
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
				"\"base_path\"!", __LINE__, filename);
			result = ENOENT;
			break;
		}

		snprintf(g_base_path, sizeof(g_base_path), "%s", pBasePath);
		chopPath(g_base_path);
		if (!fileExists(g_base_path))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" can't be accessed, error info: %s", \
				__LINE__, g_base_path, strerror(errno));
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
		if ((result=log_init(g_base_path, \
				TRACKER_ERROR_LOG_FILENAME)) != 0)
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
				FDFS_TRACKER_SERVER_DEF_PORT);
		if (g_server_port <= 0)
		{
			g_server_port = FDFS_TRACKER_SERVER_DEF_PORT;
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

		if ((result=tracker_load_store_lookup(filename, \
			items, nItemCount)) != 0)
		{
			break;
		}

		g_groups.store_server = (byte)iniGetIntValue("store_server", \
			items, nItemCount, FDFS_STORE_SERVER_ROUND_ROBIN);
		if (!(g_groups.store_server == FDFS_STORE_SERVER_FIRST || \
			g_groups.store_server == FDFS_STORE_SERVER_ROUND_ROBIN))
		{
			logWarning("file: "__FILE__", line: %d, " \
				"store_server 's value %d is invalid, " \
				"set to %d (round robin)!", \
				__LINE__, g_groups.store_server, \
				FDFS_STORE_SERVER_ROUND_ROBIN);

			g_groups.store_server = FDFS_STORE_SERVER_ROUND_ROBIN;
		}

		g_groups.download_server = (byte)iniGetIntValue( \
			"download_server", items, nItemCount, \
			FDFS_DOWNLOAD_SERVER_ROUND_ROBIN);
		if (!(g_groups.download_server==FDFS_DOWNLOAD_SERVER_ROUND_ROBIN
			|| g_groups.download_server == 
				FDFS_DOWNLOAD_SERVER_SOURCE_FIRST))
		{
			logWarning("file: "__FILE__", line: %d, " \
				"download_server 's value %d is invalid, " \
				"set to %d (round robin)!", \
				__LINE__, g_groups.download_server, \
				FDFS_DOWNLOAD_SERVER_ROUND_ROBIN);

			g_groups.download_server = \
				FDFS_DOWNLOAD_SERVER_ROUND_ROBIN;
		}

		g_groups.store_path = (byte)iniGetIntValue("store_path", \
			items, nItemCount, FDFS_STORE_PATH_ROUND_ROBIN);
		if (!(g_groups.store_path == FDFS_STORE_PATH_ROUND_ROBIN || \
			g_groups.store_path == FDFS_STORE_PATH_LOAD_BALANCE))
		{
			logWarning("file: "__FILE__", line: %d, " \
				"store_path 's value %d is invalid, " \
				"set to %d (round robin)!", \
				__LINE__, g_groups.store_path , \
				FDFS_STORE_PATH_ROUND_ROBIN);
			g_groups.store_path = FDFS_STORE_PATH_ROUND_ROBIN;
		}

		pStorageReserved = iniGetStrValue("reserved_storage_space", \
						items, nItemCount);
		if (pStorageReserved == NULL)
		{
			g_storage_reserved_mb = FDFS_DEF_STORAGE_RESERVED_MB;
		}
		else
		{
			if ((result=parse_bytes(pStorageReserved, 1, \
					&storage_reserved)) != 0)
			{
				return result;
			}

			g_storage_reserved_mb = storage_reserved / FDFS_ONE_MB;
		}

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

		g_sync_log_buff_interval = iniGetIntValue( \
				"sync_log_buff_interval", items, nItemCount, \
				SYNC_LOG_BUFF_DEF_INTERVAL);
		if (g_sync_log_buff_interval <= 0)
		{
			g_sync_log_buff_interval = SYNC_LOG_BUFF_DEF_INTERVAL;
		}

#ifdef WITH_HTTPD
		if ((result=fdfs_http_params_load(items, nItemCount, \
				filename, &g_http_params)) != 0)
		{
			return result;
		}
#endif

		logInfo("FastDFS v%d.%d, base_path=%s, " \
			"network_timeout=%ds, "    \
			"port=%d, bind_addr=%s, " \
			"max_connections=%d, "    \
			"store_lookup=%d, store_group=%s, " \
			"store_server=%d, store_path=%d, " \
			"reserved_storage_space=%dMB, " \
			"download_server=%d, " \
			"allow_ip_count=%d, sync_log_buff_interval=%ds", \
			g_version.major, g_version.minor,  \
			g_base_path, \
			g_network_timeout, \
			g_server_port, bind_addr, g_max_connections, \
			g_groups.store_lookup, g_groups.store_group, \
			g_groups.store_server, g_groups.store_path, \
			g_storage_reserved_mb, g_groups.download_server, \
			g_allow_ip_count, g_sync_log_buff_interval);
#ifdef WITH_HTTPD
		if (!g_http_params.disabled)
		{
			logInfo("HTTP supported: " \
				"server_port=%d, " \
				"anti_steal_token=%d, " \
				"token_ttl=%ds, " \
				"anti_steal_secret_key length=%d, "  \
				"token_check_fail content_type=%s, " \
				"token_check_fail buff length=%d",  \
				g_http_params.server_port, \
				g_http_params.anti_steal_token, \
				g_http_params.token_ttl, \
				g_http_params.anti_steal_secret_key.length, \
				g_http_params.token_check_fail_content_type, \
				g_http_params.token_check_fail_buff.length);
		}
#endif

	} while (0);

	iniFreeItems(items);

	return result;
}

