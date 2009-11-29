/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include "sockopt.h"
#include "fdfs_client.h"

static TrackerServerInfo *pTrackerServer;

static int list_all_groups(const char *group_name);

int main(int argc, char *argv[])
{
	char *conf_filename;
	int result;
	char *op_type;
	
	if (argc < 2)
	{
		printf("Usage: %s <config_file> [list|delete <group_name> " \
			"[storage_ip]]\n", argv[0]);
		return 1;
	}

	if (argc == 2)
	{
		op_type = "list";
	}
	else
	{
		op_type = argv[2];
	}

	conf_filename = argv[1];
	if ((result=fdfs_client_init(conf_filename)) != 0)
	{
		return result;
	}

	pTrackerServer = tracker_get_connection();
	if (pTrackerServer == NULL)
	{
		fdfs_client_destroy();
		return errno != 0 ? errno : ECONNREFUSED;
	}

	if (strcmp(op_type, "list") == 0)
	{
		if (argc <= 3)
		{
			result = list_all_groups(NULL);
		}
		else
		{
			result = list_all_groups(argv[3]);
		}

		if (fdfs_quit(pTrackerServer) != 0)
		{
		}
	}
	else if (strcmp(op_type, "delete") == 0)
	{
		TrackerServerInfo *pServer;
		TrackerServerInfo *pEnd;

		if (argc < 5)
		{
			printf("Usage: %s <config_file> delete <group_name> " \
				"<storage_ip>\n", argv[0]);
			return 1;
		}

		if ((result=tracker_get_all_connections()) != 0)
		{
			printf("connect to tracker server fail!\n");
			return result;
		}

		pEnd = g_tracker_group.servers + g_tracker_group.server_count;
		for (pServer=g_tracker_group.servers; pServer<pEnd; pServer++)
		{
			if (pServer->sock <= 0)
			{
				continue;
			}

			if ((result=tracker_delete_storage(pServer, \
				argv[3], argv[4])) == 0)
			{
				printf("delete storage server %s::%s from " \
					"tracker server %s success.\n", \
					argv[3], argv[4], pServer->ip_addr);
			}
			else
			{
				printf("delete storage server %s::%s from " \
					"tracker server %s fail, " \
					"error no: %d, error info: %s\n", \
					argv[3], argv[4], pServer->ip_addr, \
					result, strerror(result));
			}

			fdfs_quit(pServer);
		}
	}

	tracker_close_all_connections();
	fdfs_client_destroy();
	return 0;
}

static int list_storages(FDFSGroupStat *pGroupStat)
{
	int result;
	int storage_count;
	FDFSStorageInfo storage_infos[FDFS_MAX_SERVERS_EACH_GROUP];
	FDFSStorageInfo *pStorage;
	FDFSStorageInfo *pStorageEnd;
	FDFSStorageStat *pStorageStat;
	char szLastHeartBeatTime[32];
	char szSrcUpdTime[32];
	char szSyncUpdTime[32];
	char szSyncedTimestamp[32];
	char szHostname[128];
	char szHostnamePrompt[128+8];
	int k;

	printf( "group name = %s\n" \
		"free space = "INT64_PRINTF_FORMAT" GB\n" \
		"storage server count = %d\n" \
		"active server count = %d\n" \
		"storage_port = %d\n" \
		"storage_http_port = %d\n" \
		"store path count = %d\n" \
		"subdir count per path= %d\n" \
		"current write server index = %d\n\n", \
		pGroupStat->group_name, \
		pGroupStat->free_mb / 1024, \
		pGroupStat->count, \
		pGroupStat->active_count, \
		pGroupStat->storage_port, \
		pGroupStat->storage_http_port, \
		pGroupStat->store_path_count, \
		pGroupStat->subdir_count_per_path, \
		pGroupStat->current_write_server
	);

	result = tracker_list_servers(pTrackerServer, \
		pGroupStat->group_name, \
		storage_infos, FDFS_MAX_SERVERS_EACH_GROUP, \
		&storage_count);
	if (result != 0)
	{
		return result;
	}

	k = 0;
	pStorageEnd = storage_infos + storage_count;
	for (pStorage=storage_infos; pStorage<pStorageEnd; \
		pStorage++)
	{
		pStorageStat = &(pStorage->stat);

		getHostnameByIp(pStorage->ip_addr, szHostname, sizeof(szHostname));
		if (*szHostname != '\0')
		{
			sprintf(szHostnamePrompt, " (%s)", szHostname);
		}
		else
		{
			*szHostnamePrompt = '\0';
		}

		printf( "\tHost %d:\n" \
			"\t\tip_addr = %s%s  %s\n" \
			"\t\ttotal storage = %dGB\n" \
			"\t\tfree storage = %dGB\n" \
			"\t\tupload priority = %d\n" \
			"\t\ttotal_upload_count = "INT64_PRINTF_FORMAT"\n"   \
			"\t\tsuccess_upload_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\ttotal_set_meta_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\tsuccess_set_meta_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\ttotal_delete_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\tsuccess_delete_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\ttotal_download_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\tsuccess_download_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\ttotal_get_meta_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\tsuccess_get_meta_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\ttotal_create_link_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\tsuccess_create_link_count = "INT64_PRINTF_FORMAT"\n"\
			"\t\ttotal_delete_link_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\tsuccess_delete_link_count = "INT64_PRINTF_FORMAT"\n" \
			"\t\tlast_heart_beat_time = %s\n" \
			"\t\tlast_source_update = %s\n" \
			"\t\tlast_sync_update = %s\n"   \
			"\t\tlast_synced_timestamp= %s\n",  \
			++k, pStorage->ip_addr, szHostnamePrompt, \
			get_storage_status_caption(pStorage->status), \
			pStorage->total_mb / 1024, \
			pStorage->free_mb / 1024,  \
			pStorage->upload_priority,  \
			pStorageStat->total_upload_count, \
			pStorageStat->success_upload_count, \
			pStorageStat->total_set_meta_count, \
			pStorageStat->success_set_meta_count, \
			pStorageStat->total_delete_count, \
			pStorageStat->success_delete_count, \
			pStorageStat->total_download_count, \
			pStorageStat->success_download_count, \
			pStorageStat->total_get_meta_count, \
			pStorageStat->success_get_meta_count, \
			pStorageStat->total_create_link_count, \
			pStorageStat->success_create_link_count, \
			pStorageStat->total_delete_link_count, \
			pStorageStat->success_delete_link_count, \
			formatDatetime(pStorageStat->last_heart_beat_time, \
				"%Y-%m-%d %H:%M:%S", \
				szLastHeartBeatTime, sizeof(szLastHeartBeatTime)), \
			formatDatetime(pStorageStat->last_source_update, \
				"%Y-%m-%d %H:%M:%S", \
				szSrcUpdTime, sizeof(szSrcUpdTime)), \
			formatDatetime(pStorageStat->last_sync_update, \
				"%Y-%m-%d %H:%M:%S", \
				szSyncUpdTime, sizeof(szSyncUpdTime)), \
			formatDatetime(pStorageStat->last_synced_timestamp, \
				"%Y-%m-%d %H:%M:%S", \
				szSyncedTimestamp, sizeof(szSyncedTimestamp))
		);
	}

	return 0;
}

static int list_all_groups(const char *group_name)
{
	int result;
	int group_count;
	FDFSGroupStat group_stats[FDFS_MAX_GROUPS];
	FDFSGroupStat *pGroupStat;
	FDFSGroupStat *pGroupEnd;
	int i;

	result = tracker_list_groups(pTrackerServer, \
		group_stats, FDFS_MAX_GROUPS, \
		&group_count);
	if (result != 0)
	{
		tracker_close_all_connections();
		fdfs_client_destroy();
		return result;
	}

	pGroupEnd = group_stats + group_count;
	if (group_name == NULL)
	{
		printf("group count: %d\n", group_count);
		i = 0;
		for (pGroupStat=group_stats; pGroupStat<pGroupEnd; \
			pGroupStat++)
		{
			printf( "\nGroup %d:\n", ++i);
			list_storages(pGroupStat);
		}
	}
	else
	{
		for (pGroupStat=group_stats; pGroupStat<pGroupEnd; \
			pGroupStat++)
		{
			if (strcmp(pGroupStat->group_name, group_name) == 0)
			{
				list_storages(pGroupStat);
				break;
			}
		}
	}

	return 0;
}

