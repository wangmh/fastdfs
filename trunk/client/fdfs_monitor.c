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
#include "fdfs_client.h"

int main(int argc, char *argv[])
{
	char *conf_filename;
	TrackerServerInfo *pTrackerServer;
	int result;
	int group_count;
	int storage_count;
	FDFSGroupStat group_stats[FDFS_MAX_GROUPS];
	FDFSGroupStat *pGroupStat;
	FDFSGroupStat *pGroupEnd;
	FDFSStorageInfo storage_infos[FDFS_MAX_SERVERS_EACH_GROUP];
	FDFSStorageInfo *pStorage;
	FDFSStorageInfo *pStorageEnd;
	FDFSStorageStat *pStorageStat;
	char szSrcUpdTime[32];
	char szSyncUpdTime[32];
	int i, k;

	if (argc < 2)
	{
		printf("Usage: %s <config_file>\n", argv[0]);
		return 1;
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

	result = tracker_list_groups(pTrackerServer, \
		group_stats, FDFS_MAX_GROUPS, \
		&group_count);
	if (result != 0)
	{
		tracker_close_all_connections();
		fdfs_client_destroy();
		return result;
	}

	printf("group count: %d\n", group_count);
	i = 0;
	pGroupEnd = group_stats + group_count;
	for (pGroupStat=group_stats; pGroupStat<pGroupEnd; \
		pGroupStat++)
	{
		printf( "\nGroup %d:\n" \
			"group name = %s\n" \
			"free space = %d GB\n" \
			"storage server count = %d\n" \
			"active server count = %d\n" \
			"storage_port = %d\n" \
			"current write server index = %d\n\n", \
			++i, \
			pGroupStat->group_name, \
			pGroupStat->free_mb / 1024, \
			pGroupStat->count, \
			pGroupStat->active_count, \
			pGroupStat->storage_port, \
			pGroupStat->current_write_server
		);

		result = tracker_list_servers(pTrackerServer, \
			pGroupStat->group_name, \
			storage_infos, FDFS_MAX_SERVERS_EACH_GROUP, \
			&storage_count);
		if (result != 0)
		{
			continue;
		}

		k = 0;
		pStorageEnd = storage_infos + storage_count;
		for (pStorage=storage_infos; pStorage<pStorageEnd; \
			pStorage++)
		{
			pStorageStat = &(pStorage->stat);

			printf( "\tHost %d:\n" \
				"\t\tip_addr = %s  %s\n" \
				"\t\ttotal storage = %dGB\n" \
				"\t\tfree storage = %dGB\n" \
				"\t\ttotal_upload_count = %d\n"   \
				"\t\tsuccess_upload_count = %d\n" \
				"\t\ttotal_set_meta_count = %d\n" \
				"\t\tsuccess_set_meta_count = %d\n" \
				"\t\ttotal_delete_count = %d\n" \
				"\t\tsuccess_delete_count = %d\n" \
				"\t\ttotal_download_count = %d\n" \
				"\t\tsuccess_download_count = %d\n" \
				"\t\ttotal_get_meta_count = %d\n" \
				"\t\tsuccess_get_meta_count = %d\n" \
				"\t\tlast_source_update = %s\n" \
				"\t\tlast_sync_update = %s\n",  \
				++k, pStorage->ip_addr, \
				get_storage_status_caption(pStorage->status), \
				pStorage->total_mb / 1024, \
				pStorage->free_mb / 1024,  \
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
				formatDatetime(pStorageStat->last_source_update, \
					"%Y-%m-%d %H:%M:%S", \
					szSrcUpdTime, sizeof(szSrcUpdTime)), \
				formatDatetime(pStorageStat->last_sync_update, \
					"%Y-%m-%d %H:%M:%S", \
					szSyncUpdTime, sizeof(szSyncUpdTime))
			);
		}
	}

	if (tracker_quit(pTrackerServer) != 0)
	{
	}

	tracker_close_all_connections();
	fdfs_client_destroy();
	return 0;
}

