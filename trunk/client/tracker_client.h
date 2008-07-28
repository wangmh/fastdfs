/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#ifndef TRACKER_CLIENT_H
#define TRACKER_CLIENT_H

#include "tracker_types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
        char status;
        char ip_addr[FDFS_IPADDR_SIZE];
	int total_mb;  //total disk storage in MB
	int free_mb;  //free disk storage in MB
        FDFSStorageStat stat;
} FDFSStorageInfo;

/**
* close all connections to tracker servers
* params:
* return:
**/
void tracker_close_all_connections();

/**
* get a connection to tracker server
* params:
* return: != NULL for success, NULL for fail
**/
TrackerServerInfo *tracker_get_connection();

/**
* close all connections to tracker servers
* params:
*	pTrackerServer: tracker server
* return:
**/
void tracker_disconnect_server(TrackerServerInfo *pTrackerServer);

/**
* connect to the tracker server
* params:
*	pTrackerServer: tracker server
* return: 0 success, !=0 fail, return the error code
**/
int tracker_connect_server(TrackerServerInfo *pTrackerServer);

/**
* list all groups
* params:
*	pTrackerServer: tracker server
*	group_stats: return group info array
*	max_groups: max group count(group array capacity)
*	group_count: return group count
* return: 0 success, !=0 fail, return the error code
**/
int tracker_list_groups(TrackerServerInfo *pTrackerServer, \
		FDFSGroupStat *group_stats, const int max_groups, \
		int *group_count);

/**
* list all servers of the specified group
* params:
*	pTrackerServer: tracker server
*	szGroupName: group name to query
*	storage_infos: return storage info array
*	max_storages: max storage count(storage array capacity)
*	storage_count: return storage count
* return: 0 success, !=0 fail, return the error code
**/
int tracker_list_servers(TrackerServerInfo *pTrackerServer, \
		const char *szGroupName, \
		FDFSStorageInfo *storage_infos, const int max_storages, \
		int *storage_count);

/**
* query storage server to upload file
* params:
*	pTrackerServer: tracker server
*	pStorageServer: return storage server
* return: 0 success, !=0 fail, return the error code
**/
int tracker_query_storage_store(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer);

/**
* query storage server to download file
* params:
*	pTrackerServer: tracker server
*	pStorageServer: return storage server
*       group_name: the group name of storage server
*       filename: filename on storage server
* return: 0 success, !=0 fail, return the error code
**/
int tracker_query_storage_fetch(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *group_name, const char *filename);

#ifdef __cplusplus
}
#endif

#endif
