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
        char ip_addr[IP_ADDRESS_SIZE];
	int total_mb;  //total disk storage in MB
	int free_mb;  //free disk storage in MB
        FDFSStorageStat stat;
} FDFSStorageInfo;

/**
* get a connection to tracker server
* params:
* return: != NULL for success, NULL for fail
**/
TrackerServerInfo *tracker_get_connection();


/**
* get a connection to tracker server
* params:
*       pTrackerServer: tracker server
* return: 0 success, !=0 fail
**/
int tracker_get_connection_ex(TrackerServerInfo *pTrackerServer);

/**
* connect to the tracker server
* params:
*	pTrackerServer: tracker server
* return: 0 success, !=0 fail, return the error code
**/
int tracker_connect_server(TrackerServerInfo *pTrackerServer);

/**
* close all connections to tracker servers
* params:
*	pTrackerServer: tracker server
* return:
**/
void tracker_disconnect_server(TrackerServerInfo *pTrackerServer);


/**
* connect to all tracker servers
* params:
* return: 0 success, !=0 fail, return the error code
**/
int tracker_get_all_connections();

/**
* close all connections to tracker servers
* params:
* return:
**/
void tracker_close_all_connections();

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
*       store_path_index: return the index of path on the storage server
* return: 0 success, !=0 fail, return the error code
**/
int tracker_query_storage_store(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, int *store_path_index);

/**
* query storage server to update (delete file or set meta data)
* params:
*	pTrackerServer: tracker server
*	pStorageServer: return storage server
*       group_name: the group name of storage server
*       filename: filename on storage server
* return: 0 success, !=0 fail, return the error code
**/
#define tracker_query_storage_update(pTrackerServer, \
		pStorageServer, group_name, filename) \
	tracker_do_query_storage(pTrackerServer, \
		pStorageServer, TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE,\
		group_name, filename)

/**
* query storage server to download file
* params:
*	pTrackerServer: tracker server
*	pStorageServer: return storage server
*       group_name: the group name of storage server
*       filename: filename on storage server
* return: 0 success, !=0 fail, return the error code
**/
#define tracker_query_storage_fetch(pTrackerServer, \
		pStorageServer, group_name, filename) \
	tracker_do_query_storage(pTrackerServer, \
		pStorageServer, TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH,\
		group_name, filename)

/**
* query storage server to fetch or update
* params:
*	pTrackerServer: tracker server
*	pStorageServer: return storage server
*       cmd : command, TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH or 
*             TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE
*       group_name: the group name of storage server
*       filename: filename on storage server
* return: 0 success, !=0 fail, return the error code
**/
int tracker_do_query_storage(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, const byte cmd, \
		const char *group_name, const char *filename);

/**
* delete a storage server from cluster
* params:
*	pTrackerServer: tracker server
*	group_name: the group name which the storage server belongs to
*	ip_addr: the ip address of the storage server
* return: 0 success, !=0 fail, return the error code
**/
int tracker_delete_storage(TrackerServerInfo *pTrackerServer, \
		const char *group_name, const char *ip_addr);

#ifdef __cplusplus
}
#endif

#endif
