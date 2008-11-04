/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_types.h

#ifndef _TRACKER_TYPES_H_
#define _TRACKER_TYPES_H_

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include "fdfs_define.h"

#define FDFS_ONE_MB	(1024 * 1024)

#define FDFS_GROUP_NAME_MAX_LEN		16
#define FDFS_MAX_SERVERS_EACH_GROUP	32
#define FDFS_MAX_GROUPS			64
#define FDFS_MAX_TRACKERS		16

#define FDFS_MAX_META_NAME_LEN		64
#define FDFS_MAX_META_VALUE_LEN		256

#define FDFS_FILE_EXT_NAME_MAX_LEN	5

//status order is important!
#define FDFS_STORAGE_STATUS_INIT	  0
#define FDFS_STORAGE_STATUS_WAIT_SYNC	  1
#define FDFS_STORAGE_STATUS_SYNCING	  2
#define FDFS_STORAGE_STATUS_DELETED	  3
#define FDFS_STORAGE_STATUS_OFFLINE	  4
#define FDFS_STORAGE_STATUS_ONLINE	  5
#define FDFS_STORAGE_STATUS_ACTIVE	  6
#define FDFS_STORAGE_STATUS_NONE	 99

//which group to upload file
#define FDFS_STORE_LOOKUP_ROUND_ROBIN	0  //round robin
#define FDFS_STORE_LOOKUP_SPEC_GROUP	1  //specify group
#define FDFS_STORE_LOOKUP_LOAD_BALANCE	2  //load balance

//which server to upload file
#define FDFS_STORE_SERVER_ROUND_ROBIN	0  //round robin
#define FDFS_STORE_SERVER_FIRST		1  //the first server

typedef struct
{
	char status;
	char ip_addr[IP_ADDRESS_SIZE];
} FDFSStorageBrief;

typedef struct
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	int64_t free_mb;  //free disk storage in MB
	int count;        //server count
	int storage_port; //storage server port
	int active_count; //active server count
	int current_write_server; //current server index to upload file
} FDFSGroupStat;

typedef struct
{
	/* following count stat by source server,
           not including synced count
	*/
	int64_t total_upload_count;
	int64_t success_upload_count;
	int64_t total_set_meta_count;
	int64_t success_set_meta_count;
	int64_t total_delete_count;
	int64_t success_delete_count;
	int64_t total_download_count;
	int64_t success_download_count;
	int64_t total_get_meta_count;
	int64_t success_get_meta_count;

	/* last update timestamp as source server, 
           current server' timestamp
	*/
	time_t last_source_update;

	/* last update timestamp as dest server, 
           current server' timestamp
	*/
	time_t last_sync_update;

	/* last syned timestamp, 
	   source server's timestamp
	*/
	time_t last_synced_timestamp;

} FDFSStorageStat;

typedef struct
{
	char sz_total_upload_count[8];
	char sz_success_upload_count[8];
	char sz_total_set_meta_count[8];
	char sz_success_set_meta_count[8];
	char sz_total_delete_count[8];
	char sz_success_delete_count[8];
	char sz_total_download_count[8];
	char sz_success_download_count[8];
	char sz_total_get_meta_count[8];
	char sz_success_get_meta_count[8];
	char sz_last_source_update[8];
	char sz_last_sync_update[8];
	char sz_last_synced_timestamp[8];
} FDFSStorageStatBuff;

typedef struct StructFDFSStorageDetail
{
	char status;
	bool dirty;
	char ip_addr[IP_ADDRESS_SIZE];

	struct StructFDFSStorageDetail *psync_src_server;
	time_t sync_until_timestamp;

	int64_t total_mb;  //total disk storage in MB
	int64_t free_mb;  //free disk storage in MB

	int *ref_count;   //group/storage servers referer count
	int version;      //current server version
	FDFSStorageStat stat;
} FDFSStorageDetail;

typedef struct
{
	bool dirty;
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	int64_t free_mb;  //free disk storage in MB
	int alloc_size;
	int count;    //server count
	int storage_port;
	FDFSStorageDetail *all_servers;
	FDFSStorageDetail **sorted_servers;  //order by addr
	int active_count;
	FDFSStorageDetail **active_servers;  //order by addr
	int current_read_server;
	int current_write_server;
	int **last_sync_timestamps;//row for src storage, col for dest storage
	int *ref_count;  //groups referer count
	int version;     //current group version
	time_t last_source_update;
	time_t last_sync_update;
} FDFSGroupInfo;

typedef struct
{
	int alloc_size;
	int count;  //group count
	FDFSGroupInfo *groups;
	FDFSGroupInfo **sorted_groups; //order by group_name
	FDFSGroupInfo *pStoreGroup;  //the group to store uploaded files
	int current_write_group;  //current group index to upload file
	byte store_lookup;  //store to which group
	byte store_server;  //store to which server
	char store_group[FDFS_GROUP_NAME_MAX_LEN + 1];
} FDFSGroups;

typedef struct
{
	int sock;
	int port;
	char ip_addr[IP_ADDRESS_SIZE];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
} TrackerServerInfo;

typedef struct
{
	int sock;
	int storage_port;
	char ip_addr[IP_ADDRESS_SIZE];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	FDFSGroupInfo *pGroup;
	FDFSStorageDetail *pStorage;
	FDFSGroupInfo *pAllocedGroups;		//for free
	FDFSStorageDetail *pAllocedStorages;	//for free
} TrackerClientInfo;

typedef struct
{
	int sock;
	char ip_addr[IP_ADDRESS_SIZE];
} StorageClientInfo;

typedef struct
{
	char name[FDFS_MAX_META_NAME_LEN + 1];  //key
	char value[FDFS_MAX_META_VALUE_LEN + 1]; //value
} FDFSMetaData;

typedef struct
{
	FDFSGroupInfo *pGroup;
	FDFSStorageDetail *pStorage;
	char sync_src_ip_addr[IP_ADDRESS_SIZE];
} FDFSStorageSync;

#endif

