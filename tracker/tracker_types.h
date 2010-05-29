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

#define FDFS_MAX_META_NAME_LEN		 64
#define FDFS_MAX_META_VALUE_LEN		256

#define FDFS_FILE_PREFIX_MAX_LEN	16
#define FDFS_FILE_PATH_LEN		10
#define FDFS_FILENAME_BASE64_LENGTH     27

#define FDFS_VERSION_SIZE		6

//status order is important!
#define FDFS_STORAGE_STATUS_INIT	  0
#define FDFS_STORAGE_STATUS_WAIT_SYNC	  1
#define FDFS_STORAGE_STATUS_SYNCING	  2
#define FDFS_STORAGE_STATUS_IP_CHANGED    3
#define FDFS_STORAGE_STATUS_DELETED	  4
#define FDFS_STORAGE_STATUS_OFFLINE	  5
#define FDFS_STORAGE_STATUS_ONLINE	  6
#define FDFS_STORAGE_STATUS_ACTIVE	  7
#define FDFS_STORAGE_STATUS_NONE	 99

//which group to upload file
#define FDFS_STORE_LOOKUP_ROUND_ROBIN	0  //round robin
#define FDFS_STORE_LOOKUP_SPEC_GROUP	1  //specify group
#define FDFS_STORE_LOOKUP_LOAD_BALANCE	2  //load balance

//which server to upload file
#define FDFS_STORE_SERVER_ROUND_ROBIN	0  //round robin
#define FDFS_STORE_SERVER_FIRST_BY_IP	1  //the first server order by ip
#define FDFS_STORE_SERVER_FIRST_BY_PRI	2  //the first server order by priority

//which server to download file
#define FDFS_DOWNLOAD_SERVER_ROUND_ROBIN	0  //round robin
#define FDFS_DOWNLOAD_SERVER_SOURCE_FIRST	1  //the source server

//which path to upload file
#define FDFS_STORE_PATH_ROUND_ROBIN	0  //round robin
#define FDFS_STORE_PATH_LOAD_BALANCE	2  //load balance

//the mode of the files distributed to the data path
#define FDFS_FILE_DIST_PATH_ROUND_ROBIN	0  //round robin
#define FDFS_FILE_DIST_PATH_RANDOM	1  //random

//http check alive type
#define FDFS_HTTP_CHECK_ALIVE_TYPE_TCP  0  //tcp
#define FDFS_HTTP_CHECK_ALIVE_TYPE_HTTP 1  //http

#define FDFS_DOWNLOAD_TYPE_TCP	0  //tcp
#define FDFS_DOWNLOAD_TYPE_HTTP	1  //http

#define FDFS_FILE_DIST_DEFAULT_ROTATE_COUNT   100

#define FDFS_DOMAIN_NAME_MAX_SIZE	128

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
	int storage_http_port; //storage server http port
	int active_count; //active server count
	int current_write_server; //current server index to upload file
	int store_path_count;  //store base path count of each storage server
	int subdir_count_per_path;
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
	int64_t total_create_link_count;
	int64_t success_create_link_count;
	int64_t total_delete_link_count;
	int64_t success_delete_link_count;

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

	/* last heart beat time */
	time_t last_heart_beat_time;
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
	char sz_total_create_link_count[8];
	char sz_success_create_link_count[8];
	char sz_total_delete_link_count[8];
	char sz_success_delete_link_count[8];
	char sz_last_source_update[8];
	char sz_last_sync_update[8];
	char sz_last_synced_timestamp[8];
	char sz_last_heart_beat_time[8];
} FDFSStorageStatBuff;

typedef struct StructFDFSStorageDetail
{
	char status;
	bool dirty;
	char ip_addr[IP_ADDRESS_SIZE];
	char domain_name[FDFS_DOMAIN_NAME_MAX_SIZE];
	char version[FDFS_VERSION_SIZE];

	struct StructFDFSStorageDetail *psync_src_server;
	time_t sync_until_timestamp;

	time_t up_time;
	int64_t total_mb;  //total disk storage in MB
	int64_t free_mb;  //free disk storage in MB
	int64_t changelog_offset;  //changelog file offset

	int64_t *path_total_mbs; //total disk storage in MB
	int64_t *path_free_mbs;  //free disk storage in MB

	int store_path_count;  //store base path count of each storage server
	int subdir_count_per_path;
	int upload_priority; //storage upload priority

	int current_write_path; //current write path index

	int *ref_count;   //group/storage servers referer count
	int chg_count;    //current server changed counter
	FDFSStorageStat stat;

#ifdef WITH_HTTPD
	int http_check_last_errno;
	int http_check_last_status;
	int http_check_fail_count;
	char http_check_error_info[256];
#endif
} FDFSStorageDetail;

typedef struct
{
	bool dirty;
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	int64_t free_mb;  //free disk storage in MB
	int alloc_size;
	int count;    //total server count
	int active_count; //active server count
	int storage_port;
	int storage_http_port; //storage http server port
	FDFSStorageDetail *all_servers;
	FDFSStorageDetail **sorted_servers;  //order by ip addr
	FDFSStorageDetail **active_servers;  //order by ip addr
	FDFSStorageDetail *pStoreServer;  //for upload priority mode

#ifdef WITH_HTTPD
	FDFSStorageDetail **http_servers;  //order by ip addr
	int http_server_count; //http server count
	int current_http_server;
#endif

	int current_read_server;
	int current_write_server;

	int store_path_count;  //store base path count of each storage server

	/* subdir_count * subdir_count directories will be auto created
	   under each store_path (disk) of the storage servers
	*/
	int subdir_count_per_path;

	int **last_sync_timestamps;//row for src storage, col for dest storage

	int *ref_count;  //groups referer count
	int chg_count;   //current group changed count 
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
	byte download_server; //download from which server
	byte store_path;  //store to which path
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
	char tracker_client_ip[IP_ADDRESS_SIZE];
} StorageClientInfo;

typedef struct
{
	char name[FDFS_MAX_META_NAME_LEN + 1];  //key
	char value[FDFS_MAX_META_VALUE_LEN + 1]; //value
} FDFSMetaData;

typedef struct
{
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char ip_addr[IP_ADDRESS_SIZE];
	char sync_src_ip_addr[IP_ADDRESS_SIZE];
} FDFSStorageSync;

typedef struct
{
	int storage_port;
	int storage_http_port;
	int store_path_count;
	int subdir_count_per_path;
	int upload_priority;
	int up_time;   //storage service started timestamp
        char version[FDFS_VERSION_SIZE];   //storage version
        char domain_name[FDFS_DOMAIN_NAME_MAX_SIZE];
        char init_flag;
	signed char status;
} FDFSStorageJoinBody;

#endif

