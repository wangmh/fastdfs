/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_global.h

#ifndef _STORAGE_GLOBAL_H
#define _STORAGE_GLOBAL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fdfs_define.h"
#include "tracker_types.h"
#include "client_global.h"
#include "fdht_types.h"
#include "fdfs_base64.h"

#ifdef WITH_HTTPD
#include "fdfs_http_shared.h"
#endif

#define STORAGE_BEAT_DEF_INTERVAL    30
#define STORAGE_REPORT_DEF_INTERVAL  300
#define STORAGE_DEF_SYNC_WAIT_MSEC   100
#define STORAGE_SYNC_STAT_FILE_FREQ  50000
#define DEFAULT_DATA_DIR_COUNT_PER_PATH	 256

#define STORAGE_MAX_LOCAL_IP_ADDRS	4

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	FDFSStorageBrief server;
	int last_sync_src_timestamp;
} FDFSStorageServer;

extern char **g_store_paths; //file store paths
extern int g_path_count;   //store path count

/* subdirs under store path, g_subdir_count * g_subdir_count 2 level subdirs */
extern int g_subdir_count_per_path;

extern int g_server_port;
extern int g_max_connections;
//extern int g_max_write_thread_count;
extern int g_file_distribute_path_mode;
extern int g_file_distribute_rotate_count;
extern int g_fsync_after_written_bytes;

extern int g_dist_path_index_high; //current write to high path
extern int g_dist_path_index_low;  //current write to low path
extern int g_dist_write_file_count; //current write file count

extern int g_storage_count;  //stoage server count in my group
extern FDFSStorageServer g_storage_servers[FDFS_MAX_SERVERS_EACH_GROUP];
extern FDFSStorageServer *g_sorted_storages[FDFS_MAX_SERVERS_EACH_GROUP];

extern char g_group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
extern int g_tracker_reporter_count;
extern int g_heart_beat_interval;
extern int g_stat_report_interval;
extern int g_sync_wait_usec;
extern int g_sync_interval; //unit: milliseconds
extern TimeInfo g_sync_start_time;
extern TimeInfo g_sync_end_time;
extern bool g_sync_part_time; //true for part time, false for all time of a day
extern int g_sync_log_buff_interval; //sync log buff to disk every interval seconds
extern int g_sync_binlog_buff_interval; //sync binlog buff to disk every interval seconds

extern FDFSStorageStat g_storage_stat;
extern int g_stat_change_count;
extern int g_sync_change_count; //sync src timestamp change counter

extern int g_storage_join_time;
extern bool g_sync_old_done;
extern char g_sync_src_ip_addr[IP_ADDRESS_SIZE];
extern int g_sync_until_timestamp;

extern int g_local_host_ip_count;
extern char g_local_host_ip_addrs[STORAGE_MAX_LOCAL_IP_ADDRS * \
				IP_ADDRESS_SIZE];

extern int g_allow_ip_count;  /* -1 means match any ip address */
extern in_addr_t *g_allow_ip_addrs;  /* sorted array, asc order */

extern bool g_check_file_duplicate;
extern char g_key_namespace[FDHT_MAX_NAMESPACE_LEN+1];
extern int g_namespace_len;
extern struct base64_context g_base64_context;

extern char g_run_by_group[32];
extern char g_run_by_user[32];

#ifdef WITH_HTTPD
extern FDFSHTTPParams g_http_params;
extern int g_http_trunk_size;
#endif

int storage_cmp_by_ip_addr(const void *p1, const void *p2);

void load_local_host_ip_addrs();
bool is_local_host_ip(const char *client_ip);
int insert_into_local_host_ip(const char *client_ip);
void print_local_host_ip_addrs();

#ifdef __cplusplus
}
#endif

#endif
