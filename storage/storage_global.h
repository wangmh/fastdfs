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

#define STORAGE_BEAT_DEF_INTERVAL    30
#define STORAGE_REPORT_DEF_INTERVAL  300
#define STORAGE_DEF_SYNC_WAIT_MSEC   100
#define STORAGE_SYNC_STAT_FILE_FREQ  1000

#define STORAGE_MAX_LOCAL_IP_ADDRS	4

#ifdef __cplusplus
extern "C" {
#endif

extern int g_server_port;
extern int g_max_connections;

extern int g_storage_count;
extern FDFSStorageBrief g_storage_servers[FDFS_MAX_SERVERS_EACH_GROUP];
extern FDFSStorageBrief *g_sorted_storages[FDFS_MAX_SERVERS_EACH_GROUP];

extern char g_group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
extern int g_tracker_reporter_count;
extern int g_heart_beat_interval;
extern int g_stat_report_interval;
extern int g_sync_wait_usec;
extern FDFSStorageStat g_storage_stat;
extern int g_stat_change_count;

extern int g_storage_join_time;
extern bool g_sync_old_done;
extern char g_sync_src_ip_addr[FDFS_IPADDR_SIZE];
extern int g_sync_until_timestamp;

extern int g_tracker_server_count;
extern TrackerServerInfo *g_tracker_servers;

extern int g_local_host_ip_count;
extern char g_local_host_ip_addrs[STORAGE_MAX_LOCAL_IP_ADDRS * \
				FDFS_IPADDR_SIZE];

void load_local_host_ip_addrs();
bool is_local_host_ip(const char *client_ip);
int insert_into_local_host_ip(const char *client_ip);
void print_local_host_ip_addrs();

#ifdef __cplusplus
}
#endif

#endif
