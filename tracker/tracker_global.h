/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_global.h

#ifndef _TRACKER_GLOBAL_H
#define _TRACKER_GLOBAL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "common_define.h"
#include "fdfs_define.h"
#include "tracker_types.h"
#include "tracker_status.h"
#include "base64.h"

#ifdef WITH_HTTPD
#include "fdfs_http_shared.h"
#endif

#define TRACKER_SYNC_TO_FILE_FREQ		1000
#define TRACKER_MAX_PACKAGE_SIZE		(8 * 1024)
#define TRACKER_SYNC_STATUS_FILE_INTERVAL	3600   //one hour

#ifdef __cplusplus
extern "C" {
#endif

extern bool g_continue_flag;
extern int g_server_port;
extern FDFSGroups g_groups;
extern int g_storage_stat_chg_count;
extern int g_storage_sync_time_chg_count; //sync timestamp
extern int g_max_connections;
extern int g_work_threads;
extern int g_storage_reserved_mb;
extern int g_sync_log_buff_interval; //sync log buff to disk every interval seconds
extern int g_check_active_interval; //check storage server alive every interval seconds

extern struct timeval g_network_tv;

extern int g_allow_ip_count;  /* -1 means match any ip address */
extern in_addr_t *g_allow_ip_addrs;  /* sorted array, asc order */
extern struct base64_context g_base64_context;

gid_t g_run_by_gid;
uid_t g_run_by_uid;

extern char g_run_by_group[32];
extern char g_run_by_user[32];

extern bool g_storage_ip_changed_auto_adjust;

extern int g_thread_stack_size;
extern int g_storage_sync_file_max_delay;
extern int g_storage_sync_file_max_time;

extern time_t g_up_time;
extern TrackerStatus g_tracker_last_status;  //the status of last running

#ifdef WITH_HTTPD
extern FDFSHTTPParams g_http_params;
extern int g_http_check_interval;
extern int g_http_check_type;
extern char g_http_check_uri[128];
extern bool g_http_servers_dirty;
#endif

#if defined(DEBUG_FLAG) && defined(OS_LINUX)
extern char g_exe_name[256];
#endif

extern struct tracker_thread_data *g_thread_data;

#ifdef __cplusplus
}
#endif

#endif
