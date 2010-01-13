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
#include "fdfs_define.h"
#include "tracker_types.h"
#include "fdfs_base64.h"

#ifdef WITH_HTTPD
#include "fdfs_http_shared.h"
#endif

#define TRACKER_SYNC_TO_FILE_FREQ 1000

#ifdef __cplusplus
extern "C" {
#endif

extern int g_server_port;
extern FDFSGroups g_groups;
extern int g_storage_stat_chg_count;
extern int g_storage_sync_time_chg_count; //sync timestamp
extern int g_max_connections;
extern int g_storage_reserved_mb;
extern int g_sync_log_buff_interval; //sync log buff to disk every interval seconds
extern int g_check_active_interval; //check storage server alive every interval seconds

extern int g_allow_ip_count;  /* -1 means match any ip address */
extern in_addr_t *g_allow_ip_addrs;  /* sorted array, asc order */
extern struct base64_context g_base64_context;

extern char g_run_by_group[32];
extern char g_run_by_user[32];

extern int g_thread_stack_size;

#ifdef WITH_HTTPD
extern FDFSHTTPParams g_http_params;
#endif

#if defined(DEBUG_FLAG) && defined(OS_LINUX)
extern char g_exe_name[256];
#endif

#ifdef __cplusplus
}
#endif

#endif
