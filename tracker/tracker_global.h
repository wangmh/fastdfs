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

extern int g_allow_ip_count;  /* -1 means match any ip address */
extern in_addr_t *g_allow_ip_addrs;  /* sorted array, asc order */

#ifdef __cplusplus
}
#endif

#endif
