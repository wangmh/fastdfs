/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdfs_global.h

#ifndef _FDFS_GLOBAL_H
#define _FDFS_GLOBAL_H

#include "fdfs_define.h"

typedef struct
{
	char major;
	char minor;
} FDFSVersion;

#ifdef __cplusplus
extern "C" {
#endif

extern bool g_continue_flag;
extern int g_network_timeout;
extern char g_base_path[MAX_PATH_SIZE];
extern FDFSVersion g_version;

#ifdef __cplusplus
}
#endif

#endif

