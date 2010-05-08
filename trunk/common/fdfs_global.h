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

#include "common_define.h"
#include "fdfs_define.h"

#define FDFS_FILE_EXT_NAME_MAX_LEN	6


#ifdef __cplusplus
extern "C" {
#endif

extern int g_fdfs_network_timeout;
extern char g_fdfs_base_path[MAX_PATH_SIZE];
extern Version g_fdfs_version;

int fdfs_check_data_filename(const char *filename, const int len);
int fdfs_gen_slave_filename(const char *master_filename, \
		const char *prefix_name, const char *ext_name, \
		char *filename, int *filename_len);

#ifdef __cplusplus
}
#endif

#endif

