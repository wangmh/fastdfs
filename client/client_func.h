/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//client_func.h

#include "tracker_types.h"
#include "client_global.h"

#ifndef _CLIENT_FUNC_H_
#define _CLIENT_FUNC_H_

#ifdef __cplusplus
extern "C" {
#endif

#define fdfs_client_init(filename) \
	fdfs_client_init_ex((&g_tracker_group), filename)

#define fdfs_client_destroy() \
	fdfs_client_destroy_ex((&g_tracker_group))

/**
* client initial function
* params:
*       pTrackerGroup: tracker group
*	conf_filename: client config filename
* return: 0 success, !=0 fail, return the error code
**/
int fdfs_client_init_ex(TrackerServerGroup *pTrackerGroup, \
		const char *conf_filename);


/**
* load tracker server group
* params:
*       pTrackerGroup: tracker group
*	conf_filename: tracker server group config filename
* return: 0 success, !=0 fail, return the error code
**/
int fdfs_load_tracker_group(TrackerServerGroup *pTrackerGroup, \
		const char *conf_filename);

/**
* load tracker server group
* params:
*       pTrackerGroup: tracker group
*	conf_filename: config filename
*       items: ini file items
*       nItemCount: ini file item count
* return: 0 success, !=0 fail, return the error code
**/
int fdfs_load_tracker_group_ex(TrackerServerGroup *pTrackerGroup, \
		const char *conf_filename, \
		IniItemInfo *items, const int nItemCount);

/**
* client destroy function
* params:
*       pTrackerGroup: tracker group
* return: 
**/
void fdfs_client_destroy_ex(TrackerServerGroup *pTrackerGroup);

#ifdef __cplusplus
}
#endif

#endif
