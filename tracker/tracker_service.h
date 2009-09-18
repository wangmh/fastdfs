/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_service.h

#ifndef _TRACKER_SERVICE_H_
#define _TRACKER_SERVICE_H_

#ifdef __cplusplus
extern "C" {
#endif

extern int g_tracker_thread_count;

int tracker_service_init();
int tracker_service_destroy();

void* tracker_thread_entrance(void* arg);

#ifdef __cplusplus
}
#endif

#endif
