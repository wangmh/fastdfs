/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_service.h

#ifndef _STORAGE_SERVICE_H_
#define _STORAGE_SERVICE_H_

#ifdef __cplusplus
extern "C" {
#endif

extern int g_storage_thread_count;
extern pthread_mutex_t g_storage_thread_lock;

void* storage_thread_entrance(void* arg);
int storage_service_init();
void storage_service_destroy();

#ifdef __cplusplus
}
#endif

#endif
