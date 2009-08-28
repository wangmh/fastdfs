/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_mem.h

#ifndef _TRACKER_MEM_H_
#define _TRACKER_MEM_H_

#include <pthread.h>
#include "tracker_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int tracker_mem_init();
int tracker_mem_destroy();

int tracker_mem_init_pthread_lock(pthread_mutex_t *pthread_lock);
int tracker_mem_pthread_lock();
int tracker_mem_pthread_unlock();

FDFSGroupInfo *tracker_mem_get_group(const char *group_name);
FDFSStorageDetail *tracker_mem_get_storage(FDFSGroupInfo *pGroup, \
				const char *ip_addr);
FDFSStorageDetail *tracker_mem_get_active_storage(FDFSGroupInfo *pGroup, \
				const char *ip_addr);

int tracker_mem_add_group(TrackerClientInfo *pClientInfo, \
			const bool bIncRef, bool *bInserted);
int tracker_mem_add_storage(TrackerClientInfo *pClientInfo, \
			const bool bIncRef, bool *bInserted);
int tracker_mem_delete_storage(FDFSGroupInfo *pGroup, const char *ip_addr);
int tracker_mem_add_group_and_storage(TrackerClientInfo *pClientInfo, \
		const int store_path_count, const int subdir_count_per_path, \
		const bool bIncRef, const bool init_flag);
int tracker_mem_offline_store_server(TrackerClientInfo *pClientInfo);
int tracker_mem_active_store_server(FDFSGroupInfo *pGroup, \
			FDFSStorageDetail *pTargetServer);

int tracker_mem_sync_storages(TrackerClientInfo *pClientInfo, \
                FDFSStorageBrief *briefServers, const int server_count);
int tracker_save_storages();
int tracker_save_sync_timestamps();

int tracker_get_group_file_count(FDFSGroupInfo *pGroup);
int tracker_get_group_success_upload_count(FDFSGroupInfo *pGroup);
FDFSStorageDetail *tracker_get_group_sync_src_server(FDFSGroupInfo *pGroup, \
			FDFSStorageDetail *pDestServer);

extern int pthread_mutexattr_settype(pthread_mutexattr_t *attr, int kind);

FDFSStorageDetail *tracker_get_writable_storage(FDFSGroupInfo *pStoreGroup);

int tracker_mem_get_storage_by_filename(const byte cmd, const char *group_name,\
	const char *filename, const int filename_len, FDFSGroupInfo **ppGroup, \
	FDFSStorageDetail **ppStoreServers, int *server_count);

int tracker_mem_check_alive(void *arg);

#ifdef __cplusplus
}
#endif

#endif
