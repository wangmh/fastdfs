/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_nio.h

#ifndef _TRACKER_NIO_H
#define _TRACKER_NIO_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <event.h>
#include "tracker_types.h"
#include "fast_task_queue.h"
#include "storage_global.h"
#include "fdht_types.h"


#define FDFS_STORAGE_STAGE_NIO_INIT   '\0'
#define FDFS_STORAGE_STAGE_NIO_RECV   'r'
#define FDFS_STORAGE_STAGE_NIO_SEND   's'

#define FDFS_STORAGE_FILE_OP_READ     'r'
#define FDFS_STORAGE_FILE_OP_WRITE    'w'

typedef struct
{
	char filename[MAX_PATH_SIZE + 128];
	char op;        //w for writing, r for reading
	int fd;         //file description no
	int64_t start;  //file start offset
	int64_t end;    //file end offset
	int64_t offset; //file current offset
} StorageFileContext;

typedef struct
{
	int nio_thread_index;
	int dio_thread_index;
	int sock;
	char stage;  //nio stage
	char sync_flag;
	char ip_addr[IP_ADDRESS_SIZE];  //to be removed
	char tracker_client_ip[IP_ADDRESS_SIZE];

	StorageFileContext file_context;

	int64_t total_length;   //pkg total length
	int64_t total_offset;   //pkg current offset

	int src_sync_timestamp;
	FDFSStorageServer *pSrcStorage;
} StorageClientInfo;

struct storage_nio_thread_data
{
        struct event_base *ev_base;
        int pipe_fds[2];
        int dealing_file_count;
	GroupArray group_array;
};

#ifdef __cplusplus
extern "C" {
#endif

void recv_notify_read(int sock, short event, void *arg);
int send_add_event(struct fast_task_info *pTask);

void task_finish_clean_up(struct fast_task_info *pTask);

#ifdef __cplusplus
}
#endif

#endif

