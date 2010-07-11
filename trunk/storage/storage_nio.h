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
#include "fast_task_queue.h"


#define FDFS_STORAGE_STAGE_NIO_RECV    11
#define FDFS_STORAGE_STAGE_NIO_SEND    12
#define FDFS_STORAGE_STAGE_DIO_READ    21
#define FDFS_STORAGE_STAGE_DIO_WRITE   22

#define FDFS_STORAGE_FILE_OP_WRITE    'w'
#define FDFS_STORAGE_FILE_OP_READ     'r'

typedef struct
{
	int thread_index;
	int stage;
	int sock;
	char ip_addr[IP_ADDRESS_SIZE];  //to be removed
	char tracker_client_ip[IP_ADDRESS_SIZE];

	int64_t total_length;   //pkg total length
	int64_t total_offset;   //pkg current offset

	char sync_flag;
	char file_op;     //w for writing, r for reading
	int fd;           //file description no
	int64_t file_size;   //file size
	int64_t file_offset; //file offset

	int src_sync_timestamp;
	FDFSStorageServer *pSrcStorage;
} StorageClientInfo;

struct storage_thread_data
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

