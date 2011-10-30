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
#include "storage_func.h"
#include "fast_task_queue.h"
#include "storage_global.h"
#include "fdht_types.h"
#include "trunk_mem.h"

#define FDFS_STORAGE_STAGE_NIO_INIT   '\0'
#define FDFS_STORAGE_STAGE_NIO_RECV   'r'
#define FDFS_STORAGE_STAGE_NIO_SEND   's'

#define FDFS_STORAGE_FILE_OP_READ     'R'
#define FDFS_STORAGE_FILE_OP_WRITE    'W'
#define FDFS_STORAGE_FILE_OP_APPEND   'A'
#define FDFS_STORAGE_FILE_OP_DELETE   'D'
#define FDFS_STORAGE_FILE_OP_DISCARD  'd'

typedef int (*TaskDealFunc)(struct fast_task_info *pTask);

/* this clean func will be called when connection disconnected */
typedef void (*DisconnectCleanFunc)(struct fast_task_info *pTask);

typedef void (*DeleteFileLogCallback)(struct fast_task_info *pTask, \
		const int err_no);

typedef void (*FileDealDoneCallback)(struct fast_task_info *pTask, \
		const int err_no);

typedef int (*FileBeforeOpenCallback)(struct fast_task_info *pTask);
typedef int (*FileBeforeCloseCallback)(struct fast_task_info *pTask);

typedef struct
{
	bool if_gen_filename;	//if upload generate filename
	bool if_appender_file;  //if upload appender file
	bool if_trunk_file;     //if trunk file, since V3.0
	bool if_sub_path_alloced; //if sub path alloced since V3.0
	char file_type;           //regular or link file
	char master_filename[128];
	char file_ext_name[FDFS_FILE_EXT_NAME_MAX_LEN + 1];
	char formatted_ext_name[FDFS_FILE_EXT_NAME_MAX_LEN + 2];
	char prefix_name[FDFS_FILE_PREFIX_MAX_LEN + 1];
	int start_time;		//upload start timestamp
	FDFSTrunkFullInfo trunk_info;
	FileBeforeOpenCallback before_open_callback;
	FileBeforeCloseCallback before_close_callback;
} StorageUploadInfo;

typedef struct
{
	char op_flag;
	char *meta_buff;
	int meta_bytes;
} StorageSetMetaInfo;

typedef struct
{
	char filename[MAX_PATH_SIZE + 128];  	//full filename
	char fname2log[128+sizeof(FDFS_STORAGE_META_FILE_EXT)];  //filename to log
	char op;            //w for writing, r for reading, d for deleting etc.
	char sync_flag;     //sync flag log to binlog
	bool calc_crc32;    //if calculate file content hash code
	bool calc_file_hash;      //if calculate file content hash code
	int open_flags;           //open file flags
	int file_hash_codes[4];  //file hash code
	int crc32;   //file content crc32 signature

	union
	{
		StorageUploadInfo upload;
		StorageSetMetaInfo setmeta;
	} extra_info;

	int dio_thread_index;		//dio thread index
	int timestamp2log;		//timestamp to log
	int delete_flag;     //delete file flag
	int create_flag;    //create file flag
	int buff_offset;    //buffer offset after recv to write to file
	int fd;         //file description no
	int64_t start;  //file start offset
	int64_t end;    //file end offset
	int64_t offset; //file current offset
	FileDealDoneCallback done_callback;
	DeleteFileLogCallback log_callback;
} StorageFileContext;

typedef struct
{
	int nio_thread_index;  //nio thread index
	int sock;
	char stage;  //nio stage, send or recv
	char tracker_client_ip[IP_ADDRESS_SIZE];

	StorageFileContext file_context;

	int64_t total_length;   //pkg total length
	int64_t total_offset;   //pkg current offset

	FDFSStorageServer *pSrcStorage;
	TaskDealFunc deal_func;  //function pointer to deal this task
	void *extra_arg;   //store extra arg, such as (BinLogReader *)
	DisconnectCleanFunc clean_func;  //clean function pointer when finished
} StorageClientInfo;

struct storage_nio_thread_data
{
        struct event_base *ev_base;  //libevent base pointer
        int pipe_fds[2];   //for notify nio thread to deal task
	GroupArray group_array;  //FastDHT group array
};

#ifdef __cplusplus
extern "C" {
#endif

void storage_recv_notify_read(int sock, short event, void *arg);
int storage_send_add_event(struct fast_task_info *pTask);

void task_finish_clean_up(struct fast_task_info *pTask);

#ifdef __cplusplus
}
#endif

#endif

