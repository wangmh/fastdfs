/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_sync.h

#ifndef _STORAGE_SYNC_H_
#define _STORAGE_SYNC_H_

#define STORAGE_OP_TYPE_SOURCE_CREATE_FILE	'C'
#define STORAGE_OP_TYPE_SOURCE_DELETE_FILE	'D'
#define STORAGE_OP_TYPE_SOURCE_UPDATE_FILE	'U'
#define STORAGE_OP_TYPE_REPLICA_CREATE_FILE	'c'
#define STORAGE_OP_TYPE_REPLICA_DELETE_FILE	'd'
#define STORAGE_OP_TYPE_REPLICA_UPDATE_FILE	'u'

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	char ip_addr[FDFS_IPADDR_SIZE];
	bool need_sync_old;
	bool sync_old_done;
	time_t until_timestamp;
	int mark_fd;
	int binlog_index;
	int binlog_fd;
	off_t binlog_offset;
	int64_t scan_row_count;
	int64_t sync_row_count;
	int64_t last_write_row_count;
} BinLogReader;

typedef struct
{
	time_t timestamp;
	char op_type;
	char filename[32];
	int filename_len;
} BinLogRecord;

extern int g_binlog_fd;
extern int g_binlog_index;

extern int g_storage_sync_thread_count;

int storage_sync_init();
int storage_sync_destroy();
int storage_binlog_write(const char op_type, const char *filename);

int storage_sync_thread_start(const FDFSStorageBrief *pStorage);
int kill_storage_sync_threads();

#ifdef __cplusplus
}
#endif

#endif
