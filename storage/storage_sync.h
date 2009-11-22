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
#define STORAGE_OP_TYPE_SOURCE_CREATE_LINK	'L'
#define STORAGE_OP_TYPE_REPLICA_CREATE_FILE	'c'
#define STORAGE_OP_TYPE_REPLICA_DELETE_FILE	'd'
#define STORAGE_OP_TYPE_REPLICA_UPDATE_FILE	'u'
#define STORAGE_OP_TYPE_REPLICA_CREATE_LINK	'l'
#define STORAGE_BINLOG_BUFFER_SIZE		64 * 1024

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	char buffer[STORAGE_BINLOG_BUFFER_SIZE];
	char *current;
	int length;
	int version;
} BinLogBuffer;

typedef struct
{
	char ip_addr[IP_ADDRESS_SIZE];
	bool need_sync_old;
	bool sync_old_done;
	BinLogBuffer binlog_buff;
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
	char filename[64];  //filename with path index prefix which should be trimed
	char true_filename[64]; //pure filename
	int filename_len;
	int true_filename_len;
	char *pBasePath;
} BinLogRecord;

extern int g_binlog_fd;
extern int g_binlog_index;

extern int g_storage_sync_thread_count;

int storage_sync_init();
int storage_sync_destroy();
int storage_binlog_write(const int timestamp, const char op_type, \
		const char *filename);

int storage_sync_thread_start(const FDFSStorageBrief *pStorage);
int kill_storage_sync_threads();
int fdfs_binlog_sync_func(void *args);

#ifdef __cplusplus
}
#endif

#endif
