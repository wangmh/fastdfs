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

#include "storage_func.h"

#define STORAGE_OP_TYPE_SOURCE_CREATE_FILE	'C'
#define STORAGE_OP_TYPE_SOURCE_DELETE_FILE	'D'
#define STORAGE_OP_TYPE_SOURCE_UPDATE_FILE	'U'
#define STORAGE_OP_TYPE_SOURCE_CREATE_LINK	'L'
#define STORAGE_OP_TYPE_REPLICA_CREATE_FILE	'c'
#define STORAGE_OP_TYPE_REPLICA_DELETE_FILE	'd'
#define STORAGE_OP_TYPE_REPLICA_UPDATE_FILE	'u'
#define STORAGE_OP_TYPE_REPLICA_CREATE_LINK	'l'
#define STORAGE_BINLOG_BUFFER_SIZE		64 * 1024
#define STORAGE_BINLOG_LINE_SIZE		256

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	char *buffer;
	char *current;
	int length;
	int version;
} BinLogBuffer;

typedef struct
{
	char ip_addr[IP_ADDRESS_SIZE];
	bool need_sync_old;
	bool sync_old_done;
	bool last_file_exist;   //if the last file exist on the dest server
	BinLogBuffer binlog_buff;
	time_t until_timestamp;
	int mark_fd;
	int binlog_index;
	int binlog_fd;
	int64_t binlog_offset;
	int64_t scan_row_count;
	int64_t sync_row_count;

	int64_t last_scan_rows;  //for write to mark file
	int64_t last_sync_rows;  //for write to mark file
} BinLogReader;

typedef struct
{
	time_t timestamp;
	char op_type;
	char filename[128];  //filename with path index prefix which should be trimed
	char true_filename[128]; //pure filename
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

int storage_binlog_read(BinLogReader *pReader, \
			BinLogRecord *pRecord, int *record_length);

int storage_sync_thread_start(const FDFSStorageBrief *pStorage);
int kill_storage_sync_threads();
int fdfs_binlog_sync_func(void *args);

char *get_mark_filename_by_reader(const void *pArg, char *full_filename);
int storage_unlink_mark_file(const char *ip_addr);
int storage_rename_mark_file(const char *old_ip_addr, const int old_port, \
		const char *new_ip_addr, const int new_port);

int storage_open_readable_binlog(BinLogReader *pReader, \
		get_filename_func filename_func, const void *pArg);

int storage_reader_init(FDFSStorageBrief *pStorage, BinLogReader *pReader);
void storage_reader_destroy(BinLogReader *pReader);

#ifdef __cplusplus
}
#endif

#endif
