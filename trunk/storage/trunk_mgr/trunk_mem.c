/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//trunk_mem.c

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "fdfs_define.h"
#include "chain.h"
#include "logger.h"
#include "sockopt.h"
#include "shared_func.h"
#include "pthread_func.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "storage_global.h"
#include "storage_func.h"
#include "storage_service.h"
#include "trunk_sync.h"
#include "trunk_mem.h"

#define STORAGE_TRUNK_DATA_FILENAME  "storage_trunk.dat"

int g_slot_min_size;
int g_trunk_file_size;

int g_slot_max_size;
int g_store_path_mode = FDFS_STORE_PATH_ROUND_ROBIN;
int g_storage_reserved_mb = FDFS_DEF_STORAGE_RESERVED_MB;
int g_avg_storage_reserved_mb = FDFS_DEF_STORAGE_RESERVED_MB;
int g_store_path_index = 0;
int g_current_trunk_file_id = 0;
TrackerServerInfo g_trunk_server = {-1, 0};
bool g_if_use_trunk_file = false;
bool g_if_trunker_self = false;

static FDFSTrunkSlot *slots = NULL;
static FDFSTrunkSlot *slot_end = NULL;
static pthread_mutex_t trunk_file_lock;
static struct fast_mblock_man trunk_blocks_man;

static int trunk_create_next_file(FDFSTrunkFullInfo *pTrunkInfo);
static int trunk_add_node(FDFSTrunkNode *pNode, const bool bWriteBinLog);

static int trunk_restore_node(const FDFSTrunkFullInfo *pTrunkInfo);
static int trunk_delete_space(const FDFSTrunkFullInfo *pTrunkInfo, \
		const bool bWriteBinLog);

static int storage_trunk_save();
static int storage_trunk_load();

static int trunk_init_slot(FDFSTrunkSlot *pTrunkSlot, const int bytes)
{
	int result;

	pTrunkSlot->size = bytes;
	pTrunkSlot->free_trunk_head = NULL;
	if ((result=init_pthread_lock(&(pTrunkSlot->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	return 0;
}

int storage_trunk_init()
{
	int result;
	int slot_count;
	int bytes;
	FDFSTrunkSlot *pTrunkSlot;

	if (!g_if_trunker_self)
	{
		return 0;
	}

	memset(&g_trunk_server, 0, sizeof(g_trunk_server));
	g_trunk_server.sock = -1;
	g_trunk_server.port = g_server_port;

	slot_count = 1;
	bytes = g_slot_min_size;
	while (bytes < g_slot_max_size)
	{
		slot_count++;
		bytes *= 2;
	}
	slot_count++;

	slots = (FDFSTrunkSlot *)malloc(sizeof(FDFSTrunkSlot) * slot_count);
	if (slots == NULL)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, (int)sizeof(FDFSTrunkSlot) * slot_count, \
			result, STRERROR(result));
		return result;
	}

	if ((result=trunk_init_slot(slots, 0)) != 0)
	{
		return result;
	}

	bytes = g_slot_min_size;
	slot_end = slots + slot_count;
	for (pTrunkSlot=slots+1; pTrunkSlot<slot_end; pTrunkSlot++)
	{
		if ((result=trunk_init_slot(pTrunkSlot, bytes)) != 0)
		{
			return result;
		}

		bytes *= 2;
	}
	(slot_end - 1)->size = g_slot_max_size;

	if ((result=init_pthread_lock(&trunk_file_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	if ((result=fast_mblock_init(&trunk_blocks_man, \
			sizeof(FDFSTrunkNode), 0)) != 0)
	{
		return result;
	}

	if ((result=storage_trunk_load()) != 0)
	{
		return result;
	}

	return 0;
}

int storage_trunk_destroy()
{
	int result;

	if (slots == NULL)
	{
		return 0;
	}

	result = storage_trunk_save();

	fast_mblock_destroy(&trunk_blocks_man);
	pthread_mutex_destroy(&trunk_file_lock);

	free(slots);
	slots = NULL;
	slot_end = NULL;

	return result;
}

static int64_t storage_trunk_get_binlog_size()
{
	char full_filename[MAX_PATH_SIZE];
	struct stat stat_buf;

	get_trunk_binlog_filename(full_filename);
	if (stat(full_filename, &stat_buf) != 0)
	{
		if (errno == ENOENT)
		{
			return 0;
		}

		logError("file: "__FILE__", line: %d, " \
			"stat file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, full_filename, \
			errno, STRERROR(errno));
		return -1;
	}

	return stat_buf.st_size;
}

static int storage_trunk_save()
{
	FDFSTrunkSlot *pSlot;
	FDFSTrunkNode *pCurrent;
	FDFSTrunkFullInfo *pTrunkInfo;
	int64_t trunk_binlog_size;
	char true_trunk_filename[MAX_PATH_SIZE];
	char temp_trunk_filename[MAX_PATH_SIZE];
	char buff[16 * 1024];
	char *p;
	int len;
	int fd;
	int result;

	trunk_binlog_size = storage_trunk_get_binlog_size();
	if (trunk_binlog_size < 0)
	{
		return errno != 0 ? errno : EPERM;
	}

	sprintf(temp_trunk_filename, "%s/data/.%s.tmp", \
		g_fdfs_base_path, STORAGE_TRUNK_DATA_FILENAME);

	fd = open(temp_trunk_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"open file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, temp_trunk_filename, \
			result, STRERROR(result));
		return result;
	}

	p = buff;
	len = sprintf(p, INT64_PRINTF_FORMAT"\n", trunk_binlog_size);
	p += len;

	result = 0;
	pthread_mutex_lock(&trunk_file_lock);
	for (pSlot=slots; pSlot<slot_end; pSlot++)
	{
		pCurrent = pSlot->free_trunk_head;
		while (pCurrent != NULL)
		{
			pTrunkInfo = &pCurrent->trunk;
			len = sprintf(p, "%d %d %d %d %d %d\n", \
				pTrunkInfo->path.store_path_index, \
				pTrunkInfo->path.sub_path_high, \
				pTrunkInfo->path.sub_path_low,  \
				pTrunkInfo->file.id, \
				pTrunkInfo->file.offset, \
				pTrunkInfo->file.size);
			p += len;
			if (p - buff > sizeof(buff) - 128)
			{
				if (write(fd, buff, p - buff) != p - buff)
				{
					result = errno != 0 ? errno : EIO;
					logError("file: "__FILE__", line: %d, "\
						"write to file %s fail, " \
						"errno: %d, error info: %s", \
						__LINE__, temp_trunk_filename, \
						result, STRERROR(result));
					break;
				}

				p = buff;
			}

			pCurrent = pCurrent->next;
		}
	}

	if (p - buff > 0 && result == 0)
	{
		if (write(fd, buff, p - buff) != p - buff)
		{
			result = errno != 0 ? errno : EIO;
			logError("file: "__FILE__", line: %d, "\
				"write to file %s fail, " \
				"errno: %d, error info: %s", \
				__LINE__, temp_trunk_filename, \
				result, STRERROR(result));
		}
	}

	if (result == 0 && fsync(fd) != 0)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, "\
			"fsync file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, temp_trunk_filename, \
			result, STRERROR(result));
	}

	close(fd);
	pthread_mutex_unlock(&trunk_file_lock);

	if (result != 0)
	{
		return result;
	}

	sprintf(true_trunk_filename, "%s/data/%s", \
		g_fdfs_base_path, STORAGE_TRUNK_DATA_FILENAME);
	if (rename(temp_trunk_filename, true_trunk_filename) != 0)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, "\
			"rename file %s to %s fail, " \
			"errno: %d, error info: %s", __LINE__, \
			temp_trunk_filename, true_trunk_filename, \
			result, STRERROR(result));
	}

	return result;
}

static int storage_trunk_restore(const int64_t restore_offset)
{
	int64_t trunk_binlog_size;
	TrunkBinLogReader reader;
	TrunkBinLogRecord record;
	char trunk_mark_filename[MAX_PATH_SIZE];
	int record_length;
	int result;

	trunk_binlog_size = storage_trunk_get_binlog_size();
	if (trunk_binlog_size < 0)
	{
		return errno != 0 ? errno : EPERM;
	}

	if (restore_offset == trunk_binlog_size)
	{
		return 0;
	}

	if (restore_offset > trunk_binlog_size)
	{
		logWarning("file: "__FILE__", line: %d, " \
			"restore_offset: "INT64_PRINTF_FORMAT \
			" > trunk_binlog_size: "INT64_PRINTF_FORMAT, \
			__LINE__, restore_offset, trunk_binlog_size);
		return storage_trunk_save();
	}

	logDebug("file: "__FILE__", line: %d, " \
		"trunk metadata recovering, start offset: " \
		INT64_PRINTF_FORMAT", recovery file size: " \
		INT64_PRINTF_FORMAT, __LINE__, \
		restore_offset, trunk_binlog_size - restore_offset);

	memset(&reader, 0, sizeof(reader));
	reader.binlog_offset = restore_offset;
	if ((result=trunk_reader_init(NULL, &reader)) != 0)
	{
		return result;
	}

	while (1)
	{
		result = trunk_binlog_read(&reader, &record, &record_length);
		if (result != 0)
		{
			if (result == ENOENT)
			{
				result = (reader.binlog_offset >= \
					trunk_binlog_size) ? 0 : EINVAL;
			}
			break;
		}

		if (record.op_type == TRUNK_OP_TYPE_ADD_SPACE)
		{
			record.trunk.status = FDFS_TRUNK_STATUS_FREE;
			if ((result=trunk_add_space(&record.trunk, false))!=0)
			{
				break;
			}
		}
		else if (record.op_type == TRUNK_OP_TYPE_DEL_SPACE)
		{
			record.trunk.status = FDFS_TRUNK_STATUS_FREE;
			if ((result=trunk_delete_space(&record.trunk,false))!=0)
			{
				if (result == ENOENT)
				{
					result = 0;
				}
				else
				{
					break;
				}
			}
		}

		reader.binlog_offset += record_length;
	}

	trunk_reader_destroy(&reader);
	trunk_mark_filename_by_reader(&reader, trunk_mark_filename);
	if (unlink(trunk_mark_filename) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"unlink file %s fail, " \
			"errno: %d, error info: %s", __LINE__, \
			trunk_mark_filename, errno, STRERROR(errno));
	}

	if (result == 0)
	{
		logDebug("file: "__FILE__", line: %d, " \
			"trunk metadata recovery done. start offset: " \
			INT64_PRINTF_FORMAT", recovery file size: " \
			INT64_PRINTF_FORMAT, __LINE__, \
			restore_offset, trunk_binlog_size - restore_offset);
		return storage_trunk_save();
	}

	return result;
}

static int storage_trunk_load()
{
#define TRUNK_DATA_FIELD_COUNT  6

	int64_t restore_offset;
	char trunk_data_filename[MAX_PATH_SIZE];
	char buff[4 * 1024 + 1];
	int line_count;
	char *pLineStart;
	char *pLineEnd;
	char *cols[TRUNK_DATA_FIELD_COUNT];
	FDFSTrunkFullInfo trunkInfo;
	int result;
	int fd;
	int bytes;
	int len;

	sprintf(trunk_data_filename, "%s/data/%s", \
		g_fdfs_base_path, STORAGE_TRUNK_DATA_FILENAME);
	fd = open(trunk_data_filename, O_RDONLY);
	if (fd < 0)
	{
		result = errno != 0 ? errno : EIO;
		if (result == ENOENT)
		{
			restore_offset = 0;
			return storage_trunk_restore(restore_offset);
		}

		logError("file: "__FILE__", line: %d, " \
			"open file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, trunk_data_filename, \
			result, STRERROR(result));
		return result;
	}

	if ((bytes=read(fd, buff, sizeof(buff) - 1)) < 0)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"read from file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, trunk_data_filename, \
			result, STRERROR(result));
		close(fd);
		return result;
	}

	*(buff + bytes) = '\0';
	pLineEnd = strchr(buff, '\n');
	if (pLineEnd == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"read offset from file %s fail", \
			__LINE__, trunk_data_filename);
		close(fd);
		return EINVAL;
	}

	*pLineEnd = '\0';
	restore_offset = strtoll(buff, NULL, 10);
	pLineStart = pLineEnd + 1;  //skip \n
	line_count = 0;
	while (1)
	{
		pLineEnd = strchr(pLineStart, '\n');
		if (pLineEnd == NULL)
		{
			if (bytes < sizeof(buff) - 1) //EOF
			{
				break;
			}

			len = strlen(pLineStart);
			if (len > 64)
			{
				logError("file: "__FILE__", line: %d, " \
					"file %s, line length: %d too long", \
					__LINE__, trunk_data_filename, len);
				close(fd);
				return EINVAL;
			}

			memcpy(buff, pLineStart, len);
			if ((bytes=read(fd, buff + len, sizeof(buff) \
					- len - 1)) < 0)
			{
				result = errno != 0 ? errno : EIO;
				logError("file: "__FILE__", line: %d, " \
					"read from file %s fail, " \
					"errno: %d, error info: %s", \
					__LINE__, trunk_data_filename, \
					result, STRERROR(result));
				close(fd);
				return result;
			}

			bytes += len;
			*(buff + bytes) = '\0';
			pLineStart = buff;
			continue;
		}

		++line_count;
		*pLineEnd = '\0';
		if (splitEx(pLineStart, ' ', cols, TRUNK_DATA_FIELD_COUNT) \
			!= TRUNK_DATA_FIELD_COUNT)
		{
			logError("file: "__FILE__", line: %d, " \
				"file %s, line: %d is invalid", \
				__LINE__, trunk_data_filename, line_count);
			close(fd);
			return EINVAL;
		}

		trunkInfo.path.store_path_index = atoi(cols[0]);
		trunkInfo.path.sub_path_high = atoi(cols[1]);
		trunkInfo.path.sub_path_low = atoi(cols[2]);
		trunkInfo.file.id = atoi(cols[3]);
		trunkInfo.file.offset = atoi(cols[4]);
		trunkInfo.file.size = atoi(cols[5]);
		if ((result=trunk_add_space(&trunkInfo, false)) != 0)
		{
			close(fd);
			return result;
		}

		pLineStart = pLineEnd + 1;  //next line
	}

	close(fd);

	if (*pLineStart != '\0')
	{
		logError("file: "__FILE__", line: %d, " \
			"file %s does not end correctly", \
			__LINE__, trunk_data_filename);
		return EINVAL;
	}

	return storage_trunk_restore(restore_offset);
}

static FDFSTrunkSlot *trunk_get_slot(const int size)
{
	FDFSTrunkSlot *pSlot;

	for (pSlot=slots; pSlot<slot_end; pSlot++)
	{
		if (size <= pSlot->size)
		{
			return pSlot;
		}
	}

	return NULL;
}

int trunk_free_space(const FDFSTrunkFullInfo *pTrunkInfo, \
		const bool bWriteBinLog)
{
	int result;
	struct fast_mblock_node *pMblockNode;
	FDFSTrunkNode *pTrunkNode;

	if (!g_if_trunker_self)
	{
		return EINVAL;
	}

	if (pTrunkInfo->file.size < g_slot_min_size)
	{
		return 0;
	}

	pMblockNode = fast_mblock_alloc(&trunk_blocks_man);
	if (pMblockNode == NULL)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, (int)sizeof(FDFSTrunkNode), \
			result, STRERROR(result));
		return result;
	}

	pTrunkNode = (FDFSTrunkNode *)pMblockNode->data;
	memcpy(&pTrunkNode->trunk, pTrunkInfo, sizeof(FDFSTrunkFullInfo));

	pTrunkNode->pMblockNode = pMblockNode;
	pTrunkNode->trunk.status = FDFS_TRUNK_STATUS_FREE;

	return trunk_add_node(pTrunkNode, bWriteBinLog);
}

static int trunk_add_node(FDFSTrunkNode *pNode, const bool bWriteBinLog)
{
	int result;
	FDFSTrunkSlot *pSlot;
	FDFSTrunkNode *pPrevious;
	FDFSTrunkNode *pCurrent;

	for (pSlot=slot_end-1; pSlot>=slots; pSlot--)
	{
		if (pNode->trunk.file.size >= pSlot->size)
		{
			break;
		}
	}

	pthread_mutex_lock(&pSlot->lock);

	pPrevious = NULL;
	pCurrent = pSlot->free_trunk_head;
	while (pCurrent != NULL && pNode->trunk.file.size > \
		pCurrent->trunk.file.size)
	{
		pPrevious = pCurrent;
		pCurrent = pCurrent->next;
	}

	pNode->next = pCurrent;
	if (pPrevious == NULL)
	{
		pSlot->free_trunk_head = pNode;
	}
	else
	{
		pPrevious->next = pNode;
	}

	if (bWriteBinLog)
	{
		result = trunk_binlog_write(time(NULL), \
				TRUNK_OP_TYPE_ADD_SPACE, &(pNode->trunk));
	}
	else
	{
		result = 0;
	}

	pthread_mutex_unlock(&pSlot->lock);
	return result;
}

static int trunk_delete_space(const FDFSTrunkFullInfo *pTrunkInfo, \
		const bool bWriteBinLog)
{
	int result;
	FDFSTrunkSlot *pSlot;
	FDFSTrunkNode *pPrevious;
	FDFSTrunkNode *pCurrent;

	for (pSlot=slot_end-1; pSlot>=slots; pSlot--)
	{
		if (pTrunkInfo->file.size >= pSlot->size)
		{
			break;
		}
	}
	
	pthread_mutex_lock(&pSlot->lock);
	pPrevious = NULL;
	pCurrent = pSlot->free_trunk_head;
	while (pCurrent != NULL && memcmp(&(pCurrent->trunk), pTrunkInfo, \
		sizeof(FDFSTrunkFullInfo)) != 0)
	{
		pPrevious = pCurrent;
		pCurrent = pCurrent->next;
	}

	if (pCurrent == NULL)
	{
		char buff[256];

		pthread_mutex_unlock(&pSlot->lock);
		logError("file: "__FILE__", line: %d, " \
			"can't find trunk entry: %s", __LINE__, \
			trunk_info_dump(pTrunkInfo, buff, sizeof(buff)));
		return ENOENT;
	}

	if (pPrevious == NULL)
	{
		pSlot->free_trunk_head = pCurrent->next;
	}
	else
	{
		pPrevious->next = pCurrent->next;
	}

	pthread_mutex_unlock(&pSlot->lock);
	if (bWriteBinLog)
	{
		result = trunk_binlog_write(time(NULL), \
				TRUNK_OP_TYPE_DEL_SPACE, &(pCurrent->trunk));
	}
	else
	{
		result = 0;
	}

	fast_mblock_free(&trunk_blocks_man, pCurrent->pMblockNode);

	return result;
}

static int trunk_restore_node(const FDFSTrunkFullInfo *pTrunkInfo)
{
	FDFSTrunkSlot *pSlot;
	FDFSTrunkNode *pCurrent;

	for (pSlot=slot_end-1; pSlot>=slots; pSlot--)
	{
		if (pTrunkInfo->file.size >= pSlot->size)
		{
			break;
		}
	}
	
	pthread_mutex_lock(&pSlot->lock);
	pCurrent = pSlot->free_trunk_head;
	while (pCurrent != NULL && memcmp(&(pCurrent->trunk), pTrunkInfo, \
		sizeof(FDFSTrunkFullInfo)) != 0)
	{
		pCurrent = pCurrent->next;
	}

	if (pCurrent == NULL)
	{
		char buff[256];

		pthread_mutex_unlock(&pSlot->lock);

		logError("file: "__FILE__", line: %d, " \
			"can't find trunk entry: %s", __LINE__, \
			trunk_info_dump(pTrunkInfo, buff, sizeof(buff)));
		return ENOENT;
	}

	pCurrent->trunk.status = FDFS_TRUNK_STATUS_FREE;
	pthread_mutex_unlock(&pSlot->lock);

	return 0;
}

static int trunk_split(FDFSTrunkNode *pNode, const int size)
{
	int result;
	struct fast_mblock_node *pMblockNode;
	FDFSTrunkNode *pTrunkNode;

	if (pNode->trunk.file.size - size < g_slot_min_size)
	{
		return trunk_binlog_write(time(NULL), \
			TRUNK_OP_TYPE_DEL_SPACE, &(pNode->trunk));
	}

	pMblockNode = fast_mblock_alloc(&trunk_blocks_man);
	if (pMblockNode == NULL)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, (int)sizeof(FDFSTrunkNode), \
			result, STRERROR(result));
		return result;
	}

	pTrunkNode = (FDFSTrunkNode *)pMblockNode->data;
	memcpy(pTrunkNode, pNode, sizeof(FDFSTrunkNode));

	pTrunkNode->pMblockNode = pMblockNode;
	pTrunkNode->trunk.file.offset = pNode->trunk.file.offset + size;
	pTrunkNode->trunk.file.size = pNode->trunk.file.size - size;
	pTrunkNode->trunk.status = FDFS_TRUNK_STATUS_FREE;

	result = trunk_add_node(pTrunkNode, true);
	if (result != 0)
	{
		return result;
	}

	result = trunk_binlog_write(time(NULL), \
			TRUNK_OP_TYPE_DEL_SPACE, &(pNode->trunk));
	if (result != 0)
	{
		trunk_delete_space(&(pTrunkNode->trunk), true); //rollback
		return result;
	}

	pNode->trunk.file.size = size;
	return 0;
}

int trunk_alloc_space(const int size, FDFSTrunkFullInfo *pResult)
{
	FDFSTrunkSlot *pSlot;
	FDFSTrunkNode *pPreviousNode;
	FDFSTrunkNode *pTrunkNode;
	struct fast_mblock_node *pMblockNode;
	int result;

	if (!g_if_trunker_self)
	{
		return EINVAL;
	}

	pSlot = trunk_get_slot(size);
	if (pSlot == NULL)
	{
		return ENOENT;
	}

	while (1)
	{
		pthread_mutex_lock(&pSlot->lock);

		pPreviousNode = NULL;
		pTrunkNode = pSlot->free_trunk_head;
		while (pTrunkNode != NULL && \
			pTrunkNode->trunk.status == FDFS_TRUNK_STATUS_HOLD)
		{
			pPreviousNode = pTrunkNode;
			pTrunkNode = pTrunkNode->next;
		}

		if (pTrunkNode != NULL)
		{
			break;
		}

		pthread_mutex_unlock(&pSlot->lock);

		pSlot++;
		if (pSlot >= slot_end)
		{
			break;
		}
	}

	if (pTrunkNode != NULL)
	{
		if (pPreviousNode == NULL)
		{
			pSlot->free_trunk_head = pTrunkNode->next;
		}
		else
		{
			pPreviousNode->next = pTrunkNode->next;
		}

		pthread_mutex_unlock(&pSlot->lock);
	}
	else
	{
		pMblockNode = fast_mblock_alloc(&trunk_blocks_man);
		if (pMblockNode == NULL)
		{
			result = errno != 0 ? errno : EIO;
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", \
				__LINE__, (int)sizeof(FDFSTrunkNode), \
				result, STRERROR(result));
			return result;
		}
		pTrunkNode = (FDFSTrunkNode *)pMblockNode->data;
		pTrunkNode->pMblockNode = pMblockNode;

		pTrunkNode->trunk.file.offset = 0;
		pTrunkNode->trunk.file.size = g_trunk_file_size;
		pTrunkNode->trunk.status = FDFS_TRUNK_STATUS_FREE;

		result = trunk_create_next_file(&(pTrunkNode->trunk));
		if (result != 0)
		{
			fast_mblock_free(&trunk_blocks_man, pMblockNode);
			return result;
		}

		trunk_binlog_write(time(NULL), \
			TRUNK_OP_TYPE_ADD_SPACE, &(pTrunkNode->trunk));
	}

	result = trunk_split(pTrunkNode, size);
	if (result != 0)
	{
		return result;
	}

	pTrunkNode->trunk.status = FDFS_TRUNK_STATUS_HOLD;
	result = trunk_add_node(pTrunkNode, true);
	if (result == 0)
	{
		memcpy(pResult, &(pTrunkNode->trunk), \
			sizeof(FDFSTrunkFullInfo));
	}

	return result;
}

int trunk_alloc_confirm(const FDFSTrunkFullInfo *pTrunkInfo, const int status)
{
	if (!g_if_trunker_self)
	{
		return EINVAL;
	}

	if (status == 0)
	{
		return trunk_delete_space(pTrunkInfo, true);
	}
	else
	{
		return trunk_restore_node(pTrunkInfo);
	}
}

static int trunk_create_next_file(FDFSTrunkFullInfo *pTrunkInfo)
{
	char buff[16];
	int i;
	int result;
	int filename_len;
	char filename[64];
	char full_filename[MAX_PATH_SIZE];
	int store_path_index;
	int sub_path_high;
	int sub_path_low;

	store_path_index = g_store_path_index;
	if (g_store_path_mode == FDFS_STORE_PATH_LOAD_BALANCE)
	{
		if (store_path_index < 0)
		{
			return ENOSPC;
		}
	}
	else
	{
		if (store_path_index >= g_fdfs_path_count)
		{
			store_path_index = 0;
		}

		if (g_path_free_mbs[store_path_index] <= \
			g_avg_storage_reserved_mb)
		{
			for (i=0; i<g_fdfs_path_count; i++)
			{
				if (g_path_free_mbs[i] > g_avg_storage_reserved_mb)
				{
					store_path_index = i;
					g_store_path_index = i;
					break;
				}
			}

			if (i == g_fdfs_path_count)
			{
				return ENOSPC;
			}
		}

		g_store_path_index++;
		if (g_store_path_index >= g_fdfs_path_count)
		{
			g_store_path_index = 0;
		}
	}

	pTrunkInfo->path.store_path_index = store_path_index;

	while (1)
	{
		pthread_mutex_lock(&trunk_file_lock);
		pTrunkInfo->file.id = ++g_current_trunk_file_id;
		result = storage_write_to_sync_ini_file();
		pthread_mutex_unlock(&trunk_file_lock);
		if (result != 0)
		{
			return result;
		}

		int2buff(pTrunkInfo->file.id, buff);
		base64_encode_ex(&g_fdfs_base64_context, buff, sizeof(int), \
				filename, &filename_len, false);

		storage_get_store_path(filename, filename_len, \
					&sub_path_high, &sub_path_low);

		pTrunkInfo->path.sub_path_high = sub_path_high;
		pTrunkInfo->path.sub_path_low = sub_path_low;

		trunk_get_full_filename(pTrunkInfo, full_filename, \
			sizeof(full_filename));
		if (!fileExists(full_filename))
		{
			break;
		}
	}

	if ((result=trunk_init_file(full_filename)) != 0)
	{
		return result;
	}

	return 0;
}

int trunk_init_file_ex(const char *filename, const int64_t file_size)
{
	int fd;
	int result;

	fd = open(filename, O_WRONLY | O_CREAT | O_EXCL, 0644);
	if (fd < 0)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"open file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filename, \
			result, STRERROR(result));
		return result;
	}

	if (ftruncate(fd, file_size) == 0)
	{
		result = 0;
	}
	else
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"ftruncate file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filename, \
			result, STRERROR(result));
	}

	close(fd);
	return result;
}

int trunk_check_and_init_file_ex(const char *filename, const int64_t file_size)
{
	struct stat file_stat;
	int fd;
	int result;

	if (stat(filename, &file_stat) != 0)
	{
		result = errno != 0 ? errno : ENOENT;
		if (result != ENOENT)
		{
			logError("file: "__FILE__", line: %d, " \
				"stat file %s fail, " \
				"errno: %d, error info: %s", \
				__LINE__, filename, \
				result, STRERROR(result));
			return result;
		}

		return trunk_init_file_ex(filename, file_size);
	}

	if (file_stat.st_size >= file_size)
	{
		return 0;
	}

	logWarning("file: "__FILE__", line: %d, " \
		"file: %s, file size: "INT64_PRINTF_FORMAT \
		" < "INT64_PRINTF_FORMAT", should be resize", \
		__LINE__, filename, (int64_t)file_stat.st_size, file_size);

	fd = open(filename, O_WRONLY, 0644);
	if (fd < 0)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"open file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filename, \
			result, STRERROR(result));
		return result;
	}

	if (ftruncate(fd, file_size) == 0)
	{
		result = 0;
	}
	else
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"ftruncate file %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filename, \
			result, STRERROR(result));
	}

	close(fd);
	return result;
}

bool trunk_check_size(const int64_t file_size)
{
	return file_size <= g_slot_max_size;
}

int trunk_file_delete(const char *trunk_filename, \
			FDFSTrunkFullInfo *pTrunkInfo)
{
	char pack_buff[FDFS_TRUNK_FILE_HEADER_SIZE];
	FDFSTrunkHeader trunkHeader;
	int fd;
	int write_bytes;
	int result;

	fd = open(trunk_filename, O_WRONLY);
	if (fd < 0)
	{
		return errno != 0 ? errno : EIO;
	}

	if (lseek(fd, pTrunkInfo->file.offset, SEEK_SET) < 0)
	{
		result = errno != 0 ? errno : EIO;
		close(fd);
		return result;
	}

	memset(&trunkHeader, 0, sizeof(trunkHeader));
	trunkHeader.alloc_size = pTrunkInfo->file.size;
	trunkHeader.file_type = FDFS_TRUNK_FILE_TYPE_NONE;
	trunk_pack_header(&trunkHeader, pack_buff);

	write_bytes = write(fd, pack_buff, FDFS_TRUNK_FILE_HEADER_SIZE);
	result = errno;
	close(fd);
	if (write_bytes != FDFS_TRUNK_FILE_HEADER_SIZE)
	{
		return result != 0 ? errno : EINVAL;
	}

	return 0;
}

