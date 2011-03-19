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
#include "fdfs_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "pthread_func.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "storage_global.h"
#include "storage_service.h"
#include "trunk_sync.h"
#include "trunk_mem.h"

int g_slot_min_size;
int g_trunk_file_size;

static int slot_max_size;
int g_store_path_mode = FDFS_STORE_PATH_ROUND_ROBIN;
int g_storage_reserved_mb = FDFS_DEF_STORAGE_RESERVED_MB;
int g_avg_storage_reserved_mb = FDFS_DEF_STORAGE_RESERVED_MB;
int g_store_path_index = 0;
int g_current_trunk_file_id = 0;

static FDFSTrunkSlot *slots = NULL;
static FDFSTrunkSlot *slot_end = NULL;
static pthread_mutex_t trunk_file_lock;
static struct fast_mblock_man trunk_blocks_man;

static int trunk_create_file(int *store_path_index, int *sub_path_high, \
		int *sub_path_low, int *file_id);
static int trunk_init_file(const char *filename, const int64_t file_size);
static int trunk_add_node(FDFSTrunkNode *pNode, const bool bNeedLock);

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

	slot_count = 1;
	slot_max_size = g_trunk_file_size / 2;
	bytes = g_slot_min_size;
	while (bytes < slot_max_size)
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
	(slot_end - 1)->size = slot_max_size;

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

	return 0;
}

int storage_trunk_destroy()
{
	return 0;
}

static char *trunk_info_dump(const FDFSTrunkFullInfo *pTrunkInfo, char *buff, \
				const int buff_size)
{
	snprintf(buff, buff_size, \
		"store_path_index=%d, " \
		"sub_path_high=%d, " \
		"sub_path_low=%d, " \
		"id=%d, offset=%d, size=%d, status=%d", \
		pTrunkInfo->path.store_path_index, \
		pTrunkInfo->path.sub_path_high, \
		pTrunkInfo->path.sub_path_low,  \
		pTrunkInfo->file.id, pTrunkInfo->file.offset, pTrunkInfo->file.size, \
		pTrunkInfo->status);

	return buff;
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

static int trunk_add_node(FDFSTrunkNode *pNode, const bool bNeedLock)
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

	if (bNeedLock)
	{
		pthread_mutex_lock(&pSlot->lock);
	}

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

	if (bNeedLock)
	{
		result=trunk_binlog_write(time(NULL), TRUNK_OP_TYPE_ADD_SPACE,\
					&(pNode->trunk));
		pthread_mutex_unlock(&pSlot->lock);
		return result;
	}
	else
	{
		return 0;
	}
}

int trunk_delete_node(const FDFSTrunkFullInfo *pTrunkInfo)
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
			"can't find trunk entry: %s", \
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
	result = trunk_binlog_write(time(NULL), TRUNK_OP_TYPE_DEL_SPACE, \
				&(pCurrent->trunk));
	fast_mblock_free(&trunk_blocks_man, pCurrent->pMblockNode);

	return result;
}

int trunk_restore_node(const FDFSTrunkFullInfo *pTrunkInfo)
{
	int result;
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
			"can't find trunk entry: %s", \
			trunk_info_dump(pTrunkInfo, buff, sizeof(buff)));
		return ENOENT;
	}

	pCurrent->trunk.status = FDFS_TRUNK_STATUS_FREE;
	pthread_mutex_unlock(&pSlot->lock);

	result = trunk_binlog_write(time(NULL), TRUNK_OP_TYPE_SET_SPACE_FREE, \
				pTrunkInfo);

	return result;
}

static int trunk_slit(FDFSTrunkNode *pNode, const int size)
{
	int result;
	struct fast_mblock_node *pMblockNode;
	FDFSTrunkNode *pTrunkNode;

	if (pNode->trunk.file.size - size < g_slot_min_size)
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
	int store_path_index;
	int sub_path_high;
	int sub_path_low;
	int file_id;

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

	do
	{
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
		result = trunk_create_file(&store_path_index, &sub_path_high, \
					&sub_path_low, &file_id);
		if (result != 0)
		{
			break;
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
			break;
		}
		pTrunkNode = (FDFSTrunkNode *)pMblockNode->data;

		pTrunkNode->pMblockNode = pMblockNode;
		pTrunkNode->trunk.path.store_path_index = store_path_index;
		pTrunkNode->trunk.path.sub_path_high = sub_path_high;
		pTrunkNode->trunk.path.sub_path_low = sub_path_low;
		pTrunkNode->trunk.file.id = file_id;
		pTrunkNode->trunk.file.offset = 0;
		pTrunkNode->trunk.file.size = g_trunk_file_size;
		pTrunkNode->trunk.status = FDFS_TRUNK_STATUS_FREE;
	}
	} while (0);

	result = trunk_slit(pTrunkNode, size);
	if (result == 0)
	{
		pTrunkNode->trunk.status = FDFS_TRUNK_STATUS_HOLD;
		result = trunk_add_node(pTrunkNode, true);
	}
	else
	{
		trunk_add_node(pTrunkNode, true);
	}

	if (result == 0)
	{
		memcpy(pResult, &(pTrunkNode->trunk), sizeof(FDFSTrunkFullInfo));
	}

	return result;
}

static int trunk_create_file(int *store_path_index, int *sub_path_high, \
		int *sub_path_low, int *file_id)
{
	char buff[16];
	int i;
	int result;
	int filename_len;
	char filename[64];
	char full_filename[MAX_PATH_SIZE];
	char *pStorePath;

	*store_path_index = g_store_path_index;
	if (g_store_path_mode == FDFS_STORE_PATH_LOAD_BALANCE)
	{
		if (*store_path_index < 0)
		{
			return ENOSPC;
		}
	}
	else
	{
		if (*store_path_index >= g_path_count)
		{
			*store_path_index = 0;
		}

		if (g_path_free_mbs[*store_path_index] <= \
			g_avg_storage_reserved_mb)
		{
			for (i=0; i<g_path_count; i++)
			{
				if (g_path_free_mbs[i] > g_avg_storage_reserved_mb)
				{
					*store_path_index = i;
					g_store_path_index = i;
					break;
				}
			}

			if (i == g_path_count)
			{
				return ENOSPC;
			}
		}

		g_store_path_index++;
		if (g_store_path_index >= g_path_count)
		{
			g_store_path_index = 0;
		}
	}

	pStorePath = g_store_paths[*store_path_index];

	while (1)
	{
		pthread_mutex_lock(&trunk_file_lock);
		*file_id = ++g_current_trunk_file_id;
		pthread_mutex_unlock(&trunk_file_lock);

		int2buff(*file_id, buff);
		base64_encode_ex(&g_base64_context, buff, sizeof(int), \
				filename, &filename_len, false);

		storage_get_store_path(filename, filename_len, \
					sub_path_high, sub_path_low);

		TRUNK_GET_FILENAME(*file_id, filename);
		snprintf(full_filename, sizeof(full_filename), \
			"%s/data/"STORAGE_DATA_DIR_FORMAT"/" \
			STORAGE_DATA_DIR_FORMAT"/%s", \
			pStorePath, *sub_path_high, *sub_path_low, filename);
		if (!fileExists(full_filename))
		{
			break;
		}
	}

	if ((result=trunk_init_file(full_filename, g_trunk_file_size)) != 0)
	{
		return result;
	}

	return 0;
}

static int trunk_init_file(const char *filename, const int64_t file_size)
{
	int fd;
	int result;
	int64_t remain_bytes;
	int write_bytes;
	char buff[256 * 1024];

	fd = open(filename, O_WRONLY | O_CREAT, 0644);
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

	memset(buff, 0, sizeof(buff));
	remain_bytes = file_size;
	while (remain_bytes > 0)
	{
		write_bytes = remain_bytes > sizeof(buff) ? \
				sizeof(buff) : remain_bytes;
		if (write(fd, buff, write_bytes) != write_bytes)
		{
			result = errno != 0 ? errno : EIO;
			logError("file: "__FILE__", line: %d, " \
				"write file %s fail, " \
				"errno: %d, error info: %s", \
				__LINE__, filename, \
				result, STRERROR(result));
			close(fd);
			return result;
		}

		remain_bytes -= write_bytes;
	}

	if (fsync(fd) != 0)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"fsync file \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filename, \
			result, STRERROR(result));
		close(fd);
		return result;
	}

	close(fd);
	return 0;
}

void trunk_file_info_encode(const FDFSTrunkFileInfo *pTrunkFile, char *str)
{
	char buff[sizeof(int) * 3];
	int len;

	int2buff(pTrunkFile->id, buff);
	int2buff(pTrunkFile->offset, buff + sizeof(int));
	int2buff(pTrunkFile->size, buff + sizeof(int) * 2);
	base64_encode_ex(&g_base64_context, buff, sizeof(buff), \
			str, &len, false);
}

void trunk_file_info_decode(char *str, FDFSTrunkFileInfo *pTrunkFile)
{
	char buff[sizeof(int) * 3];
	int len;

	base64_decode_auto(&g_base64_context, str, FDFS_TRUNK_FILE_INFO_LEN, \
		buff, &len);

	pTrunkFile->id = buff2int(buff);
	pTrunkFile->offset = buff2int(buff + sizeof(int));
	pTrunkFile->size = buff2int(buff + sizeof(int) * 2);
}

bool trunk_check_size(const int file_size)
{
	return file_size <= slot_max_size;
}

