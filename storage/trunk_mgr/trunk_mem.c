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

int storage_trunk_init()
{
	int slot_count;

	slot_max_size = g_trunk_file_size / 2;
	return 0;
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

static void trunk_add_node(FDFSTrunkInfo *pNode)
{
	FDFSTrunkSlot *pSlot;
	FDFSTrunkInfo *pPrevious;
	FDFSTrunkInfo *pCurrent;

	for (pSlot=slot_end-1; pSlot>=slots; pSlot--)
	{
		if (pNode->size >= pSlot->size)
		{
			break;
		}
	}

	pPrevious = NULL;
	pCurrent = pSlot->free_trunk_head;
	while (pCurrent != NULL && pNode->size > pCurrent->size)
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
}

static int trunk_slit(FDFSTrunkInfo *pNode, const int size)
{
	int result;
	struct fast_mblock_node *pMblockNode;
	FDFSTrunkInfo *pTrunk;

	if (pNode->size - size < g_slot_min_size)
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
			__LINE__, (int)sizeof(FDFSTrunkInfo), \
			result, STRERROR(result));
		return result;
	}

	pTrunk = (FDFSTrunkInfo *)pMblockNode->data;
	memcpy(pTrunk, pNode, sizeof(FDFSTrunkInfo));

	pTrunk->pMblockNode = pMblockNode;
	pTrunk->offset = pNode->offset + size;
	pTrunk->size = pNode->size - size;
	pTrunk->status = FDFS_TRUNK_STATUS_FREE;

	trunk_add_node(pTrunk);

	pNode->size = size;
	return 0;
}

int trunk_alloc_space(const int size, FDFSTrunkInfo *pResult)
{
	FDFSTrunkSlot *pSlot;
	FDFSTrunkInfo *pTrunk;
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

	pthread_mutex_lock(&pSlot->lock);

	pTrunk = pSlot->free_trunk_head;
	while (pTrunk != NULL && pTrunk->status == FDFS_TRUNK_STATUS_HOLD)
	{
		pTrunk = pTrunk->next;
	}

	do
	{
	if (pTrunk != NULL)
	{
		result = trunk_slit(pTrunk, size);
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
				__LINE__, (int)sizeof(FDFSTrunkInfo), \
				result, STRERROR(result));
			break;
		}
		pTrunk = (FDFSTrunkInfo *)pMblockNode->data;

		pTrunk->pMblockNode = pMblockNode;
		pTrunk->store_path_index = store_path_index;
		pTrunk->sub_path_high = sub_path_high;
		pTrunk->sub_path_low = sub_path_low;
		pTrunk->id = file_id;
		pTrunk->offset = 0;
		pTrunk->size = g_trunk_file_size;
		pTrunk->status = FDFS_TRUNK_STATUS_FREE;

		result = trunk_slit(pTrunk, size);
		trunk_add_node(pTrunk);
	}
	} while (0);

	if (result == 0)
	{
		memcpy(pResult, pTrunk, sizeof(FDFSTrunkInfo));
		pTrunk->status = FDFS_TRUNK_STATUS_HOLD;
	}

	pthread_mutex_unlock(&pSlot->lock);

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

		snprintf(full_filename, sizeof(full_filename), \
			"%s/data/%s%s", \
			pStorePath, buff, filename);
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

