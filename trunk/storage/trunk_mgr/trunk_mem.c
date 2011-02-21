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
#include "trunk_mem.h"

int g_slot_min_size;
int g_trunk_file_size;

static int slot_max_size = g_trunk_file_size / 2;

static int slot_count = 0;
static FDFSTrunkSlot *slots = NULL;
static FDFSTrunkSlot *slot_end = NULL;

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

int trunk_alloc_space(const int size, FDFSTrunkInfo *pResult)
{
	FDFSTrunkSlot *pSlot;
	ChainNode *pNode;
	FDFSTrunkInfo *pTrunk;
	bool found;

	pSlot = trunk_get_slot(size);
	if (pSlot == NULL)
	{
		return NULL;
	}

	pthread_mutext_lock(&pSlot->lock);

	pTrunk = NULL;
	found = false;
	pNode = pSlot->free_trunk.head;
	while (pNode != NULL)
	{
		pTrunk = (FDFSTrunkInfo *)pNode->data;
		if (pTrunk->status == FDFS_TRUNK_STATUS_FREE)
		{
			found = true;
			break;
		}

		pNode = pNode->next;
	}

	if (found)
	{
		memcpy(pResult, pTrunk, sizeof(FDFSTrunkInfo));
		pTrunk->status = FDFS_TRUNK_STATUS_HOLD;
	}

	pthread_mutext_unlock(&pSlot->lock);

	return NULL;
}

static int *trunk_create_file()
{
	return 0;
}

