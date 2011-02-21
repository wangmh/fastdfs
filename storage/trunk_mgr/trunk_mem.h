/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//trunk_mem.h

#ifndef _TRUNK_MEM_H_
#define _TRUNK_MEM_H_

#include <pthread.h>

#define FDFS_TRUNK_STATUS_FREE  0
#define FDFS_TRUNK_STATUS_HOLD  1

#ifdef __cplusplus
extern "C" {
#endif

extern int g_slot_min_size;
extern int g_trunk_file_size;

typedef struct {
	int id;      //trunk file id
	int offset;  //file offset
	int size;    //space size
	int status;  //normal or hold
} FDFSTrunkInfo;

typedef struct {
	int size;
	ChainList free_trunk;
	pthread_mutex_t lock;
} FDFSTrunkSlot;

#ifdef __cplusplus
}
#endif

#endif
