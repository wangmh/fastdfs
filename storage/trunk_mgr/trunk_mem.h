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
#include "chain.h"

#define FDFS_TRUNK_STATUS_FREE  0
#define FDFS_TRUNK_STATUS_HOLD  1

#ifdef __cplusplus
extern "C" {
#endif

extern int g_slot_min_size;
extern int g_trunk_file_size;
extern int g_store_path_mode;  //store which path mode, fetch from tracker
extern int g_storage_reserved_mb;  //fetch from tracker
extern int g_avg_storage_reserved_mb;  //calc by above var: g_storage_reserved_mb
extern int g_store_path_index;  //store to which path
extern int g_current_trunk_file_id;  //current trunk file id

typedef struct {
	unsigned char store_path_index;   //store which path as Mxx
	unsigned char sub_path_high;      //high sub dir index, front part of HH/HH
	unsigned char sub_path_low;       //low sub dir index, tail part of HH/HH
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
