/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_dio.h

#ifndef _STORAGE_DIO_H
#define _STORAGE_DIO_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "tracker_types.h"
#include "fast_task_queue.h"

struct storage_dio_context
{
	struct fast_task_queue queue;
	pthread_mutex_t lock;
	pthread_cond_t cond;
};

struct storage_dio_thread_data
{
	/* for mixed read / write */
	struct storage_dio_context *contexts;
	int count;  //context count

	/* for separated read / write */
	struct storage_dio_context *reader;
	struct storage_dio_context *writer;
};

#ifdef __cplusplus
extern "C" {
#endif

extern int g_dio_thread_count;

#ifdef __cplusplus
}
#endif

#endif

