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

struct storage_dio_thread_data
{
	struct fast_task_queue queue;
	pthread_mutex_t signal_lock;
};

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif

