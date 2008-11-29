/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#ifndef _SCHED_THREAD_H_
#define _SCHED_THREAD_H_

#include "fdfs_define.h"

typedef void (*TaskFunc) (void *args);

typedef struct tagScheduleEntry
{
	int interval;   //unit: second
	TaskFunc task_func;
	void *func_args;

	time_t next_call_time;
	struct tagScheduleEntry *next;
} ScheduleEntry;

typedef struct
{
	ScheduleEntry *entries;
	int count;
} ScheduleArray;

#ifdef __cplusplus
extern "C" {
#endif

extern bool g_continue_flag;
int sched_start(ScheduleArray *pScheduleArray, pthread_t *ptid);

#ifdef __cplusplus
}
#endif

#endif

