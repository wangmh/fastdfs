/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "sched_thread.h"
#include "shared_func.h"
#include "logger.h"

static int sched_cmp_by_next_call_time(const void *p1, const void *p2)
{
	return ((ScheduleEntry *)p1)->next_call_time - \
			((ScheduleEntry *)p2)->next_call_time;
}

static int sched_init_entries(ScheduleArray *pScheduleArray)
{
	ScheduleEntry *pEntry;
	ScheduleEntry *pEnd;
	time_t current_time;

	if (pScheduleArray->count <= 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"schedule count %d <= 0",  \
			__LINE__, pScheduleArray->count);
		return ENOENT;
	}

	current_time = time(NULL);
	pEnd = pScheduleArray->entries + pScheduleArray->count;
	for (pEntry=pScheduleArray->entries; pEntry<pEnd; pEntry++)
	{
		if (pEntry->interval <= 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"shedule interval %d <= 0",  \
				__LINE__, pEntry->interval);
			return EINVAL;
		}

		pEntry->next_call_time = current_time + pEntry->interval;
	}


	return 0;
}

static void sched_make_chain(ScheduleArray *pScheduleArray)
{
	ScheduleEntry *pEntry;
	ScheduleEntry *pLast;

	qsort(pScheduleArray->entries, pScheduleArray->count, \
		sizeof(ScheduleEntry), sched_cmp_by_next_call_time);

	pLast = pScheduleArray->entries + (pScheduleArray->count - 1);
	for (pEntry=pScheduleArray->entries; pEntry<pLast; pEntry++)
	{
		pEntry->next = pEntry + 1;
	}
	pLast->next = NULL;
}

static void *sched_thread_entrance(void *args)
{
	ScheduleArray *pScheduleArray;
	ScheduleEntry *pHead;
	ScheduleEntry *pTail;
	ScheduleEntry *pPrevious;
	ScheduleEntry *pCurrent;
	ScheduleEntry *pSaveNext;
	ScheduleEntry *pNode;
	ScheduleEntry *pUntil;
	int exec_count;
	int i;

	time_t current_time;
	int sleep_time;

	pScheduleArray = (ScheduleArray *)args;
	if (sched_init_entries(pScheduleArray) != 0)
	{
		return NULL;
	}
	sched_make_chain(pScheduleArray);

	pHead = pScheduleArray->entries;
	pTail = pScheduleArray->entries + (pScheduleArray->count - 1);
	while (g_continue_flag)
	{
		current_time = time(NULL);
		sleep_time = pHead->next_call_time - current_time;

		fprintf(stderr, "count=%d, sleep_time=%d\n", pScheduleArray->count, sleep_time);
		if (sleep_time > 0)
		{
			sleep(sleep_time);
		}

		current_time = time(NULL);
		exec_count = 0;
		pCurrent = pHead;
		while (g_continue_flag && (pCurrent != NULL && \
			pCurrent->next_call_time <= current_time))
		{
			fprintf(stderr, "exec task id=%d\n", pCurrent->id);
			pCurrent->task_func(pCurrent->func_args);
			pCurrent->next_call_time = current_time + \
						pCurrent->interval;
			pCurrent = pCurrent->next;
			exec_count++;
		}

		if (pScheduleArray->count == 1)
		{
			continue;
		}

		if (exec_count > pScheduleArray->count / 2)
		{
			sched_make_chain(pScheduleArray);
			pHead = pScheduleArray->entries;
			pTail = pScheduleArray->entries + \
				(pScheduleArray->count - 1);
			continue;
		}

		pNode = pHead;
		pHead = pCurrent;  //new chain head
		for (i=0; i<exec_count; i++)
		{
			if (pNode->next_call_time >= pTail->next_call_time)
			{
				pTail->next = pNode;
				pTail = pNode;
				pNode = pNode->next;
				pTail->next = NULL;
				continue;
			}

			pPrevious = NULL;
			pUntil = pHead;
			while (pUntil != NULL && \
				pNode->next_call_time > pUntil->next_call_time)
			{
				pPrevious = pUntil;
				pUntil = pUntil->next;
			}

			pSaveNext = pNode->next;
			if (pPrevious == NULL)
			{
				pHead = pNode;
			}
			else
			{
				pPrevious->next = pNode;
			}
			pNode->next = pUntil;

			pNode = pSaveNext;
		}
	}

	return NULL;
}

int sched_start(ScheduleArray *pScheduleArray, pthread_t *ptid)
{
	int result;
	pthread_attr_t thread_attr;

	if ((result=init_pthread_attr(&thread_attr)) != 0)
	{
		return result;
	}

	if ((result=pthread_create(ptid, &thread_attr, \
		sched_thread_entrance, pScheduleArray)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"create thread failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	pthread_attr_destroy(&thread_attr);
	return result;
}

