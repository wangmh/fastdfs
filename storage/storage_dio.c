/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include "shared_func.h"
#include "pthread_func.h"
#include "logger.h"
#include "sockopt.h"
#include "storage_dio.h"
#include "storage_nio.h"

static pthread_mutex_t g_dio_thread_lock;
int g_dio_thread_count = 0;

static void *dio_thread_entrance(void* arg);
 
int storage_dio_init()
{
	int result;
	struct storage_dio_thread_data *pThreadData;
	struct storage_dio_thread_data *pDataEnd;
	struct storage_dio_context *pContext;
	struct storage_dio_context *pContextEnd;
	pthread_t tid;
	pthread_attr_t thread_attr;

	if ((result=init_pthread_lock(&g_dio_thread_lock)) != 0)
	{
		return result;
	}

	if ((result=init_pthread_attr(&thread_attr, g_thread_stack_size)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_attr fail, program exit!", __LINE__);
		return result;
	}

	g_dio_thread_data = (struct storage_dio_thread_data *)malloc(sizeof( \
				struct storage_dio_thread_data) * g_path_count);
	if (g_dio_thread_data == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s", \
			__LINE__, (int)sizeof(struct storage_dio_thread_data) * \
			g_path_count, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	g_dio_thread_count = 0;
	pDataEnd = g_dio_thread_data + g_path_count;
	for (pThreadData=g_dio_thread_data; pThreadData<pDataEnd; pThreadData++)
	{
		pThreadData->count=g_disk_reader_threads+g_disk_writer_threads;
		pThreadData->contexts = (struct storage_dio_context *)malloc(\
			sizeof(struct storage_dio_context) * pThreadData->count);
		if (pThreadData->contexts == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", __LINE__, \
				(int)sizeof(struct storage_dio_context) * \
				pThreadData->count, errno, strerror(errno));
			return errno != 0 ? errno : ENOMEM;
		}

		pThreadData->reader=pThreadData->contexts;
		pThreadData->writer=pThreadData->contexts+g_disk_reader_threads;
		pContextEnd = pThreadData->contexts + pThreadData->count;
		for (pContext=pThreadData->contexts; pContext<pContextEnd; \
			pContext++)
		{
			if ((result=task_queue_init(&(pContext->queue))) != 0)
			{
				return result;
			}

			if ((result=init_pthread_lock(&(pContext->lock))) != 0)
			{
				return result;
			}

			if ((result=pthread_create(&tid, &thread_attr, \
					dio_thread_entrance, pContext)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"create thread failed, " \
					"startup threads: %d, " \
					"errno: %d, error info: %s", \
					__LINE__, g_dio_thread_count, \
					result, strerror(result));
				return result;
			}
			else
			{
				pthread_mutex_lock(&g_dio_thread_lock);
				g_dio_thread_count++;
				pthread_mutex_unlock(&g_dio_thread_lock);
			}
		}
	}

	pthread_attr_destroy(&thread_attr);

	return result;
}

static void *dio_thread_entrance(void* arg) 
{
	int result; 
	struct storage_dio_context *pContext; 

	pContext = (struct storage_dio_context *)arg; 

	while (g_continue_flag)
	{
	}

	if ((result=pthread_mutex_lock(&g_dio_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}
	g_dio_thread_count--;
	if ((result=pthread_mutex_unlock(&g_dio_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return NULL;
}

