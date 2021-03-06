//fast_task_queue.c

#include <errno.h>
#include <sys/resource.h>
#include <pthread.h>
#include "fast_task_queue.h"
#include "logger.h"
#include "shared_func.h"
#include "pthread_func.h"

static struct fast_task_queue g_free_queue;

static struct fast_task_info *g_mpool = NULL;

int task_queue_init(struct fast_task_queue *pQueue)
{
	int result;

	if ((result=init_pthread_lock(&(pQueue->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	pQueue->head = NULL;
	pQueue->tail = NULL;

	return 0;
}

int free_queue_init(const int max_connections, const int min_buff_size, \
		const int max_buff_size, const int arg_size)
{
	struct fast_task_info *pTask;
	char *p;
	char *pCharEnd;
	int block_size;
	int alloc_size;
	int64_t total_size;
	int result;

	if ((result=init_pthread_lock(&(g_free_queue.lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	block_size = sizeof(struct fast_task_info) + arg_size;
	alloc_size = block_size * max_connections;

	if (max_buff_size > min_buff_size)
	{
		total_size = alloc_size;
		g_free_queue.malloc_whole_block = false;
	}
	else
	{
		struct rlimit rlimit_data;
		rlim_t max_data_size;

		if (getrlimit(RLIMIT_DATA, &rlimit_data) < 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call getrlimit fail, " \
				"errno: %d, error info: %s", \
				__LINE__, errno, STRERROR(errno));
			return errno != 0 ? errno : EPERM;
		}
		if (rlimit_data.rlim_cur == RLIM_INFINITY)
		{
			max_data_size = 512 * 1024 * 1024;
		}
		else
		{
			max_data_size = rlimit_data.rlim_cur;
			if (max_data_size > 512 * 1024 * 1024)
			{
				max_data_size = 512 * 1024 * 1024;
			}
		}

		total_size = alloc_size+(int64_t)min_buff_size*max_connections;
		if (total_size <= max_data_size)
		{
			g_free_queue.malloc_whole_block = true;
			block_size += min_buff_size;
		}
		else
		{
			g_free_queue.malloc_whole_block = false;
			total_size = alloc_size;
		}
	}

	g_mpool = (struct fast_task_info *)malloc(total_size);
	if (g_mpool == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc "INT64_PRINTF_FORMAT" bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, total_size, errno, STRERROR(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	memset(g_mpool, 0, total_size);

	pCharEnd = ((char *)g_mpool) + total_size;
	for (p=(char *)g_mpool; p<pCharEnd; p += block_size)
	{
		pTask = (struct fast_task_info *)p;
		pTask->size = min_buff_size;

		pTask->arg = p + sizeof(struct fast_task_info);
		if (g_free_queue.malloc_whole_block)
		{
			pTask->data = (char *)pTask->arg + arg_size;
		}
		else
		{
			pTask->data = (char *)malloc(pTask->size);
			if (pTask->data == NULL)
			{
				free_queue_destroy();

				logError("file: "__FILE__", line: %d, " \
					"malloc %d bytes fail, " \
					"errno: %d, error info: %s", \
					__LINE__, pTask->size, \
					errno, STRERROR(errno));
				return errno != 0 ? errno : ENOMEM;
			}
		}
	}

	g_free_queue.tail = (struct fast_task_info *)(pCharEnd - block_size);
	for (p=(char *)g_mpool; p<(char *)g_free_queue.tail; p += block_size)
	{
		pTask = (struct fast_task_info *)p;
		pTask->next = (struct fast_task_info *)(p + block_size);
	}

	g_free_queue.max_connections = max_connections;
	g_free_queue.min_buff_size = min_buff_size;
	g_free_queue.max_buff_size = max_buff_size;
	g_free_queue.arg_size = arg_size;
	g_free_queue.head = g_mpool;
	g_free_queue.tail->next = NULL;

	return 0;
}

void free_queue_destroy()
{
	if (g_mpool == NULL)
	{
		return;
	}

	if (!g_free_queue.malloc_whole_block)
	{
		char *p;
		char *pCharEnd;
		int block_size;
		struct fast_task_info *pTask;

		block_size = sizeof(struct fast_task_info) + \
					g_free_queue.arg_size;
		pCharEnd = ((char *)g_mpool) + block_size * \
				g_free_queue.max_connections;
		for (p=(char *)g_mpool; p<pCharEnd; p += block_size)
		{
			pTask = (struct fast_task_info *)p;
			if (pTask->data != NULL)
			{
				free(pTask->data);
				pTask->data = NULL;
			}
		}
	}

	free(g_mpool);
	g_mpool = NULL;

	pthread_mutex_destroy(&(g_free_queue.lock));
}

struct fast_task_info *free_queue_pop()
{
	return task_queue_pop(&g_free_queue);;
}

int free_queue_push(struct fast_task_info *pTask)
{
	char *new_buff;
	int result;

	*(pTask->client_ip) = '\0';
	pTask->length = 0;
	pTask->offset = 0;
	pTask->req_count = 0;

	if (pTask->size > g_free_queue.min_buff_size) //need thrink
	{
		new_buff = (char *)malloc(g_free_queue.min_buff_size);
		if (new_buff == NULL)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", \
				__LINE__, g_free_queue.min_buff_size, \
				errno, STRERROR(errno));
		}
		else
		{
			free(pTask->data);
			pTask->size = g_free_queue.min_buff_size;
			pTask->data = new_buff;
		}
	}

	if ((result=pthread_mutex_lock(&g_free_queue.lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	pTask->next = g_free_queue.head;
	g_free_queue.head = pTask;
	if (g_free_queue.tail == NULL)
	{
		g_free_queue.tail = pTask;
	}

	if ((result=pthread_mutex_unlock(&g_free_queue.lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	return result;
}

int free_queue_count()
{
	return task_queue_count(&g_free_queue);
}

int task_queue_push(struct fast_task_queue *pQueue, \
		struct fast_task_info *pTask)
{
	int result;

	if ((result=pthread_mutex_lock(&(pQueue->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	pTask->next = NULL;
	if (pQueue->tail == NULL)
	{
		pQueue->head = pTask;
	}
	else
	{
		pQueue->tail->next = pTask;
	}
	pQueue->tail = pTask;

	if ((result=pthread_mutex_unlock(&(pQueue->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	return 0;
}

struct fast_task_info *task_queue_pop(struct fast_task_queue *pQueue)
{
	struct fast_task_info *pTask;
	int result;

	if ((result=pthread_mutex_lock(&(pQueue->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return NULL;
	}

	pTask = pQueue->head;
	if (pTask != NULL)
	{
		pQueue->head = pTask->next;
		if (pQueue->head == NULL)
		{
			pQueue->tail = NULL;
		}
	}

	if ((result=pthread_mutex_unlock(&(pQueue->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	return pTask;
}

int task_queue_count(struct fast_task_queue *pQueue)
{
	struct fast_task_info *pTask;
	int count;
	int result;

	if ((result=pthread_mutex_lock(&(pQueue->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return 0;
	}

	count = 0;
	pTask = pQueue->head;
	while (pTask != NULL)
	{
		pTask = pTask->next;
		count++;
	}

	if ((result=pthread_mutex_unlock(&(pQueue->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	return count;
}

