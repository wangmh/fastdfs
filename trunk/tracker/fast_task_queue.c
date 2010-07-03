//fast_task_queue.c

#include <errno.h>
#include <pthread.h>
#include "fast_task_queue.h"
#include "logger.h"
#include "shared_func.h"
#include "pthread_func.h"

static struct fast_task_queue g_free_queue;

static struct fast_task_info *g_mpool = NULL;

static struct fast_task_info *_queue_pop_task(struct fast_task_queue *pQueue);
static int _task_queue_count(struct fast_task_queue *pQueue);

int task_queue_init(const int max_connections, const int min_buff_size, \
		const int max_buff_size)
{
	struct fast_task_info *pTask;
	struct fast_task_info *pEnd;
	int alloc_size;
	int64_t total_size;
	int result;

	if ((result=init_pthread_lock(&(g_free_queue.lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	alloc_size = sizeof(struct fast_task_info) * max_connections;

	if (max_buff_size > min_buff_size)
	{
		total_size = alloc_size;
		g_free_queue.malloc_whole_block = false;
	}
	else
	{
		total_size = alloc_size+(int64_t)min_buff_size*max_connections;
		if (total_size <= 512 * 1024 * 1024)
		{
			g_free_queue.malloc_whole_block = true;
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
			"malloc %d bytes fail, errno: %d, error info: %s", \
			__LINE__, alloc_size, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	memset(g_mpool, 0, total_size);

	if (g_free_queue.malloc_whole_block)
	{
		char *p;
		char *pCharEnd;
		int block_size;

		block_size = sizeof(struct fast_task_info) + min_buff_size;
		pCharEnd = ((char *)g_mpool) + total_size;
		for (p=(char *)g_mpool; p<pCharEnd; p += block_size)
		{
			pTask = (struct fast_task_info *)p;
			pTask->size = min_buff_size;
			pTask->data = p + sizeof(struct fast_task_info);
		}

		g_free_queue.tail = (struct fast_task_info *) \
					(pCharEnd - block_size);

		for (p=(char *)g_mpool; p<(char *)g_free_queue.tail; \
			p += block_size)
		{
			pTask = (struct fast_task_info *)p;
			pTask->next = (struct fast_task_info *)(p + block_size);
		}
	}
	else
	{
		pEnd = g_mpool + max_connections;
		for (pTask=g_mpool; pTask<pEnd; pTask++)
		{
			pTask->size = min_buff_size;
			pTask->data = malloc(pTask->size);
			if (pTask->data == NULL)
			{
				task_queue_destroy();

				logError("file: "__FILE__", line: %d, " \
					"malloc %d bytes fail, " \
					"errno: %d, error info: %s", \
					__LINE__, pTask->size, \
					errno, strerror(errno));
				return errno != 0 ? errno : ENOMEM;
			}
		}

		g_free_queue.tail = pEnd - 1;
		for (pTask=g_mpool; pTask<g_free_queue.tail; pTask++)
		{
			pTask->next = pTask + 1;
		}
	}

	g_free_queue.max_connections = max_connections;
	g_free_queue.min_buff_size = min_buff_size;
	g_free_queue.max_buff_size = max_buff_size;
	g_free_queue.head = g_mpool;
	g_free_queue.tail->next = NULL;

	return 0;
}

void task_queue_destroy()
{
	if (g_mpool == NULL)
	{
		return;
	}

	if (!g_free_queue.malloc_whole_block)
	{
		struct fast_task_info *pTask;
		struct fast_task_info *pEnd;

		pEnd = g_mpool + g_free_queue.max_connections;
		for (pTask=g_mpool; pTask<pEnd; pTask++)
		{
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
	return _queue_pop_task(&g_free_queue);;
}

int free_queue_push(struct fast_task_info *pTask)
{
	char *new_buff;
	int result;

	pTask->length = 0;
	pTask->offset = 0;
	if (pTask->size > g_free_queue.min_buff_size) //need thrink
	{
		new_buff = (char *)malloc(g_free_queue.min_buff_size);
		if (new_buff == NULL)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", \
				__LINE__, g_free_queue.min_buff_size, \
				errno, strerror(errno));
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
			__LINE__, result, strerror(result));
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
			__LINE__, result, strerror(result));
	}

	return result;
}

int free_queue_count()
{
	return _task_queue_count(&g_free_queue);
}

static struct fast_task_info *_queue_pop_task(struct fast_task_queue *pQueue)
{
	struct fast_task_info *pTask;
	int result;

	if ((result=pthread_mutex_lock(&(pQueue->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
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
			__LINE__, result, strerror(result));
	}

	return pTask;
}

static int _task_queue_count(struct fast_task_queue *pQueue)
{
	struct fast_task_info *pTask;
	int count;
	int result;

	if ((result=pthread_mutex_lock(&(pQueue->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
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
			__LINE__, result, strerror(result));
	}

	return count;
}

