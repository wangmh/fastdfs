//fast_mpool.c

#include <errno.h>
#include <sys/resource.h>
#include <pthread.h>
#include "fast_mpool.h"
#include "logger.h"
#include "shared_func.h"
#include "pthread_func.h"

int fast_mpool_init(struct fast_mpool_man *mpool, const int element_size, \
		const int int inc_elements_once)
{
	int result;

	if (element_size <= 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"invalid block size: %d", \
			__LINE__, element_size);
		return EINVAL;
	}

	mpool->element_size = element_size;
	if (inc_elements_once > 0)
	{
		mpool->inc_elements_once = inc_elements_once;
	}
	else
	{
		int block_size;
		block_size = sizeof(struct fast_mpool_node) + element_size;
		mpool->inc_elements_once = (1024 * 1024) / block_size;
		
	}

	if ((result=init_pthread_lock(&(mpool->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	mpool->mpool_src_head = NULL;
	mpool->head = NULL;

	return 0;
}

static int fast_mpool_prealloc(struct fast_mpool_man *mpool)
{
	struct fast_mpool_node *pNode;
	char *p;
	char *pCharEnd;
	int block_size;
	int alloc_size;
	int64_t total_size;
	int result;

	block_size = sizeof(struct fast_mpool_node) + mpool->element_size;
	alloc_size = element_size * mpool->inc_elements_once;

	mpool_src_head = (struct fast_mpool_node *)malloc(total_size);
	if (mpool_src_head == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc "INT64_PRINTF_FORMAT" bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, total_size, errno, STRERROR(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	memset(mpool_src_head, 0, total_size);

	pCharEnd = ((char *)mpool_src_head) + total_size;
	for (p=(char *)mpool_src_head; p<pCharEnd; p += element_size)
	{
		pNode = (struct fast_mpool_node *)p;
		pNode->size = min_buff_size;

		pNode->arg = p + sizeof(void);
		if (g_fast_1mpool.malloc_whole_block)
		{
			pNode->data = (char *)pNode->arg + arg_size;
		}
		else
		{
			pNode->data = (char *)malloc(pNode->size);
			if (pNode->data == NULL)
			{
				fast_mpool_destroy();

				logError("file: "__FILE__", line: %d, " \
					"malloc %d bytes fail, " \
					"errno: %d, error info: %s", \
					__LINE__, pNode->size, \
					errno, STRERROR(errno));
				return errno != 0 ? errno : ENOMEM;
			}
		}
	}

	g_fast_1mpool.tail = (struct fast_mpool_node *)(pCharEnd - element_size);
	for (p=(char *)mpool_src_head; p<(char *)g_fast_1mpool.tail; p += element_size)
	{
		pNode = (struct fast_mpool_node *)p;
		pNode->next = (struct fast_mpool_node *)(p + element_size);
	}

	g_fast_1mpool.max_connections = max_connections;
	g_fast_1mpool.min_buff_size = min_buff_size;
	g_fast_1mpool.max_buff_size = max_buff_size;
	g_fast_1mpool.arg_size = arg_size;
	g_fast_1mpool.head = mpool_src_head;
	g_fast_1mpool.tail->next = NULL;

	return 0;
}

void fast_mpool_destroy()
{
	if (mpool_src_head == NULL)
	{
		return;
	}

	if (!g_fast_1mpool.malloc_whole_block)
	{
		char *p;
		char *pCharEnd;
		int element_size;
		struct fast_mpool_node *pNode;

		element_size = sizeof(void) + \
					g_fast_1mpool.arg_size;
		pCharEnd = ((char *)mpool_src_head) + element_size * \
				g_fast_1mpool.max_connections;
		for (p=(char *)mpool_src_head; p<pCharEnd; p += element_size)
		{
			pNode = (struct fast_mpool_node *)p;
			if (pNode->data != NULL)
			{
				free(pNode->data);
				pNode->data = NULL;
			}
		}
	}

	free(mpool_src_head);
	mpool_src_head = NULL;

	pthread_mutex_destroy(&(g_fast_1mpool.lock));
}

struct fast_mpool_node *fast_mpool_alloc()
{
	return fast_mpool_pop(&g_fast_1mpool);;
}

void fast_mpool_free(struct fast_mpool_man *mpool, struct fast_mpool_node *pNode)
{
	char *new_buff;
	int result;

	*(pNode->client_ip) = '\0';
	pNode->length = 0;
	pNode->offset = 0;
	pNode->req_count = 0;

	if (pNode->size > g_fast_1mpool.min_buff_size) //need thrink
	{
		new_buff = (char *)malloc(g_fast_1mpool.min_buff_size);
		if (new_buff == NULL)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", \
				__LINE__, g_fast_1mpool.min_buff_size, \
				errno, STRERROR(errno));
		}
		else
		{
			free(pNode->data);
			pNode->size = g_fast_1mpool.min_buff_size;
			pNode->data = new_buff;
		}
	}

	if ((result=pthread_mutex_lock(&g_fast_1mpool.lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	pNode->next = g_fast_1mpool.head;
	g_fast_1mpool.head = pNode;
	if (g_fast_1mpool.tail == NULL)
	{
		g_fast_1mpool.tail = pNode;
	}

	if ((result=pthread_mutex_unlock(&g_fast_1mpool.lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	return result;
}

int fast_mpool_count()
{
	return fast_mpool_count(&g_fast_1mpool);
}

int fast_mpool_push(struct fast_mpool_man *mpool, \
		struct fast_mpool_node *pNode)
{
	int result;

	if ((result=pthread_mutex_lock(&(mpool->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	pNode->next = NULL;
	if (mpool->tail == NULL)
	{
		mpool->head = pNode;
	}
	else
	{
		mpool->tail->next = pNode;
	}
	mpool->tail = pNode;

	if ((result=pthread_mutex_unlock(&(mpool->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	return 0;
}

struct fast_mpool_node *fast_mpool_pop(struct fast_mpool_man *mpool)
{
	struct fast_mpool_node *pNode;
	int result;

	if ((result=pthread_mutex_lock(&(mpool->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return NULL;
	}

	pNode = mpool->head;
	if (pNode != NULL)
	{
		mpool->head = pNode->next;
		if (mpool->head == NULL)
		{
			mpool->tail = NULL;
		}
	}

	if ((result=pthread_mutex_unlock(&(mpool->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	return pNode;
}

int fast_mpool_count(struct fast_mpool_man *mpool)
{
	struct fast_mpool_node *pNode;
	int count;
	int result;

	if ((result=pthread_mutex_lock(&(mpool->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return 0;
	}

	count = 0;
	pNode = mpool->head;
	while (pNode != NULL)
	{
		pNode = pNode->next;
		count++;
	}

	if ((result=pthread_mutex_unlock(&(mpool->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	return count;
}

