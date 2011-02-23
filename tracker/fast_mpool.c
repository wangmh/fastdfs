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

	mpool->mpool_malloc_head = NULL;
	mpool->head = NULL;

	return 0;
}

static int fast_mpool_prealloc(struct fast_mpool_man *mpool)
{
	struct fast_mpool_node *pNode;
	struct fast_mpool_malloc *pMallocNode;
	char *pNew;
	char *pTrunkStart;
	char *p;
	char *pLast;
	int block_size;
	int alloc_size;
	int result;

	block_size = sizeof(struct fast_mpool_node) + mpool->element_size;
	alloc_size = sizeof(struct fast_mpool_malloc) + mpool->element_size * \
			mpool->inc_elements_once;

	pNew = (char *)malloc(alloc_size);
	if (pNew == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, alloc_size, errno, STRERROR(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	memset(pNew, 0, alloc_size);

	pMallocNode = (struct fast_mpool_malloc *)pNew;

	pTrunkStart = pNew + sizeof(struct fast_mpool_malloc);
	pLast = pNew + (alloc_size - block_size);
	for (p=pTrunkStart; p<pLast; p += block_size)
	{
		pNode = (struct fast_mpool_node *)p;
		pNode->next = (struct fast_mpool_node *)(p + block_size);
	}

	((struct fast_mpool_node *)pLast)->next = NULL;
	mpool->head = (struct fast_mpool_node *)pTrunkStart;

	pMallocNode->next = mpool->mpool_malloc_head;
	mpool->mpool_malloc_head = pMallocNode;

	return 0;
}

void fast_mpool_destroy(struct fast_mpool_man *mpool)
{
	struct fast_mpool_malloc *pMallocNode;
	struct fast_mpool_malloc *pMallocTmp;

	if (mpool->mpool_malloc_head == NULL)
	{
		return;
	}

	pMallocNode = mpool->mpool_malloc_head;
	while (pMallocNode != NULL)
	{
		pMallocTmp = pMallocNode;
		pMallocNode = pMallocNode->next;

		free(pMallocTmp);
	}
	mpool->mpool_malloc_head = NULL;
	mpool->head = NULL;

	pthread_mutex_destroy(&(mpool->lock));
}

struct fast_mpool_node *fast_mpool_alloc(struct fast_mpool_man *mpool)
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

	if (mpool->head != NULL)
	{
		pNode = mpool->head;
		mpool->head = pNode->next;
	}
	else
	{
		if ((result=fast_mpool_prealloc(mpool)) == 0)
		{
			pNode = mpool->head;
			mpool->head = pNode->next;
		}
		else
		{
			pNode = NULL;
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

int fast_mpool_free(struct fast_mpool_man *mpool, \
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

	pNode->next = mpool->head;
	mpool->head = pNode;

	if ((result=pthread_mutex_unlock(&(mpool->lock))) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
	}

	return 0;
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

