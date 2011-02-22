/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fast_mpool.h

#ifndef _TRUNK_MPOOL_H
#define _TRUNK_MPOOL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "common_define.h"
#include "chain.h"
 
struct fast_mpool_src
{
	struct fast_mpool_node *mpool;
	struct fast_mpool_src *next;
};

struct fast_mpool_man
{
	struct fast_mpool_node *head;
	struct fast_mpool_src *mpool_src_head;
	int element_size;
	int inc_elements_once;
	pthread_mutex_t lock;
};

struct fast_mpool_node
{
	struct fast_mpool_node *next;
	char data[0];
}

#ifdef __cplusplus
extern "C" {
#endif

int fast_mpool_init(struct fast_mpool_man *mpool, const int element_size, \
		const int int inc_elements_once);
void fast_mpool_destroy(struct fast_mpool_man *mpool);

struct fast_mpool_node *fast_mpool_alloc(struct fast_mpool_man *mpool);
void fast_mpool_free(struct fast_mpool_man *mpool, struct fast_mpool_node *pNode);

int fast_mpool_count(struct fast_mpool_man *mpool);

#ifdef __cplusplus
}
#endif

#endif

