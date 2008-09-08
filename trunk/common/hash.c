/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include "hash.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

static unsigned int prime_array[] = {
    1,              /* 0 */
    3,              /* 1 */
    17,             /* 2 */
    37,             /* 3 */
    79,             /* 4 */
    163,            /* 5 */
    331,            /* 6 */
    673,            /* 7 */
    1361,           /* 8 */
    2729,           /* 9 */
    5471,           /* 10 */
    10949,          /* 11 */
    21911,          /* 12 */
    43853,          /* 13 */
    87719,          /* 14 */
    175447,         /* 15 */
    350899,         /* 16 */
    701819,         /* 17 */
    1403641,        /* 18 */
    2807303,        /* 19 */
    5614657,        /* 20 */
    11229331,       /* 21 */
    22458671,       /* 22 */
    44917381,       /* 23 */
    89834777,       /* 24 */
    179669557,      /* 25 */
    359339171,      /* 26 */
    718678369,      /* 27 */
    1437356741,     /* 28 */
    2147483647      /* 29 (largest signed int prime) */
};

#define PRIME_ARRAY_SIZE  30

int _hash_alloc_buckets(HashArray *pHash)
{
	ChainList *plist;
	ChainList *list_end;

	pHash->items=(ChainList *)malloc(sizeof(ChainList)*(*pHash->capacity));
	if (pHash->items == NULL)
	{
		return ENOMEM;
	}

	list_end = pHash->items + (*pHash->capacity);
	for (plist=pHash->items; plist!=list_end; plist++)
	{
		chain_init(plist, CHAIN_TYPE_APPEND, free, NULL);
	}

	return 0;
}

int hash_init(HashArray *pHash, HashFunc hash_func, \
		const unsigned int capacity, const double load_factor)
{
	unsigned int *pprime;
	unsigned int *prime_end;
	int result;

	if (pHash == NULL || hash_func == NULL)
	{
		return EINVAL;
	}

	memset(pHash, 0, sizeof(HashArray));
	prime_end = prime_array + PRIME_ARRAY_SIZE;
	for (pprime = prime_array; pprime!=prime_end; pprime++)
	{
		if (*pprime > capacity)
		{
			pHash->capacity = pprime;
			break;
		}
	}

	if (pHash->capacity == NULL)
	{
		return EINVAL;
	}

	if ((result=_hash_alloc_buckets(pHash)) != 0)
	{
		return result;
	}

	pHash->hash_func = hash_func;

	if (load_factor >= 0.10 && load_factor <= 1.00)
	{
		pHash->load_factor = load_factor;
	}
	else
	{
		pHash->load_factor = 0.50;
	}

	return 0;
}

void hash_destroy(HashArray *pHash)
{
	ChainList *plist;
	ChainList *list_end;

	if (pHash == NULL || pHash->items == NULL)
	{
		return;
	}

	list_end = pHash->items + (*pHash->capacity);
	for (plist=pHash->items; plist!=list_end; plist++)
	{
		chain_destroy(plist);
	}

	free(pHash->items);
	pHash->items = NULL;
	if (pHash->is_malloc_capacity)
	{
		free(pHash->capacity);
		pHash->capacity = NULL;
		pHash->is_malloc_capacity = false;
	}
}

void hash_stat_print(HashArray *pHash)
{
#define STAT_MAX_NUM  17
	ChainList *plist;
	ChainList *list_end;
	int totalLength;
	//ChainNode *pnode;
	//HashData *hash_data;
	int stats[STAT_MAX_NUM];
	int last;
	int index;
	int i;
	int max_length;
	int list_count;
	int bucket_used;

	if (pHash == NULL || pHash->items == NULL)
	{
		return;
	}

	memset(stats, 0, sizeof(stats));
	last = STAT_MAX_NUM - 1;
	list_end = pHash->items + (*pHash->capacity);
	max_length = 0;
	bucket_used = 0;
	for (plist=pHash->items; plist!=list_end; plist++)
	{
		list_count = chain_count(plist);
		if (list_count == 0)
		{
			continue;
		}

		bucket_used++;
		index = list_count - 1;
		if (index > last)
		{
			index = last;
		}
		stats[index]++;

		if (list_count > max_length)
		{
			max_length = list_count;
		}

		/*
		pnode = plist->head;
		while (pnode != NULL)
		{
			pnode = pnode->next;
		}
		*/
	}

	/*
	printf("collision stat:\n");
	for (i=0; i<last; i++)
	{
		if (stats[i] > 0) printf("%d: %d\n", i+1, stats[i]);
	}
	if (stats[i] > 0) printf(">=%d: %d\n", i+1, stats[i]);
	*/

	totalLength = 0;
	for (i=0; i<STAT_MAX_NUM; i++)
	{
		if (stats[i] > 0) totalLength += (i+1) * stats[i];
	}
	printf("capacity: %d, item_count=%d, bucket_used: %d, " \
		"avg length: %.4f, max length: %d, bucket / item = %.2f%%\n", 
               *pHash->capacity, pHash->item_count, bucket_used,
               bucket_used > 0 ? (double)totalLength / (double)bucket_used:0.00,
               max_length, (double)bucket_used*100.00/(double)*pHash->capacity);
}

static int _rehash1(HashArray *pHash, const int old_capacity, \
		unsigned int *new_capacity)
{
	ChainList *old_items;
	ChainList *plist;
	ChainList *list_end;
	ChainNode *pnode;
	HashData *hash_data;
	ChainList *pNewList;
	int result;

	old_items = pHash->items;
	pHash->capacity = new_capacity;
	if ((result=_hash_alloc_buckets(pHash)) != 0)
	{
		pHash->items = old_items;
		return result;
	}

	//printf("old: %d, new: %d\n", old_capacity, *pHash->capacity);
	list_end = old_items + old_capacity;
	for (plist=old_items; plist!=list_end; plist++)
	{
		pnode = plist->head;
		while (pnode != NULL)
		{
			hash_data = (HashData *) pnode->data;
			pNewList = pHash->items + (hash_data->hash_code % \
				(*pHash->capacity));

			//success to add node
			if (addNode(pNewList, hash_data) == 0)
			{
			}

			pnode = pnode->next;
		}

		plist->freeDataFunc = NULL;
		chain_destroy(plist);
	}

	free(old_items);
	return 0;
}

static int _rehash(HashArray *pHash)
{
	int result;
	unsigned int *pOldCapacity;

	pOldCapacity = pHash->capacity;
	if (pHash->is_malloc_capacity)
	{
		unsigned int *pprime;
		unsigned int *prime_end;

		pHash->capacity = NULL;

		prime_end = prime_array + PRIME_ARRAY_SIZE;
		for (pprime = prime_array; pprime!=prime_end; pprime++)
		{
			if (*pprime > *pOldCapacity)
			{
				pHash->capacity = pprime;
				break;
			}
		}
	}
	else
	{
		pHash->capacity++;
	}

	if ((result=_rehash1(pHash, *pOldCapacity, pHash->capacity)) != 0)
	{
		pHash->capacity = pOldCapacity;  //rollback
	}
	else
	{
		if (pHash->is_malloc_capacity)
		{
			free(pOldCapacity);
			pHash->is_malloc_capacity = false;
		}
	}

	/*printf("rehash, old_capacity=%d, new_capacity=%d\n", \
		old_capacity, *pHash->capacity);
	*/
	return result;
}

int _hash_conflict_count(HashArray *pHash)
{
	ChainList *plist;
	ChainList *list_end;
	ChainNode *pnode;
	ChainNode *pSubNode;
	int conflicted;
	int conflict_count;

	if (pHash == NULL || pHash->items == NULL)
	{
		return 0;
	}

	list_end = pHash->items + (*pHash->capacity);
	conflict_count = 0;
	for (plist=pHash->items; plist!=list_end; plist++)
	{
		if (plist->head == NULL || plist->head->next == NULL)
		{
			continue;
		}

		conflicted = 0;
		pnode = plist->head;
		while (pnode != NULL)
		{
			pSubNode = pnode->next;
			while (pSubNode != NULL)
			{
				if (((HashData *)pnode->data)->hash_code != \
					((HashData *)pSubNode->data)->hash_code)
				{
					conflicted = 1;
					break;
				}

				pSubNode = pSubNode->next;
			}

			if (conflicted)
			{
				break;
			}

			pnode = pnode->next;
		}

		conflict_count += conflicted;
	}

	return conflict_count;
}

int hash_best_op(HashArray *pHash, const int suggest_capacity)
{
	int old_capacity;
	int conflict_count;
	unsigned int *new_capacity;
	int result;

	if ((conflict_count=_hash_conflict_count(pHash)) == 0)
	{
		return 0;
	}

	old_capacity = *pHash->capacity;
	new_capacity = (unsigned int *)malloc(sizeof(unsigned int));
	if (new_capacity == NULL)
	{
		return -ENOMEM;
	}

	if ((suggest_capacity > 2) && (suggest_capacity >= pHash->item_count))
	{
		*new_capacity = suggest_capacity - 2;
		if (*new_capacity % 2 == 0)
		{
			++(*new_capacity);
		}
	}
	else
	{
		*new_capacity = 2 * (pHash->item_count - 1) + 1;
	}

	do
	{
		do
		{
			*new_capacity += 2;
		} while ((*new_capacity % 3 == 0) || (*new_capacity % 5 == 0) \
			 || (*new_capacity % 7 == 0));

		if ((result=_rehash1(pHash, old_capacity, new_capacity)) != 0)
		{
			pHash->is_malloc_capacity = \
					(pHash->capacity == new_capacity);
			*pHash->capacity = old_capacity;
			return -1 * result;
		}

		old_capacity = *new_capacity;
		/*printf("rehash, conflict_count=%d, old_capacity=%d, " \
			"new_capacity=%d\n", conflict_count, \
			old_capacity, *new_capacity);
		*/
	} while ((conflict_count=_hash_conflict_count(pHash)) > 0);

	pHash->is_malloc_capacity = true;

	//hash_stat_print(pHash);
	return 1;
}

HashData *_chain_find_entry(ChainList *plist, const void *key, \
		const int key_len, const unsigned int hash_code)
{
	ChainNode *pnode;
	HashData *hash_data;

	pnode = plist->head;
	while (pnode != NULL)
	{
		hash_data = (HashData *)pnode->data;
		if (key_len == hash_data->key_len && \
			memcmp(key, hash_data->key, key_len) == 0)
		{
			return hash_data;
		}

		pnode = pnode->next;
	}

	return NULL;
}

void *hash_find(HashArray *pHash, const void *key, const int key_len)
{
	unsigned int hash_code;
	ChainList *plist;
	HashData *hash_data;

	if (pHash == NULL || key == NULL || key_len < 0)
	{
		return NULL;
	}

	hash_code = pHash->hash_func(key, key_len);
	plist = pHash->items + (hash_code % (*pHash->capacity));

	hash_data = _chain_find_entry(plist, key, key_len, hash_code);
	if (hash_data != NULL)
	{
		return hash_data->value;
	}
	else
	{
		return NULL;
	}
}

int hash_insert(HashArray *pHash, const void *key, const int key_len, \
		void *value)
{
	unsigned int hash_code;
	ChainList *plist;
	HashData *hash_data;
	char *pBuff;
	int result;

	if (pHash == NULL || key == NULL || key_len < 0)
	{
		return -EINVAL;
	}

	hash_code = pHash->hash_func(key, key_len);
	plist = pHash->items + (hash_code % (*pHash->capacity));
	hash_data = _chain_find_entry(plist, key, key_len, hash_code);
	if (hash_data != NULL)
	{
		hash_data->value = value;
		return 0; 
	}

	pBuff = (char *)malloc(sizeof(HashData) + key_len);
	if (pBuff == NULL)
	{
		return -ENOMEM;
	}

	hash_data = (HashData *)pBuff;
	hash_data->key = pBuff + sizeof(HashData);

	hash_data->key_len = key_len;
	memcpy(hash_data->key, key, key_len);
	hash_data->hash_code = hash_code;
	hash_data->value = value;

	if ((result=addNode(plist, hash_data)) != 0) //fail to add node
	{
		free(hash_data);
		return -1 * result;
	}

	pHash->item_count++;

	if ((double)pHash->item_count / (double)*pHash->capacity >= \
		pHash->load_factor)
	{
		_rehash(pHash);
	}

	return 1;
}

int hash_delete(HashArray *pHash, const void *key, const int key_len)
{
	unsigned int hash_code;
	ChainList *plist;
	ChainNode *previous;
	ChainNode *pnode;
	HashData *hash_data;

	if (pHash == NULL || key == NULL || key_len < 0)
	{
		return -EINVAL;
	}

	hash_code = pHash->hash_func(key, key_len);
	plist = pHash->items + (hash_code % (*pHash->capacity));

	previous = NULL;
	pnode = plist->head;
	while (pnode != NULL)
	{
		hash_data = (HashData *)pnode->data;
		if (key_len == hash_data->key_len && \
			memcmp(key, hash_data->key, key_len) == 0)
		{
			deleteNodeEx(plist, previous, pnode);
			pHash->item_count--;
			return 1;
		}

		previous = pnode;
		pnode = pnode->next;
	}

	return 0;
}

void hash_walk(HashArray *pHash, HashWalkFunc walkFunc, void *args)
{
	ChainList *plist;
	ChainList *list_end;
	ChainNode *pnode;
	HashData *hash_data;
	int index;

	if (pHash == NULL || pHash->items == NULL || walkFunc == NULL)
	{
		return;
	}

	index = 0;
	list_end = pHash->items + (*pHash->capacity);
	for (plist=pHash->items; plist!=list_end; plist++)
	{
		pnode = plist->head;
		while (pnode != NULL)
		{
			hash_data = (HashData *) pnode->data;
			walkFunc(index, hash_data, args);

			pnode = pnode->next;
			index++;
		}
	}
}

// RS Hash Function
unsigned int RSHash(const void *key, const int key_len)
{
    unsigned char *pKey;
    unsigned char *pEnd;
    unsigned int b = 378551;
    unsigned int a = 63689;
    unsigned int hash = 0;

    pEnd = (unsigned char *)key + key_len;
    for (pKey = (unsigned char *)key; pKey != pEnd; pKey++)
    {
        hash = hash * a + (*pKey);
        a *= b;
    }

    return hash;
} 
 
// JS Hash Function
unsigned int JSHash(const void *key, const int key_len)
{
    unsigned char *pKey;
    unsigned char *pEnd;
    unsigned int hash = 1315423911;

    pEnd = (unsigned char *)key + key_len;
    for (pKey = (unsigned char *)key; pKey != pEnd; pKey++)
    {
        hash ^= ((hash << 5) + (*pKey) + (hash >> 2));
    }

    return hash;
}
 
// P.J.Weinberger Hash Function
unsigned int PJWHash(const void *key, const int key_len)
{
    unsigned char *pKey;
    unsigned char *pEnd;
    unsigned int BitsInUnignedInt = (unsigned int)(sizeof(unsigned int) * 8);
    unsigned int ThreeQuarters    = (unsigned int)((BitsInUnignedInt * 3) / 4);
    unsigned int OneEighth        = (unsigned int)(BitsInUnignedInt / 8);

    unsigned int HighBits         = (unsigned int)(0xFFFFFFFF) << \
				(BitsInUnignedInt - OneEighth);
    unsigned int hash             = 0;
    unsigned int test             = 0;

    pEnd = (unsigned char *)key + key_len;
    for (pKey = (unsigned char *)key; pKey != pEnd; pKey++)
    {
        hash = (hash << OneEighth) + (*(pKey));
        if ((test = hash & HighBits) != 0)
        {
            hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
        }
    }

    return hash;
} 
 
// ELF Hash Function
unsigned int ELFHash(const void *key, const int key_len)
{
    unsigned char *pKey;
    unsigned char *pEnd;
    unsigned int hash = 0;
    unsigned int x    = 0;

    pEnd = (unsigned char *)key + key_len;
    for (pKey = (unsigned char *)key; pKey != pEnd; pKey++)
    {
        hash = (hash << 4) + (*pKey);
        if ((x = hash & 0xF0000000L) != 0)
        {
            hash ^= (x >> 24);
            hash &= ~x;
        }
    }

    return hash;
}

// BKDR Hash Function
unsigned int BKDRHash(const void *key, const int key_len)
{
    unsigned char *pKey;
    unsigned char *pEnd;
    unsigned int seed = 131;  //  31 131 1313 13131 131313 etc..
    unsigned int hash = 0;

    pEnd = (unsigned char *)key + key_len;
    for (pKey = (unsigned char *)key; pKey != pEnd; pKey++)
    {
        hash = hash * seed + (*pKey);
    }

    return hash;
}

// SDBM Hash Function
unsigned int SDBMHash(const void *key, const int key_len)
{
    unsigned char *pKey;
    unsigned char *pEnd;
    unsigned int hash = 0;

    pEnd = (unsigned char *)key + key_len;
    for (pKey = (unsigned char *)key; pKey != pEnd; pKey++)
    {
        hash = (*pKey) + (hash << 6) + (hash << 16) - hash;
    }

    return hash;
}

unsigned int Time33Hash(const void *key, const int key_len)
{
	unsigned int nHash;
	unsigned char *pKey;
	unsigned char *pEnd;

	nHash = 0;
	pEnd = (unsigned char *)key + key_len;
	for (pKey = (unsigned char *)key; pKey != pEnd; pKey++)
	{
		nHash = (nHash << 5) + nHash + (*pKey);
	}

	return nHash;
}

// DJB Hash Function
unsigned int DJBHash(const void *key, const int key_len)
{
    unsigned char *pKey;
    unsigned char *pEnd;
    unsigned int hash = 5381;

    pEnd = (unsigned char *)key + key_len;
    for (pKey = (unsigned char *)key; pKey != pEnd; pKey++)
    {
        hash += (hash << 5) + (*pKey);
    }

    return hash;
}

// AP Hash Function
unsigned int APHash(const void *key, const int key_len)
{
    unsigned char *pKey;
    unsigned char *pEnd;
    int i;
    unsigned int hash = 0;

    pEnd = (unsigned char *)key + key_len;
    for (pKey = (unsigned char *)key, i=0; pKey != pEnd; pKey++, i++)
    {
        if ((i & 1) == 0)
        {
            hash ^= ((hash << 7) ^ (*pKey) ^ (hash >> 3));
        }
        else
        {
            hash ^= (~((hash << 11) ^ (*pKey) ^ (hash >> 5)));
        }
    }

    return hash;
}

unsigned int calc_hashnr (const void* key, const int key_len)
{
  unsigned char *pKey;
  unsigned char *pEnd;
  unsigned int nr = 1, nr2 = 4;

  pEnd = (unsigned char *)key + key_len;
  for (pKey = (unsigned char *)key; pKey != pEnd; pKey++)
  {
      nr ^= (((nr & 63) + nr2) * (*pKey)) + (nr << 8);
      nr2 += 3;
  }

  return nr;
}

unsigned int calc_hashnr1(const void* key, const int key_len)
{
  unsigned char *pKey;
  unsigned char *pEnd;
  uint hash = 0;

  pEnd = (unsigned char *)key + key_len;
  for (pKey = (unsigned char *)key; pKey != pEnd; pKey++)
    {
      hash *= 16777619;
      hash ^= *pKey;
    }
  return hash;
}

unsigned int simple_hash(const void* key, const int key_len)
{
  unsigned int h;
  unsigned char *p;
  unsigned char *pEnd;

  h = 0;
  pEnd = (unsigned char *)key + key_len;
  for (p = (unsigned char *)key; p!= pEnd; p++)
  {
    h = 31 * h + *p;
  }

  return h;
}
