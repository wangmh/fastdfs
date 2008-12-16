/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#ifndef _HASH_H_
#define _HASH_H_

#include "chain.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CRC32_XINIT 0xFFFFFFFF		/* initial value */
#define CRC32_XOROT 0xFFFFFFFF		/* final xor value */

typedef unsigned int (*HashFunc) (const void *key, const int key_len);

typedef struct tagHashArray
{
	ChainList *items;
	HashFunc hash_func;
	int item_count;
	unsigned int *capacity;
	double load_factor;
	bool is_malloc_capacity;
} HashArray;

typedef struct tagHashData
{
	void *key;
	int key_len;
	void *value;
	unsigned int hash_code;
} HashData;

typedef void (*HashWalkFunc)(const int index, const HashData *data, void *args);

int hash_init(HashArray *pHash, HashFunc hash_func, \
		const unsigned int capacity, const double load_factor);
void hash_destroy(HashArray *pHash);
int hash_insert(HashArray *pHash, const void *key, const int key_len, \
		void *value);
void *hash_find(HashArray *pHash, const void *key, const int key_len);
int hash_delete(HashArray *pHash, const void *key, const int key_len);
void hash_walk(HashArray *pHash, HashWalkFunc walkFunc, void *args);
int hash_best_op(HashArray *pHash, const int suggest_capacity);
void hash_stat_print(HashArray *pHash);

unsigned int RSHash(const void *key, const int key_len);

unsigned int JSHash(const void *key, const int key_len);
unsigned int JSHash_ex(const void *key, const int key_len, \
	const unsigned int init_value);

unsigned int PJWHash(const void *key, const int key_len);
unsigned int PJWHash_ex(const void *key, const int key_len, \
	const unsigned int init_value);

unsigned int ELFHash(const void *key, const int key_len);
unsigned int ELFHash_ex(const void *key, const int key_len, \
	const unsigned int init_value);

unsigned int BKDRHash(const void *key, const int key_len);
unsigned int BKDRHash_ex(const void *key, const int key_len, \
	const unsigned int init_value);

unsigned int SDBMHash(const void *key, const int key_len);
unsigned int SDBMHash_ex(const void *key, const int key_len, \
	const unsigned int init_value);

unsigned int Time33Hash(const void *key, const int key_len);
unsigned int Time33Hash_ex(const void *key, const int key_len, \
	const unsigned int init_value);

unsigned int DJBHash(const void *key, const int key_len);
unsigned int DJBHash_ex(const void *key, const int key_len, \
	const unsigned int init_value);

unsigned int APHash(const void *key, const int key_len);
unsigned int APHash_ex(const void *key, const int key_len, \
	const unsigned int init_value);

unsigned int calc_hashnr (const void* key, const int key_len);

unsigned int calc_hashnr1(const void* key, const int key_len);
unsigned int calc_hashnr1_ex(const void* key, const int key_len, \
	const unsigned int init_value);

unsigned int simple_hash(const void* key, const int key_len);
unsigned int simple_hash_ex(const void* key, const int key_len, \
	const unsigned int init_value);

unsigned int CRC32(void *key, const int key_len);
unsigned int CRC32_ex(void *key, const int key_len, \
	const unsigned int init_value);
#define CRC32_FINAL(crc)  (crc ^ CRC32_XOROT)


#define INIT_HASH_CODES4(hash_codes) \
	hash_codes[0] = CRC32_XINIT; \
	hash_codes[1] = 0; \
	hash_codes[2] = 0; \
	hash_codes[3] = 0; \


#define CALC_HASH_CODES4(buff, buff_len, hash_codes) \
	hash_codes[0] = CRC32_ex(buff, buff_len, hash_codes[0]); \
	hash_codes[1] = ELFHash_ex(buff, buff_len, hash_codes[1]); \
	hash_codes[2] = APHash_ex(buff, buff_len, hash_codes[2]); \
	hash_codes[3] = Time33Hash_ex(buff, buff_len, hash_codes[3]); \


#define FINISH_HASH_CODES4(hash_codes) \
	hash_codes[0] = CRC32_FINAL(hash_codes[0]); \


#ifdef __cplusplus
}
#endif

#endif

