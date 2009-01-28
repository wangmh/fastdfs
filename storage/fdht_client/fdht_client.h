/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdht_client.h

#ifndef _FDHT_CLIENT_H
#define _FDHT_CLIENT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fdht_define.h"
#include "fdht_types.h"
#include "fdht_proto.h"
#include "fdht_func.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* (*MallocFunc)(size_t size);

extern GroupArray g_group_array;
extern bool g_keep_alive;

int fdht_client_init(const char *filename);
void fdht_client_destroy();

#define fdht_get(pKeyInfo, ppValue, value_len) \
	fdht_get_ex1((&g_group_array), g_keep_alive, pKeyInfo, \
		FDHT_EXPIRES_NONE, ppValue, value_len, malloc)

#define fdht_get_ex(pKeyInfo, expires, ppValue, value_len) \
	fdht_get_ex1((&g_group_array), g_keep_alive, pKeyInfo, expires, \
			ppValue, value_len, malloc)

#define fdht_set(pKeyInfo, expires, pValue, value_len) \
	fdht_set_ex((&g_group_array), g_keep_alive, pKeyInfo, expires, \
		pValue, value_len)

#define fdht_inc(pKeyInfo, expires, increase, pValue, value_len) \
	fdht_inc_ex((&g_group_array), g_keep_alive, pKeyInfo, expires, \
		increase, pValue, value_len)

#define fdht_delete(pKeyInfo) \
	fdht_delete_ex((&g_group_array), g_keep_alive, pKeyInfo)


int fdht_get_ex1(GroupArray *pGroupArray, const bool bKeepAlive, \
		FDHTKeyInfo *pKeyInfo, const time_t expires, \
		char **ppValue, int *value_len, MallocFunc malloc_func);

int fdht_set_ex(GroupArray *pGroupArray, const bool bKeepAlive, \
		FDHTKeyInfo *pKeyInfo, const time_t expires, \
		const char *pValue, const int value_len);

int fdht_inc_ex(GroupArray *pGroupArray, const bool bKeepAlive, \
		FDHTKeyInfo *pKeyInfo, const time_t expires, \
		const int increase, char *pValue, int *value_len);

int fdht_delete_ex(GroupArray *pGroupArray, const bool bKeepAlive, \
		FDHTKeyInfo *pKeyInfo);

#ifdef __cplusplus
}
#endif

#endif

