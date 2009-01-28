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
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include "fdfs_base64.h"
#include "sockopt.h"
#include "logger.h"
#include "hash.h"
#include "shared_func.h"
#include "ini_file_reader.h"
#include "fdht_types.h"
#include "fdht_proto.h"
#include "fdht_func.h"
#include "fdht_client.h"

GroupArray g_group_array = {NULL, 0};
bool g_keep_alive = false;

extern int g_network_timeout;
extern char g_base_path[MAX_PATH_SIZE];

int fdht_client_init(const char *filename)
{
	char *pBasePath;
	IniItemInfo *items;
	int nItemCount;
	int result;

	if ((result=iniLoadItems(filename, &items, &nItemCount)) != 0)
	{
		logError("load conf file \"%s\" fail, ret code: %d", \
			filename, result);
		return result;
	}

	//iniPrintItems(items, nItemCount);

	while (1)
	{
		pBasePath = iniGetStrValue("base_path", items, nItemCount);
		if (pBasePath == NULL)
		{
			logError("conf file \"%s\" must have item " \
				"\"base_path\"!", filename);
			result = ENOENT;
			break;
		}

		snprintf(g_base_path, sizeof(g_base_path), "%s", pBasePath);
		chopPath(g_base_path);
		if (!fileExists(g_base_path))
		{
			logError("\"%s\" can't be accessed, error info: %s", \
				g_base_path, strerror(errno));
			result = errno != 0 ? errno : ENOENT;
			break;
		}
		if (!isDir(g_base_path))
		{
			logError("\"%s\" is not a directory!", g_base_path);
			result = ENOTDIR;
			break;
		}

		g_network_timeout = iniGetIntValue("network_timeout", \
				items, nItemCount, DEFAULT_NETWORK_TIMEOUT);
		if (g_network_timeout <= 0)
		{
			g_network_timeout = DEFAULT_NETWORK_TIMEOUT;
		}

		g_keep_alive = iniGetBoolValue("keep_alive", \
				items, nItemCount, false);

		if ((result=fdht_load_groups(items, nItemCount, \
				&g_group_array)) != 0)
		{
			break;
		}

		load_log_level(items, nItemCount);

		logInfo("file: "__FILE__", line: %d, " \
			"base_path=%s, " \
			"network_timeout=%d, keep_alive=%d, "\
			"group_count=%d, server_count=%d", __LINE__, \
			g_base_path, g_network_timeout, g_keep_alive, \
			g_group_array.group_count, g_group_array.server_count);

		break;
	}

	iniFreeItems(items);

	return result;
}

void fdht_client_destroy()
{
	fdht_free_group_array(&g_group_array);
}

#define get_readable_connection(pServerArray, hash_code, err_no) \
	  get_connection(pServerArray, hash_code, err_no)

#define get_writable_connection(pServerArray, hash_code, err_no) \
	  get_connection(pServerArray, hash_code, err_no)

static FDHTServerInfo *get_connection(ServerArray *pServerArray, \
		const int hash_code, int *err_no)
{
	FDHTServerInfo **ppServer;
	FDHTServerInfo **ppEnd;
	int server_index;
	unsigned int new_hash_code;

	new_hash_code = (hash_code << 16) | (hash_code >> 16);
	server_index = new_hash_code % pServerArray->count;
	ppEnd = pServerArray->servers + pServerArray->count;
	for (ppServer = pServerArray->servers + server_index; \
		ppServer<ppEnd; ppServer++)
	{
		if ((*ppServer)->sock > 0)  //already connected
		{
			return *ppServer;
		}

		if (fdht_connect_server(*ppServer) == 0)
		{
			if (g_keep_alive)
			{
				tcpsetnodelay((*ppServer)->sock);
			}
			return *ppServer;
		}
	}

	ppEnd = pServerArray->servers + server_index;
	for (ppServer = pServerArray->servers; ppServer<ppEnd; ppServer++)
	{
		if ((*ppServer)->sock > 0)  //already connected
		{
			return *ppServer;
		}

		if (fdht_connect_server(*ppServer) == 0)
		{
			if (g_keep_alive)
			{
				tcpsetnodelay((*ppServer)->sock);
			}
			return *ppServer;
		}
	}

	*err_no = ENOENT;
	return NULL;
}

#define CALC_KEY_HASH_CODE(pKeyInfo, hash_key, hash_key_len, key_hash_code) \
	if (pKeyInfo->namespace_len > FDHT_MAX_NAMESPACE_LEN) \
	{ \
		fprintf(stderr, "namespace length: %d exceeds, " \
			"max length:  %d\n", \
			pKeyInfo->namespace_len, FDHT_MAX_NAMESPACE_LEN); \
		return EINVAL; \
	} \
 \
	if (pKeyInfo->obj_id_len > FDHT_MAX_OBJECT_ID_LEN) \
	{ \
		fprintf(stderr, "object ID length: %d exceeds, " \
			"max length: %d\n", \
			pKeyInfo->obj_id_len, FDHT_MAX_OBJECT_ID_LEN); \
		return EINVAL; \
	} \
 \
	if (pKeyInfo->key_len > FDHT_MAX_SUB_KEY_LEN) \
	{ \
		fprintf(stderr, "key length: %d exceeds, max length: %d\n", \
			pKeyInfo->key_len, FDHT_MAX_SUB_KEY_LEN); \
		return EINVAL; \
	} \
 \
	if (pKeyInfo->namespace_len == 0 && pKeyInfo->obj_id_len == 0) \
	{ \
		hash_key_len = pKeyInfo->key_len; \
		memcpy(hash_key, pKeyInfo->szKey, pKeyInfo->key_len); \
	} \
	else if (pKeyInfo->namespace_len > 0 && pKeyInfo->obj_id_len > 0) \
	{ \
		hash_key_len = pKeyInfo->namespace_len+1+pKeyInfo->obj_id_len; \
		memcpy(hash_key,pKeyInfo->szNameSpace,pKeyInfo->namespace_len);\
		*(hash_key + pKeyInfo->namespace_len)=FDHT_FULL_KEY_SEPERATOR; \
		memcpy(hash_key + pKeyInfo->namespace_len + 1, \
			pKeyInfo->szObjectId, pKeyInfo->obj_id_len); \
	} \
	else \
	{ \
		fprintf(stderr, "invalid namespace length: %d and " \
				"object ID length: %d\n", \
				pKeyInfo->namespace_len, pKeyInfo->obj_id_len); \
		return EINVAL; \
	} \
 \
	key_hash_code = PJWHash(hash_key, hash_key_len); \
	if (key_hash_code < 0) \
	{ \
		key_hash_code &= 0x7FFFFFFF; \
	} \

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_len:  4 bytes big endian integer
*       key:      key name
* response body format:
*       value_len:  4 bytes big endian integer
*       value:      value buff
*/
int fdht_get_ex1(FDHTKeyInfo *pKeyInfo, const time_t expires, \
		char **ppValue, int *value_len, MallocFunc malloc_func)
{
	int result;
	ProtoHeader *pHeader;
	char hash_key[FDHT_MAX_FULL_KEY_LEN + 1];
	char buff[sizeof(ProtoHeader) + FDHT_MAX_FULL_KEY_LEN + 16];
	int in_bytes;
	int vlen;
	int group_id;
	int hash_key_len;
	int key_hash_code;
	FDHTServerInfo *pServer;
	char *p;

	CALC_KEY_HASH_CODE(pKeyInfo, hash_key, hash_key_len, key_hash_code)
	group_id = ((unsigned int)key_hash_code) % g_group_array.group_count;
	pServer = get_readable_connection((g_group_array.groups + group_id), \
                	key_hash_code, &result);
	if (pServer == NULL)
	{
		return result;
	}

	//printf("get group_id=%d\n", group_id);

	memset(buff, 0, sizeof(buff));
	pHeader = (ProtoHeader *)buff;

	pHeader->cmd = FDHT_PROTO_CMD_GET;
	pHeader->keep_alive = g_keep_alive;
	int2buff((int)time(NULL), pHeader->timestamp);
	int2buff((int)expires, pHeader->expires);
	int2buff(key_hash_code, pHeader->key_hash_code);
	int2buff(12 + pKeyInfo->namespace_len + pKeyInfo->obj_id_len + \
		pKeyInfo->key_len, pHeader->pkg_len);

	while (1)
	{
		p = buff + sizeof(ProtoHeader);
		PACK_BODY_UNTIL_KEY(pKeyInfo, p)
		if ((result=tcpsenddata(pServer->sock, buff, p - buff, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if ((result=fdht_recv_header(pServer, &in_bytes)) != 0)
		{
			break;
		}

		if (in_bytes < 4)
		{
			logError("server %s:%d reponse bytes: %d < 4", \
				pServer->ip_addr, pServer->port, in_bytes);
			result = EINVAL;
			break;
		}

		if ((result=tcprecvdata(pServer->sock, buff, \
			4, g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"server: %s:%d, recv data fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pServer->ip_addr, \
				pServer->port, \
				result, strerror(result));
			break;
		}

		vlen = buff2int(buff);
		if (vlen != in_bytes - 4)
		{
			logError("server %s:%d reponse bytes: %d " \
				"is not correct, %d != %d", pServer->ip_addr, \
				pServer->port, in_bytes, vlen, in_bytes - 4);
			result = EINVAL;
			break;
		}

		if (*ppValue != NULL)
		{
			if (vlen >= *value_len)
			{
				*value_len = 0;
				result = ENOSPC;
				break;
			}

			*value_len = vlen;
		}
		else
		{
			*value_len = vlen;
			*ppValue = (char *)malloc_func((*value_len + 1));
			if (*ppValue == NULL)
			{
				*value_len = 0;
				logError("malloc %d bytes fail, " \
					"errno: %d, error info: %s", \
					*value_len + 1, errno, strerror(errno));
				result = errno != 0 ? errno : ENOMEM;
				break;
			}
		}

		if ((result=tcprecvdata(pServer->sock, *ppValue, \
			*value_len, g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"server: %s:%d, recv data fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pServer->ip_addr, \
				pServer->port, \
				result, strerror(result));
			break;
		}

		*(*ppValue + *value_len) = '\0';
		break;
	}

	if (g_keep_alive)
	{
		if (result >= ENETDOWN) //network error
		{
			fdht_disconnect_server(pServer);
		}
	}
	else
	{
		fdht_disconnect_server(pServer);
	}

	return result;
}

int fdht_set(FDHTKeyInfo *pKeyInfo, const time_t expires, \
		const char *pValue, const int value_len)
{
	int result;
	char hash_key[FDHT_MAX_FULL_KEY_LEN + 1];
	int group_id;
	int hash_key_len;
	int key_hash_code;
	FDHTServerInfo *pServer;

	CALC_KEY_HASH_CODE(pKeyInfo, hash_key, hash_key_len, key_hash_code)
	group_id = ((unsigned int)key_hash_code) % g_group_array.group_count;
	pServer = get_writable_connection((g_group_array.groups + group_id), \
                	key_hash_code, &result);
	if (pServer == NULL)
	{
		return result;
	}

	//printf("set group_id=%d\n", group_id);
	result = fdht_client_set(pServer, g_keep_alive, time(NULL), expires, \
			FDHT_PROTO_CMD_SET, key_hash_code, \
			pKeyInfo, pValue, value_len);

	if (g_keep_alive)
	{
		if (result >= ENETDOWN) //network error
		{
			fdht_disconnect_server(pServer);
		}
	}
	else
	{
		fdht_disconnect_server(pServer);
	}

	return result;
}

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_len:  4 bytes big endian integer
*       key:      key name
*       incr      4 bytes big endian integer
* response body format:
*      value_len: 4 bytes big endian integer
*      value :  value_len bytes
*/
int fdht_inc(FDHTKeyInfo *pKeyInfo, const time_t expires, const int increase, \
		char *pValue, int *value_len)
{
	int result;
	ProtoHeader *pHeader;
	char hash_key[FDHT_MAX_FULL_KEY_LEN + 1];
	char buff[FDHT_MAX_FULL_KEY_LEN + 32];
	char *in_buff;
	int in_bytes;
	int group_id;
	int hash_key_len;
	int key_hash_code;
	FDHTServerInfo *pServer;
	char *p;

	CALC_KEY_HASH_CODE(pKeyInfo, hash_key, hash_key_len, key_hash_code)
	group_id = ((unsigned int)key_hash_code) % g_group_array.group_count;
	pServer = get_writable_connection((g_group_array.groups + group_id), \
                	key_hash_code, &result);
	if (pServer == NULL)
	{
		return result;
	}

	//printf("inc group_id=%d\n", group_id);

	memset(buff, 0, sizeof(buff));
	pHeader = (ProtoHeader *)buff;

	pHeader->cmd = FDHT_PROTO_CMD_INC;
	pHeader->keep_alive = g_keep_alive;
	int2buff((int)time(NULL), pHeader->timestamp);
	int2buff((int)expires, pHeader->expires);
	int2buff(key_hash_code, pHeader->key_hash_code);
	int2buff(16 + pKeyInfo->namespace_len + pKeyInfo->obj_id_len + \
		pKeyInfo->key_len, pHeader->pkg_len);

	while (1)
	{
		p = buff + sizeof(ProtoHeader);
		PACK_BODY_UNTIL_KEY(pKeyInfo, p)
		int2buff(increase, p);
		p += 4;
		if ((result=tcpsenddata(pServer->sock, buff, p - buff, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		in_buff = buff;
		if ((result=fdht_recv_response(pServer, &in_buff, \
			sizeof(buff), &in_bytes)) != 0)
		{
			logError("recv data from server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if (in_bytes < 4)
		{
			logError("server %s:%d reponse bytes: %d < 4!", \
				pServer->ip_addr, pServer->port, in_bytes);
			result = EINVAL;
			break;
		}

		if (in_bytes - 4 >= *value_len)
		{
			*value_len = 0;
			result = ENOSPC;
			break;
		}

		*value_len = in_bytes - 4;
		memcpy(pValue, in_buff + 4, *value_len);
		*(pValue + (*value_len)) = '\0';
		break;
	}

	if (g_keep_alive)
	{
		if (result >= ENETDOWN) //network error
		{
			fdht_disconnect_server(pServer);
		}
	}
	else
	{
		fdht_disconnect_server(pServer);
	}

	return result;
}

int fdht_delete(FDHTKeyInfo *pKeyInfo)
{
	int result;
	FDHTServerInfo *pServer;
	char hash_key[FDHT_MAX_FULL_KEY_LEN + 1];
	int group_id;
	int hash_key_len;
	int key_hash_code;

	CALC_KEY_HASH_CODE(pKeyInfo, hash_key, hash_key_len, key_hash_code)
	group_id = ((unsigned int)key_hash_code) % g_group_array.group_count;
	pServer = get_writable_connection((g_group_array.groups + group_id), \
                	key_hash_code , &result);
	if (pServer == NULL)
	{
		return result;
	}

	//printf("del group_id=%d\n", group_id);
	result = fdht_client_delete(pServer, g_keep_alive, time(NULL), \
			FDHT_PROTO_CMD_DEL, key_hash_code, pKeyInfo);

	if (g_keep_alive)
	{
		if (result >= ENETDOWN) //network error
		{
			fdht_disconnect_server(pServer);
		}
	}
	else
	{
		fdht_disconnect_server(pServer);
	}

	return result;
}

