/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdht_proto.h

#ifndef _FDHT_PROTO_H_
#define _FDHT_PROTO_H_

#include "fdht_types.h"

#define FDHT_PROTO_CMD_QUIT	10

#define FDHT_PROTO_CMD_SET	11
#define FDHT_PROTO_CMD_INC	12
#define FDHT_PROTO_CMD_GET	13
#define FDHT_PROTO_CMD_DEL	14

#define FDHT_PROTO_CMD_SYNC_REQ	   21
#define FDHT_PROTO_CMD_SYNC_NOTIFY 22  //sync done notify
#define FDHT_PROTO_CMD_SYNC_SET	   23
#define FDHT_PROTO_CMD_SYNC_DEL	   24

#define FDHT_PROTO_CMD_HEART_BEAT  30

#define FDHT_PROTO_CMD_RESP        40

#define FDHT_PROTO_PKG_LEN_SIZE		4
#define FDHT_PROTO_CMD_SIZE		1

typedef int fdht_pkg_size_t;

#define PACK_BODY_UNTIL_KEY(pKeyInfo, p) \
	int2buff(pKeyInfo->namespace_len, p); \
	p += 4; \
	if (pKeyInfo->namespace_len > 0) \
	{ \
		memcpy(p, pKeyInfo->szNameSpace, pKeyInfo->namespace_len); \
		p += pKeyInfo->namespace_len; \
	} \
	int2buff(pKeyInfo->obj_id_len, p);  \
	p += 4; \
	if (pKeyInfo->obj_id_len> 0) \
	{ \
		memcpy(p, pKeyInfo->szObjectId, pKeyInfo->obj_id_len); \
		p += pKeyInfo->obj_id_len; \
	} \
	int2buff(pKeyInfo->key_len, p); \
	p += 4; \
	memcpy(p, pKeyInfo->szKey, pKeyInfo->key_len); \
	p += pKeyInfo->key_len; \


typedef struct
{
	char pkg_len[FDHT_PROTO_PKG_LEN_SIZE];  //body length
	char key_hash_code[FDHT_PROTO_PKG_LEN_SIZE]; //the key hash code
	char timestamp[FDHT_PROTO_PKG_LEN_SIZE]; //current time

   	/* key expires, remain timeout = expires - timestamp */
	char expires[FDHT_PROTO_PKG_LEN_SIZE];
	char cmd;
	char keep_alive;
	char status;
} ProtoHeader;

#ifdef __cplusplus
extern "C" {
#endif

int fdht_recv_header(FDHTServerInfo *pServer, fdht_pkg_size_t *in_bytes);

int fdht_recv_response(FDHTServerInfo *pServer, \
		char **buff, const int buff_size, \
		fdht_pkg_size_t *in_bytes);
int fdht_quit(FDHTServerInfo *pServer);

/**
* connect to the server
* params:
*	pServer: server
* return: 0 success, !=0 fail, return the error code
**/
int fdht_connect_server(FDHTServerInfo *pServer);

/**
* close connection to the server
* params:
*	pServer: server
* return:
**/
void fdht_disconnect_server(FDHTServerInfo *pServer);

int fdht_client_set(FDHTServerInfo *pServer, const char keep_alive, \
	const time_t timestamp, const time_t expires, const int prot_cmd, \
	const int key_hash_code, FDHTKeyInfo *pKeyInfo, \
	const char *pValue, const int value_len);

int fdht_client_delete(FDHTServerInfo *pServer, const char keep_alive, \
	const time_t timestamp, const int prot_cmd, \
	const int key_hash_code, FDHTKeyInfo *pKeyInfo);

int fdht_client_heart_beat(FDHTServerInfo *pServer);

#ifdef __cplusplus
}
#endif

#endif

