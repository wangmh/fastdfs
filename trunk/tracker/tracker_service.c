/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//tracker_service.c

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "fdfs_define.h"
#include "logger.h"
#include "fdfs_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "tracker_types.h"
#include "tracker_global.h"
#include "tracker_mem.h"
#include "tracker_proto.h"
#include "tracker_service.h"

pthread_mutex_t g_tracker_thread_lock;
int g_tracker_thread_count = 0;

static int tracker_check_and_sync(TrackerClientInfo *pClientInfo, \
			const int status)
{
	TrackerHeader resp;
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppEnd;
	FDFSStorageBrief briefServers[FDFS_MAX_SERVERS_EACH_GROUP];
	FDFSStorageBrief *pDestServer;
	int out_len;
	int result;

	memset(&resp, 0, sizeof(resp));
	resp.cmd = TRACKER_PROTO_CMD_STORAGE_RESP;
	resp.status = status;

	if (status != 0 || pClientInfo->pGroup == NULL ||
		pClientInfo->pGroup->version == pClientInfo->pStorage->version)
	{
		if ((result=tcpsenddata(pClientInfo->sock, \
			&resp, sizeof(resp), g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, send data fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, \
				result, strerror(result));
			return result;
		}

		return status;
	}

	//printf("sync %d servers\n", pClientInfo->pGroup->count);

	pDestServer = briefServers;
	ppEnd = pClientInfo->pGroup->sorted_servers + \
			pClientInfo->pGroup->count;
	for (ppServer=pClientInfo->pGroup->sorted_servers; \
		ppServer<ppEnd; ppServer++)
	{
		pDestServer->status = (*ppServer)->status;
		memcpy(pDestServer->ip_addr, (*ppServer)->ip_addr, \
			IP_ADDRESS_SIZE);
		pDestServer++;
	}

	out_len = sizeof(FDFSStorageBrief) * pClientInfo->pGroup->count;
	long2buff(out_len, resp.pkg_len);
	if ((result=tcpsenddata(pClientInfo->sock, \
		&resp, sizeof(resp), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	if ((result=tcpsenddata(pClientInfo->sock, \
		briefServers, out_len, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	pClientInfo->pStorage->version = pClientInfo->pGroup->version;
	return status;
}

static void tracker_check_dirty(TrackerClientInfo *pClientInfo)
{
	bool bInserted;
	if (pClientInfo->pGroup != NULL && pClientInfo->pGroup->dirty)
	{
		tracker_mem_pthread_lock();
		if (--(*pClientInfo->pGroup->ref_count) == 0)
		{
			free(pClientInfo->pGroup->ref_count);
			free(pClientInfo->pAllocedGroups);
		}
		tracker_mem_pthread_unlock();

		tracker_mem_add_group(pClientInfo, true, &bInserted);
	}

	if (pClientInfo->pStorage != NULL && pClientInfo->pStorage->dirty)
	{
		tracker_mem_pthread_lock();
		if (--(*pClientInfo->pStorage->ref_count) == 0)
		{
			free(pClientInfo->pStorage->ref_count);
			free(pClientInfo->pAllocedStorages);
		}
		tracker_mem_pthread_unlock();

		tracker_mem_add_storage(pClientInfo, true, &bInserted);
	}
}

static int tracker_deal_storage_replica_chg(TrackerClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	int server_count;
	FDFSStorageBrief briefServers[FDFS_MAX_SERVERS_EACH_GROUP];
	int result;

	memset(&resp, 0, sizeof(resp));
	while (1)
	{
		if ((nInPackLen <= 0) || \
			(nInPackLen % sizeof(FDFSStorageBrief) != 0))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip addr: %s, " \
				"package size "INT64_PRINTF_FORMAT" " \
				"is not correct", \
				__LINE__, \
				TRACKER_PROTO_CMD_STORAGE_REPLICA_CHG, \
				pClientInfo->ip_addr, nInPackLen);
			resp.status = EINVAL;
			break;
		}

		server_count = nInPackLen / sizeof(FDFSStorageBrief);
		if (server_count > FDFS_MAX_SERVERS_EACH_GROUP)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip addr: %s, return storage count: %d" \
				" exceed max: %d", __LINE__, \
				pClientInfo->ip_addr, server_count, \
				FDFS_MAX_SERVERS_EACH_GROUP);
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata(pClientInfo->sock, briefServers, \
			nInPackLen, g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip addr: %s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		resp.status = tracker_mem_sync_storages(pClientInfo, \
				briefServers, server_count);
		break;
	}

	resp.cmd = TRACKER_PROTO_CMD_STORAGE_RESP;
	if ((result=tcpsenddata(pClientInfo->sock, \
		&resp, sizeof(resp), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

static int tracker_deal_storage_join(TrackerClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerStorageJoinBody body;
	int status;

	while (1)
	{
	if (nInPackLen != sizeof(body))
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd: %d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
			"is not correct, expect length: %d.", \
			__LINE__, TRACKER_PROTO_CMD_STORAGE_JOIN, \
			pClientInfo->ip_addr, nInPackLen, sizeof(body));
		status = EINVAL;
		break;
	}

	if ((status=tcprecvdata(pClientInfo->sock, &body, \
		nInPackLen, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, recv data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			status, strerror(status));
		break;
	}

	memcpy(pClientInfo->group_name, body.group_name, FDFS_GROUP_NAME_MAX_LEN);
	pClientInfo->group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
	if ((status=fdfs_validate_group_name(pClientInfo->group_name)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_name: %s", \
			__LINE__, pClientInfo->ip_addr, \
			pClientInfo->group_name);
		break;
	}

	pClientInfo->storage_port = (int)buff2long(body.storage_port);
	if (pClientInfo->storage_port <= 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid port: %d", \
			__LINE__, pClientInfo->ip_addr, \
			pClientInfo->storage_port);
		status = EINVAL;
		break;
	}

	status = tracker_mem_add_group_and_storage(pClientInfo, true);
	break;
	}

	return tracker_check_and_sync(pClientInfo, status);
}

static int tracker_deal_storage_sync_notify(TrackerClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerStorageSyncReqBody body;
	int status;
	char sync_src_ip_addr[IP_ADDRESS_SIZE];
	bool bSaveStorages;

	while (1)
	{
	if (nInPackLen != sizeof(body))
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd: %d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
			"is not correct, expect length: %d.", \
			__LINE__, TRACKER_PROTO_CMD_STORAGE_SYNC_NOTIFY, \
			pClientInfo->ip_addr, nInPackLen, sizeof(body));
		status = EINVAL;
		break;
	}

	if ((status=tcprecvdata(pClientInfo->sock, &body, \
			nInPackLen, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, recv data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			status, strerror(status));
		break;
	}

	if (*(body.src_ip_addr) == '\0')
	{
	if (pClientInfo->pStorage->status == FDFS_STORAGE_STATUS_INIT || \
	    pClientInfo->pStorage->status == FDFS_STORAGE_STATUS_WAIT_SYNC || \
	    pClientInfo->pStorage->status == FDFS_STORAGE_STATUS_SYNCING)
	{
		pClientInfo->pStorage->status = FDFS_STORAGE_STATUS_ONLINE;
		pClientInfo->pGroup->version++;
		tracker_save_storages();
	}

		status = 0;
		break;
	}

	bSaveStorages = false;
	if (pClientInfo->pStorage->status == FDFS_STORAGE_STATUS_INIT)
	{
		pClientInfo->pStorage->status = FDFS_STORAGE_STATUS_WAIT_SYNC;
		pClientInfo->pGroup->version++;
		bSaveStorages = true;
	}

	if (pClientInfo->pStorage->psync_src_server == NULL)
	{
		memcpy(sync_src_ip_addr, body.src_ip_addr, IP_ADDRESS_SIZE);
		sync_src_ip_addr[IP_ADDRESS_SIZE-1] = '\0';

		pClientInfo->pStorage->psync_src_server = \
			tracker_mem_get_storage(pClientInfo->pGroup, \
				sync_src_ip_addr);
		if (pClientInfo->pStorage->psync_src_server == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, " \
				"sync src server: %s not exists", \
				__LINE__, pClientInfo->ip_addr, \
				sync_src_ip_addr);
			status = ENOENT;
			break;
		}

		pClientInfo->pStorage->sync_until_timestamp = \
				(int)buff2long(body.until_timestamp);
		bSaveStorages = true;
	}

	if (bSaveStorages)
	{
		tracker_save_storages();
	}
	status = 0;
	break;
	}

	return tracker_check_and_sync(pClientInfo, status);
}

static int tracker_check_logined(TrackerClientInfo *pClientInfo)
{
	TrackerHeader resp;
	int result;

	if (pClientInfo->pGroup != NULL && pClientInfo->pStorage != NULL)
	{
		return 0;
	}

	memset(&resp, 0, sizeof(resp));
	resp.cmd = TRACKER_PROTO_CMD_STORAGE_RESP;
	resp.status = EACCES;
	if ((result=tcpsenddata(pClientInfo->sock, &resp, sizeof(resp), \
		g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s.",  \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

/**
pkg format:
Header
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
**/
static int tracker_deal_server_list_group_storages( \
		TrackerClientInfo *pClientInfo, const int64_t nInPackLen)
{
	TrackerHeader resp;
	char group_name[FDFS_GROUP_NAME_MAX_LEN+1];
	FDFSGroupInfo *pGroup;
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppEnd;
	FDFSStorageStat *pStorageStat;
	TrackerStorageStat stats[FDFS_MAX_SERVERS_EACH_GROUP];
	TrackerStorageStat *pDest;
	FDFSStorageStatBuff *pStatBuff;
	int out_len;
	int result;

	memset(&resp, 0, sizeof(resp));
	pDest = stats;
	while (1)
	{
		if (nInPackLen != FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
				"expect length: %d", \
				__LINE__, \
				TRACKER_PROTO_CMD_SERVER_LIST_STORAGE, \
				pClientInfo->ip_addr,  \
				nInPackLen, FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata(pClientInfo->sock, group_name, \
			nInPackLen, g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv data fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}

		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
		pGroup = tracker_mem_get_group(group_name);
		if (pGroup == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid group_name: %s", \
				__LINE__, pClientInfo->ip_addr, \
				pClientInfo->group_name);
			resp.status = EINVAL;
			break;
		}

		memset(stats, 0, sizeof(stats));
		ppEnd = pGroup->sorted_servers + pGroup->count;
		for (ppServer=pGroup->sorted_servers; ppServer<ppEnd; \
			ppServer++)
		{
			pStatBuff = &(pDest->stat_buff);
			pStorageStat = &((*ppServer)->stat);
			pDest->status = (*ppServer)->status;
			memcpy(pDest->ip_addr, (*ppServer)->ip_addr, \
				IP_ADDRESS_SIZE);
			long2buff((*ppServer)->total_mb, pDest->sz_total_mb);
			long2buff((*ppServer)->free_mb, pDest->sz_free_mb);

			long2buff(pStorageStat->total_upload_count, \
				 pStatBuff->sz_total_upload_count);
			long2buff(pStorageStat->success_upload_count, \
				 pStatBuff->sz_success_upload_count);
			long2buff(pStorageStat->total_set_meta_count, \
				 pStatBuff->sz_total_set_meta_count);
			long2buff(pStorageStat->success_set_meta_count, \
				 pStatBuff->sz_success_set_meta_count);
			long2buff(pStorageStat->total_delete_count, \
				 pStatBuff->sz_total_delete_count);
			long2buff(pStorageStat->success_delete_count, \
				 pStatBuff->sz_success_delete_count);
			long2buff(pStorageStat->total_download_count, \
				 pStatBuff->sz_total_download_count);
			long2buff(pStorageStat->success_download_count, \
				 pStatBuff->sz_success_download_count);
			long2buff(pStorageStat->total_get_meta_count, \
				 pStatBuff->sz_total_get_meta_count);
			long2buff(pStorageStat->success_get_meta_count, \
				 pStatBuff->sz_success_get_meta_count);
			long2buff(pStorageStat->last_source_update, \
				 pStatBuff->sz_last_source_update);
			long2buff(pStorageStat->last_sync_update, \
				 pStatBuff->sz_last_sync_update);
			pDest++;
		}

		resp.status = 0;
		break;
	}

	out_len = (pDest - stats) * sizeof(TrackerStorageStat);
	long2buff(out_len, resp.pkg_len);
	resp.cmd = TRACKER_PROTO_CMD_SERVER_RESP;
	if ((result=tcpsenddata(pClientInfo->sock, \
		&resp, sizeof(resp), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	if (out_len == 0)
	{
		return resp.status;
	}

	if ((result=tcpsenddata(pClientInfo->sock, \
		stats, out_len, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

/**
pkg format:
Header
FDFS_GROUP_NAME_MAX_LEN bytes: group_name
remain bytes: filename
**/
static int tracker_deal_service_query_fetch(TrackerClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	char in_buff[FDFS_GROUP_NAME_MAX_LEN + 32];
	char group_name[FDFS_GROUP_NAME_MAX_LEN];
	char *filename;
	int out_len;
	FDFSGroupInfo *pGroup;
	FDFSStorageDetail *pStorageServer;
	char out_buff[sizeof(TrackerHeader) + TRACKER_QUERY_STORAGE_BODY_LEN];
	int result;

	memset(&resp, 0, sizeof(resp));
	pGroup = NULL;
	pStorageServer = NULL;
	while (1)
	{
		if (nInPackLen <= FDFS_GROUP_NAME_MAX_LEN)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
				"expect length > %d", \
				__LINE__, \
				TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH, \
				pClientInfo->ip_addr,  \
				nInPackLen, FDFS_GROUP_NAME_MAX_LEN);
			resp.status = EINVAL;
			break;
		}

		if (nInPackLen >= sizeof(in_buff))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is too large, " \
				"expect length should < %d", \
				__LINE__, \
				TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH, \
				pClientInfo->ip_addr, nInPackLen, \
				sizeof(in_buff));
			resp.status = EINVAL;
			break;
		}

		if ((resp.status=tcprecvdata(pClientInfo->sock, in_buff, \
			nInPackLen, g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv data fail, " \
				"errno: %d, error info: %s", \
				__LINE__, pClientInfo->ip_addr, \
				resp.status, strerror(resp.status));
			break;
		}
		in_buff[nInPackLen] = '\0';

		memcpy(group_name, in_buff, FDFS_GROUP_NAME_MAX_LEN);
		group_name[FDFS_GROUP_NAME_MAX_LEN] = '\0';
		filename = in_buff + FDFS_GROUP_NAME_MAX_LEN;
		pGroup = tracker_mem_get_group(group_name);
		if (pGroup == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid group_name: %s", \
				__LINE__, pClientInfo->ip_addr, \
				pClientInfo->group_name);
			resp.status = ENOENT;
			break;
		}

		if (pGroup->active_count == 0)
		{
			resp.status = ENOENT;
			break;
		}

		pStorageServer = *(pGroup->active_servers + \
				   pGroup->current_read_server);
		pGroup->current_read_server++;
		if (pGroup->current_read_server >= \
				pGroup->active_count)
		{
			pGroup->current_read_server = 0;
		}

		resp.status = 0;
		break;
	}

	resp.cmd = TRACKER_PROTO_CMD_SERVICE_RESP;
	if (resp.status == 0)
	{
		out_len = TRACKER_QUERY_STORAGE_BODY_LEN;
		long2buff(out_len, resp.pkg_len);

		memcpy(out_buff, &resp, sizeof(resp));
		memcpy(out_buff + sizeof(resp), pGroup->group_name, \
				FDFS_GROUP_NAME_MAX_LEN);
		memcpy(out_buff + sizeof(resp) + FDFS_GROUP_NAME_MAX_LEN, \
				pStorageServer->ip_addr, IP_ADDRESS_SIZE-1);
		long2buff(pGroup->storage_port, out_buff + sizeof(resp) + \
			FDFS_GROUP_NAME_MAX_LEN + IP_ADDRESS_SIZE - 1);
	}
	else
	{
		out_len = 0;
		long2buff(out_len, resp.pkg_len);
		memcpy(out_buff, &resp, sizeof(resp));
	}

	if ((result=tcpsenddata(pClientInfo->sock, \
		out_buff, sizeof(resp) + out_len, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

static int tracker_deal_service_query_storage(TrackerClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	int out_len;
	FDFSGroupInfo *pStoreGroup;
	FDFSGroupInfo **ppFoundGroup;
	FDFSGroupInfo **ppGroup;
	FDFSStorageDetail *pStorageServer;
	char out_buff[sizeof(TrackerHeader) + TRACKER_QUERY_STORAGE_BODY_LEN];
	bool bHaveActiveServer;
	int result;

	memset(&resp, 0, sizeof(resp));
	pStoreGroup = NULL;
	pStorageServer = NULL;
	while (1)
	{
		if (nInPackLen != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
				"expect length: 0", \
				__LINE__, \
				TRACKER_PROTO_CMD_SERVICE_QUERY_STORE, \
				pClientInfo->ip_addr,  \
				nInPackLen);
			resp.status = EINVAL;
			break;
		}

		if (g_groups.count == 0)
		{
			resp.status = ENOENT;
			break;
		}

		if (g_groups.store_lookup == FDFS_STORE_LOOKUP_ROUND_ROBIN || \
		    g_groups.store_lookup == FDFS_STORE_LOOKUP_LOAD_BALANCE)
		{
			bHaveActiveServer = false;
			ppFoundGroup = g_groups.sorted_groups + \
					g_groups.current_write_group;
			if ((*ppFoundGroup)->active_count > 0)
			{
				bHaveActiveServer = true;
				if ((*ppFoundGroup)->free_mb > \
					g_storage_reserved_mb)
				{
					pStoreGroup = *ppFoundGroup;
				}
			}

			if (pStoreGroup == NULL)
			{
				FDFSGroupInfo **ppGroupEnd;
				ppGroupEnd = g_groups.sorted_groups + \
						g_groups.count;
				for (ppGroup=ppFoundGroup+1; \
					ppGroup<ppGroupEnd; ppGroup++)
				{
					if ((*ppGroup)->active_count == 0)
					{
						continue;
					}

					bHaveActiveServer = true;
					if ((*ppGroup)->free_mb > \
						g_storage_reserved_mb)
					{
					pStoreGroup = *ppGroup;
					if (g_groups.store_lookup == \
						FDFS_STORE_LOOKUP_LOAD_BALANCE)
					{
						g_groups.current_write_group = \
						ppGroup-g_groups.sorted_groups;
					}
					break;
					}
				}

				if (pStoreGroup == NULL)
				{
				for (ppGroup=g_groups.sorted_groups; \
					ppGroup<ppFoundGroup; ppGroup++)
				{
					if ((*ppGroup)->active_count == 0)
					{
						continue;
					}

					bHaveActiveServer = true;
					if ((*ppGroup)->free_mb > \
						g_storage_reserved_mb)
					{
					pStoreGroup = *ppGroup;
					if (g_groups.store_lookup == \
						FDFS_STORE_LOOKUP_LOAD_BALANCE)
					{
						g_groups.current_write_group = \
						ppGroup-g_groups.sorted_groups;
					}
					break;
					}
				}
				}

				if (pStoreGroup == NULL)
				{
					if (bHaveActiveServer)
					{
						resp.status = ENOSPC;
					}
					else
					{
						resp.status = ENOENT;
					}
					break;
				}
			}

			if (g_groups.store_lookup == FDFS_STORE_LOOKUP_ROUND_ROBIN)
			{
				g_groups.current_write_group++;
				if (g_groups.current_write_group >= g_groups.count)
				{
					g_groups.current_write_group = 0;
				}
			}
		}
		else if (g_groups.store_lookup == FDFS_STORE_LOOKUP_SPEC_GROUP)
		{
			if (g_groups.pStoreGroup == NULL || \
				g_groups.pStoreGroup->active_count == 0)
			{
				resp.status = ENOENT;
				break;
			}

			if (g_groups.pStoreGroup->free_mb <= \
				g_storage_reserved_mb)
			{
				resp.status = ENOSPC;
				break;
			}

			pStoreGroup = g_groups.pStoreGroup;
		}
		else
		{
			resp.status = EINVAL;
			break;
		}

		if (pStoreGroup->current_write_server >= \
				pStoreGroup->active_count)
		{
			pStoreGroup->current_write_server = 0;
		}

		/*
		//printf("pStoreGroup->current_write_server: %d, " \
			"pStoreGroup->active_count=%d\n", \
			pStoreGroup->current_write_server, \
			pStoreGroup->active_count);
		*/

		pStorageServer = *(pStoreGroup->active_servers + \
				   pStoreGroup->current_write_server);
		pStoreGroup->current_write_server++;
		resp.status = 0;
		break;
	}

	resp.cmd = TRACKER_PROTO_CMD_SERVICE_RESP;
	if (resp.status == 0)
	{
		out_len = TRACKER_QUERY_STORAGE_BODY_LEN;
		long2buff(out_len, resp.pkg_len);

		memcpy(out_buff, &resp, sizeof(resp));
		memcpy(out_buff + sizeof(resp), pStoreGroup->group_name, \
				FDFS_GROUP_NAME_MAX_LEN);
		memcpy(out_buff + sizeof(resp) + FDFS_GROUP_NAME_MAX_LEN, \
				pStorageServer->ip_addr, IP_ADDRESS_SIZE-1);
		long2buff(pStoreGroup->storage_port, out_buff + sizeof(resp) + \
			FDFS_GROUP_NAME_MAX_LEN + IP_ADDRESS_SIZE-1);
	}
	else
	{
		out_len = 0;
		long2buff(out_len, resp.pkg_len);
		memcpy(out_buff, &resp, sizeof(resp));
	}

	if ((result=tcpsenddata(pClientInfo->sock, \
		out_buff, sizeof(resp) + out_len, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

static int tracker_deal_server_list_groups(TrackerClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	TrackerHeader resp;
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppEnd;
	TrackerGroupStat groupStats[FDFS_MAX_GROUPS];
	TrackerGroupStat *pDest;
	int out_len;
	int result;

	memset(&resp, 0, sizeof(resp));
	pDest = groupStats;
	while (1)
	{
		if (nInPackLen != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
				"expect length: 0", \
				__LINE__, \
				TRACKER_PROTO_CMD_SERVER_LIST_GROUP, \
				pClientInfo->ip_addr,  \
				nInPackLen);
			resp.status = EINVAL;
			break;
		}

		ppEnd = g_groups.sorted_groups + g_groups.count;
		for (ppGroup=g_groups.sorted_groups; ppGroup<ppEnd; ppGroup++)
		{
			memcpy(pDest->group_name, (*ppGroup)->group_name, \
				FDFS_GROUP_NAME_MAX_LEN + 1);
			long2buff((*ppGroup)->free_mb, pDest->sz_free_mb);
			long2buff((*ppGroup)->count, pDest->sz_count);
			long2buff((*ppGroup)->storage_port, \
				pDest->sz_storage_port);
			long2buff((*ppGroup)->active_count, \
				pDest->sz_active_count);
			long2buff((*ppGroup)->current_write_server, \
				pDest->sz_current_write_server);
			pDest++;
		}

		resp.status = 0;
		break;
	}

	out_len = (pDest - groupStats) * sizeof(TrackerGroupStat);
	long2buff(out_len, resp.pkg_len);
	resp.cmd = TRACKER_PROTO_CMD_SERVER_RESP;
	if ((result=tcpsenddata(pClientInfo->sock, \
		&resp, sizeof(resp), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	if (out_len == 0)
	{
		return resp.status;
	}

	if ((result=tcpsenddata(pClientInfo->sock, \
		groupStats, out_len, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return resp.status;
}

static int tracker_deal_storage_sync_src_req(TrackerClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	char out_buff[sizeof(TrackerHeader)+sizeof(TrackerStorageSyncReqBody)];
	char dest_ip_addr[IP_ADDRESS_SIZE];
	TrackerHeader *pResp;
	TrackerStorageSyncReqBody *pBody;
	FDFSStorageDetail *pDestStorage;
	int out_len;
	int result;

	memset(out_buff, 0, sizeof(out_buff));
	pResp = (TrackerHeader *)out_buff;
	pBody = (TrackerStorageSyncReqBody *)(out_buff + sizeof(TrackerHeader));
	out_len = sizeof(TrackerHeader);
	while (1)
	{
		if (nInPackLen != IP_ADDRESS_SIZE)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
				"expect length: %d", \
				__LINE__, \
				TRACKER_PROTO_CMD_STORAGE_SYNC_SRC_REQ, \
				pClientInfo->ip_addr, nInPackLen, \
				IP_ADDRESS_SIZE);
			pResp->status = EINVAL;
			break;
		}

		if ((pResp->status=tcprecvdata(pClientInfo->sock, \
			dest_ip_addr, nInPackLen, g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip addr: %s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, \
				TRACKER_PROTO_CMD_STORAGE_SYNC_SRC_REQ, \
				pClientInfo->ip_addr, \
				pResp->status, strerror(pResp->status));
			break;
		}

		dest_ip_addr[IP_ADDRESS_SIZE-1] = '\0';
		pDestStorage = tracker_mem_get_storage(pClientInfo->pGroup, \
				dest_ip_addr);
		if (pDestStorage == NULL)
		{
			pResp->status = ENOENT;
			break;
		}

		if (pDestStorage->status == FDFS_STORAGE_STATUS_INIT)
		{
			pResp->status = ENOENT;
			break;
		}

		if (pDestStorage->psync_src_server != NULL)
		{
			strcpy(pBody->src_ip_addr, \
				pDestStorage->psync_src_server->ip_addr);
			long2buff(pDestStorage->sync_until_timestamp, \
				pBody->until_timestamp);
			out_len += sizeof(TrackerStorageSyncReqBody);
		}

		pResp->status = 0;
		break;
	}

	//printf("deal sync request, status=%d\n", pResp->status);

	long2buff(out_len - (int)sizeof(TrackerHeader), pResp->pkg_len);
	pResp->cmd = TRACKER_PROTO_CMD_SERVER_RESP;
	if ((result=tcpsenddata(pClientInfo->sock, \
		out_buff, out_len, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	return 0;
}

static int tracker_deal_storage_sync_dest_req(TrackerClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	char out_buff[sizeof(TrackerHeader)+sizeof(TrackerStorageSyncReqBody)];
	TrackerHeader *pResp;
	TrackerStorageSyncReqBody *pBody;
	int out_len;
	int sync_until_timestamp;
	FDFSStorageDetail *pSrcStorage;
	int result;

	sync_until_timestamp = 0;
	memset(out_buff, 0, sizeof(out_buff));
	pResp = (TrackerHeader *)out_buff;
	pBody = (TrackerStorageSyncReqBody *)(out_buff + sizeof(TrackerHeader));
	out_len = sizeof(TrackerHeader);
	pSrcStorage = NULL;
	while (1)
	{
		if (nInPackLen != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
				"expect length: 0", \
				__LINE__, \
				TRACKER_PROTO_CMD_STORAGE_SYNC_DEST_REQ, \
				pClientInfo->ip_addr, nInPackLen);
			pResp->status = EINVAL;
			break;
		}

		if (pClientInfo->pGroup->count <= 1 || \
			tracker_get_group_success_upload_count( \
				pClientInfo->pGroup) <= 0)
		{
			pResp->status = 0;
			break;
		}

		pSrcStorage = tracker_get_group_sync_src_server( \
			pClientInfo->pGroup, pClientInfo->pStorage);
		if (pSrcStorage == NULL)
		{
			pResp->status = ENOENT;
			break;
		}

		sync_until_timestamp = (int)time(NULL);
		strcpy(pBody->src_ip_addr, pSrcStorage->ip_addr);
		long2buff(sync_until_timestamp, pBody->until_timestamp);
		out_len += sizeof(TrackerStorageSyncReqBody);
		pResp->status = 0;
		break;
	}

	//printf("deal sync request, status=%d\n", pResp->status);

	long2buff(out_len - (int)sizeof(TrackerHeader), pResp->pkg_len);
	pResp->cmd = TRACKER_PROTO_CMD_SERVER_RESP;
	if ((result=tcpsenddata(pClientInfo->sock, \
		out_buff, out_len, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	if (pSrcStorage == NULL)
	{
		if (pResp->status == 0)
		{
			pClientInfo->pStorage->status = \
				FDFS_STORAGE_STATUS_ONLINE;
			pClientInfo->pGroup->version++;
			tracker_save_storages();
		}

		return pResp->status;
	}

	if ((result=tcprecvdata(pClientInfo->sock, pResp, \
		sizeof(TrackerHeader), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"cmd=%d, client ip addr: %s, recv data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, \
			TRACKER_PROTO_CMD_STORAGE_SYNC_DEST_REQ, \
			pClientInfo->ip_addr, \
			result, strerror(result));
		return result;
	}

	if (pResp->cmd != TRACKER_PROTO_CMD_STORAGE_RESP)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip addr: %s, " \
			"recv storage confirm fail, resp cmd: %d, " \
			"expect cmd: %d",  \
			__LINE__, pClientInfo->ip_addr, \
			pResp->cmd, TRACKER_PROTO_CMD_STORAGE_RESP);
		return EINVAL;
	}

	if (pResp->status != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip addr: %s, " \
			"recv storage confirm fail, resp status: %d, " \
			"expect status: 0",  \
			__LINE__, pClientInfo->ip_addr, pResp->status);
		return pResp->status;
	}

	pClientInfo->pStorage->psync_src_server = pSrcStorage;
	pClientInfo->pStorage->sync_until_timestamp = sync_until_timestamp;
	pClientInfo->pStorage->status = FDFS_STORAGE_STATUS_WAIT_SYNC;
	pClientInfo->pGroup->version++;

	tracker_save_storages();
	return 0;
}

static void tracker_find_max_free_space_group()
{
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppGroupEnd;
	FDFSGroupInfo **ppMaxGroup;

	ppMaxGroup = NULL;
	ppGroupEnd = g_groups.sorted_groups + g_groups.count;
	for (ppGroup=g_groups.sorted_groups; \
		ppGroup<ppGroupEnd; ppGroup++)
	{
		if ((*ppGroup)->active_count > 0)
		{
			if (ppMaxGroup == NULL)
			{
				ppMaxGroup = ppGroup;
			}
			else if ((*ppGroup)->free_mb > (*ppMaxGroup)->free_mb)
			{
				ppMaxGroup = ppGroup;
			}
		}
	}

	if (ppMaxGroup == NULL)
	{
		return;
	}

	g_groups.current_write_group = ppMaxGroup - g_groups.sorted_groups;
}

static int tracker_deal_storage_report(TrackerClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	int status;
	int result;
	TrackerStatReportReqBody statBuff;
 
	while (1)
	{
		if (nInPackLen != sizeof(TrackerStatReportReqBody))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
				"expect length: %d", \
				__LINE__, \
				TRACKER_PROTO_CMD_STORAGE_REPORT, \
				pClientInfo->ip_addr, nInPackLen, \
				sizeof(TrackerStatReportReqBody));
			status = EINVAL;
			break;
		}

		if ((status=tcprecvdata(pClientInfo->sock, &statBuff, \
			nInPackLen, g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip addr: %s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, \
				TRACKER_PROTO_CMD_STORAGE_REPORT, \
				pClientInfo->ip_addr, \
				status, strerror(status));
			break;
		}

		pClientInfo->pStorage->total_mb=buff2long(statBuff.sz_total_mb);
		pClientInfo->pStorage->free_mb = buff2long(statBuff.sz_free_mb);

		if ((pClientInfo->pGroup->free_mb == 0) ||
			(pClientInfo->pStorage->free_mb < \
				pClientInfo->pGroup->free_mb))
		{
		pClientInfo->pGroup->free_mb = \
			pClientInfo->pStorage->free_mb;
		if (g_groups.store_lookup == \
			FDFS_STORE_LOOKUP_LOAD_BALANCE)
		{
			if ((result=pthread_mutex_lock( \
				&g_tracker_thread_lock)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_lock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			}
			tracker_find_max_free_space_group();
			if ((result=pthread_mutex_unlock( \
				&g_tracker_thread_lock)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_unlock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
			}
		}
		}

		status = 0;

		/*
		//printf("storage: %s:%d, total_mb=%dMB, free_mb=%dMB\n", \
			pClientInfo->pStorage->ip_addr, \
			pClientInfo->pGroup->storage_port, \
			pClientInfo->pStorage->total_mb, \
			pClientInfo->pStorage->free_mb);
		*/

		break;
	}

	if (status == 0)
	{
		tracker_check_dirty(pClientInfo);
		tracker_mem_active_store_server(pClientInfo->pGroup, \
				pClientInfo->pStorage);
	}

	//printf("deal storage report, status=%d\n", status);
	return tracker_check_and_sync(pClientInfo, status);
}

static int tracker_deal_storage_beat(TrackerClientInfo *pClientInfo, \
				const int64_t nInPackLen)
{
	int status;
	FDFSStorageStatBuff statBuff;
	FDFSStorageStat *pStat;
 
	while (1)
	{
		if (nInPackLen == 0)
		{
			status = 0;
			break;
		}

		if (nInPackLen != sizeof(FDFSStorageStatBuff))
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip: %s, package size "INT64_PRINTF_FORMAT" " \
				"is not correct, " \
				"expect length: 0 or %d", \
				__LINE__, \
				TRACKER_PROTO_CMD_STORAGE_BEAT, \
				pClientInfo->ip_addr, nInPackLen, \
				sizeof(FDFSStorageStatBuff));
			status = EINVAL;
			break;
		}

		if ((status=tcprecvdata(pClientInfo->sock, &statBuff, \
			sizeof(FDFSStorageStatBuff), g_network_timeout)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"cmd=%d, client ip addr: %s, recv data fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, \
				TRACKER_PROTO_CMD_STORAGE_BEAT, \
				pClientInfo->ip_addr, \
				status, strerror(status));
			break;
		}

		pStat = &(pClientInfo->pStorage->stat);

		pStat->total_upload_count = \
			buff2long(statBuff.sz_total_upload_count);
		pStat->success_upload_count = \
			buff2long(statBuff.sz_success_upload_count);
		pStat->total_download_count = \
			buff2long(statBuff.sz_total_download_count);
		pStat->success_download_count = \
			buff2long(statBuff.sz_success_download_count);
		pStat->total_set_meta_count = \
			buff2long(statBuff.sz_total_set_meta_count);
		pStat->success_set_meta_count = \
			buff2long(statBuff.sz_success_set_meta_count);
		pStat->total_delete_count = \
			buff2long(statBuff.sz_total_delete_count);
		pStat->success_delete_count = \
			buff2long(statBuff.sz_success_delete_count);
		pStat->total_get_meta_count = \
			buff2long(statBuff.sz_total_get_meta_count);
		pStat->success_get_meta_count = \
			buff2long(statBuff.sz_success_get_meta_count);
		pStat->last_source_update = \
			buff2long(statBuff.sz_last_source_update);
		pStat->last_sync_update = \
			buff2long(statBuff.sz_last_sync_update);

		if (++g_storage_stat_chg_count % TRACKER_SYNC_TO_FILE_FREQ == 0)
		{
			status = tracker_save_storages();
		}
		else
		{
			status = 0;
		}

		//printf("g_storage_stat_chg_count=%d\n", g_storage_stat_chg_count);

		break;
	}

	if (status == 0)
	{
		tracker_check_dirty(pClientInfo);
		tracker_mem_active_store_server(pClientInfo->pGroup, \
				pClientInfo->pStorage);
	}

	//printf("deal heart beat, status=%d\n", status);
	return tracker_check_and_sync(pClientInfo, status);
}

void* tracker_thread_entrance(void* arg)
{
/*
package format:
8 bytes length (hex string)
1 bytes cmd (char)
1 bytes status(char)
data buff (struct)
*/
	TrackerClientInfo client_info;	
	TrackerHeader header;
	int result;
	int64_t nInPackLen;
	int count;
	int recv_bytes;
	int log_level;
	in_addr_t client_ip;
	int server_sock;
	
	server_sock = (int)arg;

	while (g_continue_flag)
	{
	if ((result=pthread_mutex_lock(&g_tracker_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	if (!g_continue_flag)
	{
		pthread_mutex_unlock(&g_tracker_thread_lock);
		break;
	}

	memset(&client_info, 0, sizeof(client_info));
	client_info.sock = nbaccept(server_sock, 1 * 60, &result);
	if (pthread_mutex_unlock(&g_tracker_thread_lock) != 0)
	{
		logError("file: "__FILE__", line: %d, "   \
			"call pthread_mutex_unlock fail", \
			__LINE__);
	}
	if(client_info.sock < 0) //error
	{
		if (result == ETIMEDOUT || result == EINTR || \
			result == EAGAIN)
		{
			continue;
		}
			
		if(result == EBADF)
		{
			logError("file: "__FILE__", line: %d, " \
				"accept failed, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			break;
		}
			
		logError("file: "__FILE__", line: %d, " \
			"accept failed, errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		continue;
	}
	
	client_ip = getPeerIpaddr(client_info.sock, \
				client_info.ip_addr, IP_ADDRESS_SIZE);
	if (g_allow_ip_count >= 0)
	{
		if (bsearch(&client_ip, g_allow_ip_addrs, g_allow_ip_count, \
			sizeof(in_addr_t), cmp_by_ip_addr_t) == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"ip addr %s is not allowed to access", \
				__LINE__, client_info.ip_addr);

			close(client_info.sock);
			continue;
		}
	}

	count = 0;
	while (g_continue_flag)
	{
		result = tcprecvdata_ex(client_info.sock, &header, \
			sizeof(header), g_network_timeout, &recv_bytes);
		if (result == ETIMEDOUT && count > 0)
		{
			continue;
		}

		if (result != 0)
		{
			if (result == ENOTCONN && recv_bytes == 0)
			{
				log_level = LOG_WARNING;
			}
			else
			{
				log_level = LOG_ERR;
			}
			log_it(log_level, "file: "__FILE__", line: %d, " \
				"client ip: %s, recv data fail, " \
				"errno: %d, error info: %s", \
				__LINE__, client_info.ip_addr, \
				result, strerror(result));
			break;
		}

		nInPackLen = buff2long(header.pkg_len);

		tracker_check_dirty(&client_info);

		if (header.cmd == TRACKER_PROTO_CMD_STORAGE_BEAT)
		{
			if (tracker_check_logined(&client_info) != 0)
			{
				break;
			}

			if (tracker_deal_storage_beat(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == TRACKER_PROTO_CMD_STORAGE_REPORT)
		{
			if (tracker_check_logined(&client_info) != 0)
			{
				break;
			}

			if (tracker_deal_storage_report(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == TRACKER_PROTO_CMD_STORAGE_JOIN)
		{ 
			if (tracker_deal_storage_join(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == TRACKER_PROTO_CMD_STORAGE_REPLICA_CHG)
		{
			if (tracker_check_logined(&client_info) != 0)
			{
				break;
			}

			if (tracker_deal_storage_replica_chg(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH)
		{
			if (tracker_deal_service_query_fetch(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_STORE)
		{
			if (tracker_deal_service_query_storage(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == TRACKER_PROTO_CMD_SERVER_LIST_GROUP)
		{
			if (tracker_deal_server_list_groups(&client_info, \
				nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == TRACKER_PROTO_CMD_SERVER_LIST_STORAGE)
		{
			if (tracker_deal_server_list_group_storages( \
				&client_info, nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == TRACKER_PROTO_CMD_STORAGE_SYNC_SRC_REQ)
		{
			if (tracker_deal_storage_sync_src_req( \
				&client_info, nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == TRACKER_PROTO_CMD_STORAGE_SYNC_DEST_REQ)
		{
			if (tracker_deal_storage_sync_dest_req( \
				&client_info, nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == TRACKER_PROTO_CMD_STORAGE_SYNC_NOTIFY)
		{
			if (tracker_deal_storage_sync_notify( \
				&client_info, nInPackLen) != 0)
			{
				break;
			}
		}
		else if (header.cmd == FDFS_PROTO_CMD_QUIT)
		{
			break;
		}
		else
		{
			logError("file: "__FILE__", line: %d, "   \
				"client ip: %s, unkown cmd: %d", \
				__LINE__, client_info.ip_addr, \
				header.cmd);
			break;
		}

		count++;
	}

	if (g_continue_flag)
	{
		tracker_check_dirty(&client_info);
		tracker_mem_offline_store_server(&client_info);
	}

	if (client_info.pGroup != NULL)
	{
		--(*(client_info.pGroup->ref_count));
	}

	if (client_info.pStorage != NULL)
	{
		--(*(client_info.pStorage->ref_count));
	}

	close(client_info.sock);
	}

	if ((result=pthread_mutex_lock(&g_tracker_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}
	g_tracker_thread_count--;
	if ((result=pthread_mutex_unlock(&g_tracker_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return NULL;
}

