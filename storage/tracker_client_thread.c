/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/


#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#ifdef OS_LINUX
#include <sys/vfs.h>
#include <sys/statfs.h>
#endif
#include <sys/param.h>
#include <sys/mount.h>
#include "fdfs_define.h"
#include "logger.h"
#include "fdfs_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "tracker_client_thread.h"
#include "storage_global.h"
#include "storage_sync.h"
#include "storage_func.h"

static pthread_mutex_t reporter_thread_lock;

/* save report thread ids */
static pthread_t *report_tids = NULL;

static int tracker_heart_beat(TrackerServerInfo *pTrackerServer, \
			int *pstat_chg_sync_count);
static int tracker_report_df_stat(TrackerServerInfo *pTrackerServer);
static int tracker_sync_dest_req(TrackerServerInfo *pTrackerServer);
static int tracker_sync_notify(TrackerServerInfo *pTrackerServer);
static int tracker_report_sync_timestamp(TrackerServerInfo *pTrackerServer);

int tracker_report_init()
{
	int result;

	memset(g_storage_servers, 0, sizeof(g_storage_servers));
	memset(g_sorted_storages, 0, sizeof(g_sorted_storages));

	if ((result=init_pthread_lock(&reporter_thread_lock)) != 0)
	{
		return result;
	}

	return 0;
}
 
int tracker_report_destroy()
{
	int result;

	if ((result=pthread_mutex_destroy(&reporter_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_destroy fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	return 0;
}

int kill_tracker_report_threads()
{
	int result;

	if (report_tids != NULL)
	{
		result = kill_work_threads(report_tids, \
				g_tracker_server_count);

		free(report_tids);
		report_tids = NULL;
	}
	else
	{
		result = 0;
	}

	return result;
}

static void* tracker_report_thread_entrance(void* arg)
{
	TrackerServerInfo *pTrackerServer;
	char tracker_client_ip[IP_ADDRESS_SIZE];
	char szFailPrompt[36];
	bool sync_old_done;
	int stat_chg_sync_count;
	int sync_time_chg_count;
	int sleep_secs;
	time_t current_time;
	time_t last_df_report_time;
	time_t last_sync_report_time;
	time_t last_beat_time;
	int result;
	int previousCode;
	int nContinuousFail;

	stat_chg_sync_count = 0;

	pTrackerServer = (TrackerServerInfo *)arg;
	pTrackerServer->sock = -1;

	sync_old_done = g_sync_old_done;
	while (g_continue_flag &&  \
		g_tracker_reporter_count < g_tracker_server_count)
	{
		sleep(1); //waiting for all thread started
	}

	/*
	//printf("tracker_report_thread %s:%d start.\n", \
		pTrackerServer->ip_addr, pTrackerServer->port);
	*/

	result = 0;
	previousCode = 0;
	nContinuousFail = 0;
	sleep_secs = g_heart_beat_interval < g_stat_report_interval ? \
			g_heart_beat_interval : g_stat_report_interval;

	while (g_continue_flag)
	{
		if (pTrackerServer->sock >= 0)
		{
			close(pTrackerServer->sock);
		}
		pTrackerServer->sock = socket(AF_INET, SOCK_STREAM, 0);
		if(pTrackerServer->sock < 0)
		{
			logCrit("file: "__FILE__", line: %d, " \
				"socket create failed, errno: %d, " \
				"error info: %s. program exit!", \
				__LINE__, errno, strerror(errno));
			g_continue_flag = false;
			break;
		}

		if ((result=connectserverbyip(pTrackerServer->sock, \
			pTrackerServer->ip_addr, \
			pTrackerServer->port)) != 0)
		{
			if (previousCode != result)
			{
				logError("file: "__FILE__", line: %d, " \
					"connect to tracker server %s:%d fail" \
					", errno: %d, error info: %s", \
					__LINE__, pTrackerServer->ip_addr, \
					pTrackerServer->port, \
					result, strerror(result));
				previousCode = result;
			}

			nContinuousFail++;
			if (g_continue_flag)
			{
				sleep(g_heart_beat_interval);
				continue;
			}
			else
			{
				break;
			}
		}

		if (nContinuousFail == 0)
		{
			*szFailPrompt = '\0';
		}
		else
		{
			sprintf(szFailPrompt, ", continuous fail count: %d", \
				nContinuousFail);
		}
		logInfo("file: "__FILE__", line: %d, " \
			"successfully connect to tracker server %s:%d%s", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, szFailPrompt);

		previousCode = 0;
		nContinuousFail = 0;

		getSockIpaddr(pTrackerServer->sock, \
				tracker_client_ip, IP_ADDRESS_SIZE);
		insert_into_local_host_ip(tracker_client_ip);

		/*
		//printf("file: "__FILE__", line: %d, " \
			"tracker_client_ip: %s\n", \
			__LINE__, tracker_client_ip);
		//print_local_host_ip_addrs();
		*/

		if (tracker_report_join(pTrackerServer) != 0)
		{
			sleep(g_heart_beat_interval);
			continue;
		}

		if (!sync_old_done)
		{
			if ((result=pthread_mutex_lock(&reporter_thread_lock)) \
					 != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_lock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));

				fdfs_quit(pTrackerServer);
				sleep(g_heart_beat_interval);
				continue;
			}

			if (!g_sync_old_done)
			{
				if (tracker_sync_dest_req(pTrackerServer) == 0)
				{
					g_sync_old_done = true;
					if (storage_write_to_sync_ini_file() \
						!= 0)
					{
					logCrit("file: "__FILE__", line: %d, " \
						"storage_write_to_sync_ini_file"\
						"  fail, program exit!", \
						__LINE__);

						g_continue_flag = false;
						pthread_mutex_unlock( \
							&reporter_thread_lock);
						break;
					}
				}
				else //request failed or need to try again
				{
					pthread_mutex_unlock( \
						&reporter_thread_lock);

					fdfs_quit(pTrackerServer);
					sleep(g_heart_beat_interval);
					continue;
				}
			}

			if ((result=pthread_mutex_unlock(&reporter_thread_lock))
				 != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_unlock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
			}

			sync_old_done = true;
		}

		if (tracker_sync_notify(pTrackerServer) != 0)
		{
			fdfs_quit(pTrackerServer);
			sleep(g_heart_beat_interval);
			continue;
		}

		sync_time_chg_count = 0;
		last_df_report_time = 0;
		last_beat_time = 0;
		last_sync_report_time = 0;

		while (g_continue_flag)
		{
			current_time = time(NULL);
			if (current_time - last_beat_time >= \
					g_heart_beat_interval)
			{
				if (tracker_heart_beat(pTrackerServer, \
					&stat_chg_sync_count) != 0)
				{
					break;
				}

				last_beat_time = current_time;
			}

			if (sync_time_chg_count != g_sync_change_count && \
				current_time - last_sync_report_time >= \
					g_heart_beat_interval)
			{
				if (tracker_report_sync_timestamp( \
						pTrackerServer) != 0)
				{
					break;
				}

				sync_time_chg_count = g_sync_change_count;
				last_sync_report_time = current_time;
			}

			if (current_time - last_df_report_time >= \
					g_stat_report_interval)
			{
				if (tracker_report_df_stat(pTrackerServer) != 0)
				{
					break;
				}

				last_df_report_time = current_time;
			}

			sleep(1);
		}

		if ((!g_continue_flag) && fdfs_quit(pTrackerServer) != 0)
		{
		}

		close(pTrackerServer->sock);
		pTrackerServer->sock = -1;
		if (g_continue_flag)
		{
			sleep(1);
		}
	}

	if (nContinuousFail > 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"connect to tracker server %s:%d fail, try count: %d" \
			", errno: %d, error info: %s", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, nContinuousFail, \
			result, strerror(result));
	}

	if ((result=pthread_mutex_lock(&reporter_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}
	g_tracker_reporter_count--;
	if ((result=pthread_mutex_unlock(&reporter_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return NULL;
}

static bool tracker_insert_into_sorted_servers( \
		FDFSStorageServer *pInsertedServer)
{
	FDFSStorageServer **ppServer;
	FDFSStorageServer **ppEnd;
	int nCompare;

	ppEnd = g_sorted_storages + g_storage_count;
	for (ppServer=ppEnd; ppServer > g_sorted_storages; ppServer--)
	{
		nCompare = strcmp(pInsertedServer->server.ip_addr, \
			   	(*(ppServer-1))->server.ip_addr);
		if (nCompare > 0)
		{
			*ppServer = pInsertedServer;
			return true;
		}
		else if (nCompare < 0)
		{
			*ppServer = *(ppServer-1);
		}
		else  //nCompare == 0
		{
			for (; ppServer < ppEnd; ppServer++) //restore
			{
				*ppServer = *(ppServer+1);
			}
			return false;
		}
	}

	*ppServer = pInsertedServer;
	return true;
}

int tracker_sync_diff_servers(TrackerServerInfo *pTrackerServer, \
		FDFSStorageBrief *briefServers, const int server_count)
{
	TrackerHeader resp;
	int out_len;
	int result;

	if (server_count == 0)
	{
		return 0;
	}

	memset(&resp, 0, sizeof(resp));
	resp.cmd = TRACKER_PROTO_CMD_STORAGE_REPLICA_CHG;

	out_len = sizeof(FDFSStorageBrief) * server_count;
	long2buff(out_len, resp.pkg_len);
	if ((result=tcpsenddata(pTrackerServer->sock, &resp, sizeof(resp), \
			g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"trackert server %s:%d, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, result, strerror(result));
		return result;
	}

	if ((result=tcpsenddata(pTrackerServer->sock, \
		briefServers, out_len, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"trackert server %s:%d, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, result, strerror(result));
		return result;
	}


	if ((result=tcprecvdata(pTrackerServer->sock, &resp, \
			sizeof(resp), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, recv data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, result, strerror(result));
		return result;
	}

	if (memcmp(resp.pkg_len, "\0\0\0\0\0\0\0\0", \
		FDFS_PROTO_PKG_LEN_SIZE) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, " \
			"expect pkg len 0, but recv pkg len != 0", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port);
		return EINVAL;
	}

	return resp.status;
}

static int tracker_merge_servers(TrackerServerInfo *pTrackerServer, \
		FDFSStorageBrief *briefServers, const int server_count)
{
	FDFSStorageBrief *pServer;
	FDFSStorageBrief *pEnd;
	FDFSStorageServer *pInsertedServer;
	FDFSStorageServer **ppFound;
	FDFSStorageServer *pGlobalServer;
	FDFSStorageServer *pGlobalEnd;
	FDFSStorageServer targetServer;
	FDFSStorageServer *pTargetServer;
	FDFSStorageBrief diffServers[FDFS_MAX_SERVERS_EACH_GROUP];
	FDFSStorageBrief *pDiffServer;
	int res;
	int result;
	int nDeletedCount;

	memset(&targetServer, 0, sizeof(targetServer));
	pTargetServer = &targetServer;

	nDeletedCount = 0;
	pDiffServer = diffServers;
	pEnd = briefServers + server_count;
	for (pServer=briefServers; pServer<pEnd; pServer++)
	{
		memcpy(&(targetServer.server),pServer,sizeof(FDFSStorageBrief));

		ppFound = (FDFSStorageServer **)bsearch(&pTargetServer, \
			g_sorted_storages, g_storage_count, \
			sizeof(FDFSStorageServer *), storage_cmp_by_ip_addr);
		if (ppFound != NULL)
		{
			if ((*ppFound)->server.status != pServer->status)
			{
				if ((((pServer->status == \
					FDFS_STORAGE_STATUS_WAIT_SYNC) || \
					(pServer->status == \
					FDFS_STORAGE_STATUS_SYNCING)) && \
					((*ppFound)->server.status > \
						pServer->status)) \
					 || ((*ppFound)->server.status == \
						FDFS_STORAGE_STATUS_DELETED))
				{
					memcpy(pDiffServer++, \
						&((*ppFound)->server), \
						sizeof(FDFSStorageBrief));
				}
				else if ((*ppFound)->server.status == \
					FDFS_STORAGE_STATUS_NONE && \
					pServer->status == \
					FDFS_STORAGE_STATUS_DELETED) //ignore
				{
				}
				else
				{
					if ((*ppFound)->server.status == \
						FDFS_STORAGE_STATUS_NONE && \
						pServer->status != \
						FDFS_STORAGE_STATUS_DELETED)
					{
						(*ppFound)->server.status = \
							pServer->status;
					if ((result=storage_sync_thread_start( \
						&((*ppFound)->server))) != 0)
						{
							return result;
						}
					}
					else
					{
						(*ppFound)->server.status = \
							pServer->status;
					}
				}
			}
		}
		else if (pServer->status == FDFS_STORAGE_STATUS_DELETED)//ignore
		{
			nDeletedCount++;
		}
		else
		{
			if (g_storage_count < FDFS_MAX_SERVERS_EACH_GROUP)
			{
				if ((result=pthread_mutex_lock( \
					&reporter_thread_lock)) != 0)
				{
					logError("file: "__FILE__", line: %d, "\
						"call pthread_mutex_lock fail,"\
						" errno: %d, error info: %s", \
						__LINE__, \
						result, strerror(result));
				}
				pInsertedServer = g_storage_servers + \
						g_storage_count;
				memcpy(&(pInsertedServer->server), \
					pServer, sizeof(FDFSStorageBrief));
				if (tracker_insert_into_sorted_servers( \
						pInsertedServer))
				{
					g_storage_count++;
				}
				if ((result=pthread_mutex_unlock( \
					&reporter_thread_lock)) != 0)
				{
					logError("file: "__FILE__", line: %d, "\
					"call pthread_mutex_unlock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
				}

				if ((result=storage_sync_thread_start( \
					&(pInsertedServer->server))) != 0)
				{
					return result;
				}
			}
			else
			{
				logError("file: "__FILE__", line: %d, " \
					"tracker server %s:%d, " \
					"storage servers of group \"%s\" " \
					"exceeds max: %d", \
					__LINE__, pTrackerServer->ip_addr, \
					pTrackerServer->port, \
					pTrackerServer->group_name, \
					FDFS_MAX_SERVERS_EACH_GROUP);
			}
		}
	}

	if (g_storage_count + nDeletedCount == server_count)
	{
		if (pDiffServer - diffServers > 0)
		{
			return tracker_sync_diff_servers(pTrackerServer, \
				diffServers, pDiffServer - diffServers);
		}

		return 0;
	}

	pGlobalServer = g_storage_servers;
	pGlobalEnd = g_storage_servers + g_storage_count;
	pServer = briefServers;
	while (pServer < pEnd && pGlobalServer < pGlobalEnd)
	{
		if (pGlobalServer->server.status == FDFS_STORAGE_STATUS_NONE)
		{
			pGlobalServer++;
			continue;
		}

		res = strcmp(pServer->ip_addr, pGlobalServer->server.ip_addr);
		if (res < 0)
		{
			if (pServer->status != FDFS_STORAGE_STATUS_DELETED)
			{
			logError("file: "__FILE__", line: %d, " \
				"tracker server %s:%d, " \
				"group \"%s\", " \
				"enter impossible statement branch", \
				__LINE__, pTrackerServer->ip_addr, \
				pTrackerServer->port, \
				pTrackerServer->group_name
			);
			}

			pServer++;
		}
		else if (res == 0)
		{
			pServer++;
			pGlobalServer++;
		}
		else
		{
			memcpy(pDiffServer++, &(pGlobalServer->server), \
				sizeof(FDFSStorageBrief));
			pGlobalServer++;
		}
	}

	while (pGlobalServer < pGlobalEnd)
	{
		if (pGlobalServer->server.status == FDFS_STORAGE_STATUS_NONE)
		{
			pGlobalServer++;
			continue;
		}

		memcpy(pDiffServer++, &(pGlobalServer->server), \
			sizeof(FDFSStorageBrief));
		pGlobalServer++;
	}

	return tracker_sync_diff_servers(pTrackerServer, \
			diffServers, pDiffServer - diffServers);
}

static int tracker_check_response(TrackerServerInfo *pTrackerServer)
{
	int64_t nInPackLen;
	TrackerHeader resp;
	int server_count;
	int result;
	FDFSStorageBrief briefServers[FDFS_MAX_SERVERS_EACH_GROUP];

	if ((result=tcprecvdata(pTrackerServer->sock, &resp, \
			sizeof(resp), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, recv data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port,    \
			result, strerror(result));
		return result;
	}

	//printf("resp status=%d\n", resp.status);
	if (resp.status != 0)
	{
		return resp.status;
	}

	nInPackLen = buff2long(resp.pkg_len);
	if ((nInPackLen < 0) || (nInPackLen % sizeof(FDFSStorageBrief) != 0))
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, " \
			"package size "INT64_PRINTF_FORMAT" is not correct", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, nInPackLen);
		return EINVAL;
	}
	if (nInPackLen == 0)
	{
		return resp.status;
	}

	server_count = nInPackLen / sizeof(FDFSStorageBrief);
	if (server_count > FDFS_MAX_SERVERS_EACH_GROUP)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, return storage count: %d" \
			" exceed max: %d", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			server_count, FDFS_MAX_SERVERS_EACH_GROUP);
		return EINVAL;
	}

	if ((result=tcprecvdata(pTrackerServer->sock, briefServers, \
			nInPackLen, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, recv data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, strerror(result));
		return result;
	}

	/*
	//printf("resp server count=%d\n", server_count);
	{
		int i;
		for (i=0; i<server_count; i++)
		{	
			//printf("%d. %d:%s\n", i+1, briefServers[i].status, \
				briefServers[i].ip_addr);
		}
	}
	*/

	return tracker_merge_servers(pTrackerServer, \
                briefServers, server_count);
}

int tracker_sync_src_req(TrackerServerInfo *pTrackerServer, \
			BinLogReader *pReader)
{
	char out_buff[sizeof(TrackerHeader) + IP_ADDRESS_SIZE];
	char sync_src_ip_addr[IP_ADDRESS_SIZE];
	TrackerHeader *pHeader;
	TrackerStorageSyncReqBody syncReqbody;
	char *pBuff;
	int64_t in_bytes;
	int result;

	memset(out_buff, 0, sizeof(out_buff));
	pHeader = (TrackerHeader *)out_buff;
	long2buff(IP_ADDRESS_SIZE, pHeader->pkg_len);
	pHeader->cmd = TRACKER_PROTO_CMD_STORAGE_SYNC_SRC_REQ;
	strcpy(out_buff + sizeof(TrackerHeader), pReader->ip_addr);
	if ((result=tcpsenddata(pTrackerServer->sock, out_buff, \
			sizeof(out_buff), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, strerror(result));
		return result;
	}

	pBuff = (char *)&syncReqbody;
	if ((result=fdfs_recv_response(pTrackerServer, \
                &pBuff, sizeof(syncReqbody), &in_bytes)) != 0)
	{
		return result;
	}

	if (in_bytes == 0)
	{
		pReader->need_sync_old = false;
        	pReader->until_timestamp = 0;

		return 0;
	}

	if (in_bytes != sizeof(syncReqbody))
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, " \
			"recv body length: "INT64_PRINTF_FORMAT" is invalid, " \
			"expect body length: %d", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, in_bytes, \
			sizeof(syncReqbody));
		return EINVAL;
	}

	memcpy(sync_src_ip_addr, syncReqbody.src_ip_addr, IP_ADDRESS_SIZE);
	sync_src_ip_addr[IP_ADDRESS_SIZE-1] = '\0';

	pReader->need_sync_old = is_local_host_ip(sync_src_ip_addr);
       	pReader->until_timestamp = (time_t)buff2long( \
					syncReqbody.until_timestamp);

	return 0;
}

static int tracker_sync_dest_req(TrackerServerInfo *pTrackerServer)
{
	TrackerHeader header;
	TrackerStorageSyncReqBody syncReqbody;
	char *pBuff;
	int64_t in_bytes;
	int result;

	memset(&header, 0, sizeof(header));
	header.cmd = TRACKER_PROTO_CMD_STORAGE_SYNC_DEST_REQ;
	if ((result=tcpsenddata(pTrackerServer->sock, &header, \
			sizeof(header), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, strerror(result));
		return result;
	}

	pBuff = (char *)&syncReqbody;
	if ((result=fdfs_recv_response(pTrackerServer, \
                &pBuff, sizeof(syncReqbody), &in_bytes)) != 0)
	{
		return result;
	}

	if (in_bytes == 0)
	{
		return result;
	}

	if (in_bytes != sizeof(syncReqbody))
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, " \
			"recv body length: "INT64_PRINTF_FORMAT" is invalid, " \
			"expect body length: %d", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, in_bytes, \
			sizeof(syncReqbody));
		return EINVAL;
	}

	memcpy(g_sync_src_ip_addr, syncReqbody.src_ip_addr, IP_ADDRESS_SIZE);
	g_sync_src_ip_addr[IP_ADDRESS_SIZE-1] = '\0';

	g_sync_until_timestamp = (time_t)buff2long(syncReqbody.until_timestamp);

	memset(&header, 0, sizeof(header));
	header.cmd = TRACKER_PROTO_CMD_STORAGE_RESP;
	if ((result=tcpsenddata(pTrackerServer->sock, &header, sizeof(header), \
				g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, strerror(result));
		return result;
	}

	return 0;
}

static int tracker_sync_notify(TrackerServerInfo *pTrackerServer)
{
	char out_buff[sizeof(TrackerHeader)+sizeof(TrackerStorageSyncReqBody)];
	TrackerHeader *pHeader;
	TrackerStorageSyncReqBody *pReqBody;
	int result;

	pHeader = (TrackerHeader *)out_buff;
	pReqBody = (TrackerStorageSyncReqBody*)(out_buff+sizeof(TrackerHeader));

	memset(out_buff, 0, sizeof(out_buff));
	long2buff((int)sizeof(TrackerStorageSyncReqBody), pHeader->pkg_len);
	pHeader->cmd = TRACKER_PROTO_CMD_STORAGE_SYNC_NOTIFY;
	strcpy(pReqBody->src_ip_addr, g_sync_src_ip_addr);
	long2buff(g_sync_until_timestamp, pReqBody->until_timestamp);

	if ((result=tcpsenddata(pTrackerServer->sock, out_buff, \
			sizeof(out_buff), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, strerror(result));
		return result;
	}

	return tracker_check_response(pTrackerServer);
}

int tracker_report_join(TrackerServerInfo *pTrackerServer)
{
	char out_buff[sizeof(TrackerHeader)+sizeof(TrackerStorageJoinBody)];
	TrackerHeader *pHeader;
	TrackerStorageJoinBody *pReqBody;
	int result;

	pHeader = (TrackerHeader *)out_buff;
	pReqBody = (TrackerStorageJoinBody *)(out_buff+sizeof(TrackerHeader));

	memset(out_buff, 0, sizeof(out_buff));
	long2buff((int)sizeof(TrackerStorageJoinBody), pHeader->pkg_len);
	pHeader->cmd = TRACKER_PROTO_CMD_STORAGE_JOIN;
	strcpy(pReqBody->group_name, g_group_name);
	long2buff(g_server_port, pReqBody->storage_port);
	long2buff(g_path_count, pReqBody->store_path_count);
	long2buff(g_subdir_count_per_path, pReqBody->subdir_count_per_path);

	if ((result=tcpsenddata(pTrackerServer->sock, out_buff, \
			sizeof(out_buff), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, strerror(result));
		return result;
	}

	return tracker_check_response(pTrackerServer);
}

static int tracker_report_sync_timestamp(TrackerServerInfo *pTrackerServer)
{
	char out_buff[sizeof(TrackerHeader) + (IP_ADDRESS_SIZE + 4) * \
			FDFS_MAX_SERVERS_EACH_GROUP];
	char *p;
	TrackerHeader *pHeader;
	FDFSStorageServer *pServer;
	FDFSStorageServer *pEnd;
	int result;
	int body_len;

	if (g_storage_count == 0)
	{
		return 0;
	}

	memset(out_buff, 0, sizeof(out_buff));
	pHeader = (TrackerHeader *)out_buff;
	p = out_buff + sizeof(TrackerHeader);

	body_len = (IP_ADDRESS_SIZE + 4) * g_storage_count;
	pHeader->cmd = TRACKER_PROTO_CMD_STORAGE_SYNC_REPORT;
	long2buff(body_len, pHeader->pkg_len);

	pEnd = g_storage_servers + g_storage_count;
	for (pServer=g_storage_servers; pServer<pEnd; pServer++)
	{
		memcpy(p, pServer->server.ip_addr, IP_ADDRESS_SIZE);
		p += IP_ADDRESS_SIZE;
		int2buff(pServer->last_sync_src_timestamp, p);
		p += 4;
	}

	if((result=tcpsenddata(pTrackerServer->sock, out_buff, \
		sizeof(TrackerHeader) + body_len, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, strerror(result));
		return result;
	}

	return tracker_check_response(pTrackerServer);
}

static int tracker_report_df_stat(TrackerServerInfo *pTrackerServer)
{
	char out_buff[sizeof(TrackerHeader) + \
			sizeof(TrackerStatReportReqBody) * 16];
	char *pBuff;
	TrackerHeader *pHeader;
	TrackerStatReportReqBody *pStatBuff;
	struct statfs sbuf;
	int body_len;
	int total_len;
	int i;
	int result;

	body_len = (int)sizeof(TrackerStatReportReqBody) * g_path_count;
	total_len = (int)sizeof(TrackerHeader) + body_len;
	if (total_len <= sizeof(out_buff))
	{
		pBuff = out_buff;
	}
	else
	{
		pBuff = (char *)malloc(total_len);
		if (pBuff == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", \
				__LINE__, total_len, \
				errno, strerror(errno));
			return errno != 0 ? errno : ENOMEM;
		}
	}

	pHeader = (TrackerHeader *)pBuff;
	pStatBuff = (TrackerStatReportReqBody*) \
			(pBuff + sizeof(TrackerHeader));
	long2buff(body_len, pHeader->pkg_len);
	pHeader->cmd = TRACKER_PROTO_CMD_STORAGE_REPORT;
	pHeader->status = 0;

	for (i=0; i<g_path_count; i++)
	{
		if (statfs(g_store_paths[i], &sbuf) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call statfs fail, errno: %d, error info: %s.",\
				__LINE__, errno, strerror(errno));

			if (pBuff != out_buff)
			{
				free(pBuff);
			}
			return errno != 0 ? errno : EACCES;
		}

		long2buff((((int64_t)(sbuf.f_blocks) * sbuf.f_bsize) / FDFS_ONE_MB),\
			pStatBuff->sz_total_mb);
		long2buff((((int64_t)(sbuf.f_bavail) * sbuf.f_bsize) / FDFS_ONE_MB),\
			pStatBuff->sz_free_mb);

		pStatBuff += sizeof(TrackerStatReportReqBody);
	}

	result = tcpsenddata(pTrackerServer->sock, pBuff, \
			total_len, g_network_timeout);
	if (pBuff != out_buff)
	{
		free(pBuff);
	}
	if(result != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, strerror(result));
		return result;
	}

	return tracker_check_response(pTrackerServer);
}

static int tracker_heart_beat(TrackerServerInfo *pTrackerServer, \
			int *pstat_chg_sync_count)
{
	char out_buff[sizeof(TrackerHeader) + sizeof(FDFSStorageStatBuff)];
	TrackerHeader *pHeader;
	FDFSStorageStatBuff *pStatBuff;
	int body_len;
	int result;

	pHeader = (TrackerHeader *)out_buff;
	if (*pstat_chg_sync_count != g_stat_change_count)
	{
		pStatBuff = (FDFSStorageStatBuff *)( \
				out_buff + sizeof(TrackerHeader));
		long2buff(g_storage_stat.total_upload_count, \
			pStatBuff->sz_total_upload_count);
		long2buff(g_storage_stat.success_upload_count, \
			pStatBuff->sz_success_upload_count);
		long2buff(g_storage_stat.total_download_count, \
			pStatBuff->sz_total_download_count);
		long2buff(g_storage_stat.success_download_count, \
			pStatBuff->sz_success_download_count);
		long2buff(g_storage_stat.total_set_meta_count, \
			pStatBuff->sz_total_set_meta_count);
		long2buff(g_storage_stat.success_set_meta_count, \
			pStatBuff->sz_success_set_meta_count);
		long2buff(g_storage_stat.total_delete_count, \
			pStatBuff->sz_total_delete_count);
		long2buff(g_storage_stat.success_delete_count, \
			pStatBuff->sz_success_delete_count);
		long2buff(g_storage_stat.total_get_meta_count, \
			pStatBuff->sz_total_get_meta_count);
		long2buff(g_storage_stat.success_get_meta_count, \
		 	pStatBuff->sz_success_get_meta_count);
		long2buff(g_storage_stat.last_source_update, \
			pStatBuff->sz_last_source_update);
		long2buff(g_storage_stat.last_sync_update, \
			pStatBuff->sz_last_sync_update);

		*pstat_chg_sync_count = g_stat_change_count;
		body_len = sizeof(FDFSStorageStatBuff);
	}
	else
	{
		body_len = 0;
	}

	long2buff(body_len, pHeader->pkg_len);
	pHeader->cmd = TRACKER_PROTO_CMD_STORAGE_BEAT;
	pHeader->status = 0;

	if((result=tcpsenddata(pTrackerServer->sock, out_buff, \
		sizeof(TrackerHeader) + body_len, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server %s:%d, send data fail, " \
			"errno: %d, error info: %s.", \
			__LINE__, pTrackerServer->ip_addr, \
			pTrackerServer->port, \
			result, strerror(result));
		return result;
	}

	return tracker_check_response(pTrackerServer);
}

int tracker_report_thread_start()
{
	TrackerServerInfo *pTrackerServer;
	TrackerServerInfo *pServerEnd;
	pthread_attr_t pattr;
	pthread_t tid;
	int result;

	if ((result=init_pthread_attr(&pattr)) != 0)
	{
		return result;
	}

	report_tids = (pthread_t *)malloc(sizeof(pthread_t) * \
					g_tracker_server_count);
	if (report_tids == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, sizeof(pthread_t) * \
			g_tracker_server_count, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	g_tracker_reporter_count = 0;
	pServerEnd = g_tracker_servers + g_tracker_server_count;
	for (pTrackerServer=g_tracker_servers; pTrackerServer<pServerEnd; \
		pTrackerServer++)
	{
		if((result=pthread_create(&tid, &pattr, \
			tracker_report_thread_entrance, pTrackerServer)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"create thread failed, errno: %d, " \
				"error info: %s.", \
				__LINE__, result, strerror(result));
			return result;
		}

		if ((result=pthread_mutex_lock(&reporter_thread_lock)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_lock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
		}

		report_tids[g_tracker_reporter_count] = tid;
		g_tracker_reporter_count++;
		if ((result=pthread_mutex_unlock(&reporter_thread_lock)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_unlock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
		}
	}

	pthread_attr_destroy(&pattr);

	return 0;
}

