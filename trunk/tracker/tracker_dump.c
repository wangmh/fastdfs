/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include "tracker_dump.h"
#include "shared_func.h"

/*
bool g_continue_flag = true;
int g_server_port = FDFS_TRACKER_SERVER_DEF_PORT;
int g_max_connections = DEFAULT_MAX_CONNECTONS;
int g_sync_log_buff_interval = SYNC_LOG_BUFF_DEF_INTERVAL;
int g_check_active_interval = CHECK_ACTIVE_DEF_INTERVAL;

FDFSGroups g_groups;
int g_storage_stat_chg_count = 0;
int g_storage_sync_time_chg_count = 0; //sync timestamp
int g_storage_reserved_mb = 0;

int g_allow_ip_count = 0;
in_addr_t *g_allow_ip_addrs = NULL;

struct base64_context g_base64_context;
char g_run_by_group[32] = {0};
char g_run_by_user[32] = {0};

bool g_storage_ip_changed_auto_adjust = true;
bool g_thread_kill_done = false;

int g_thread_stack_size = 64 * 1024;

#ifdef WITH_HTTPD
FDFSHTTPParams g_http_params;
int g_http_check_interval = 30;
int g_http_check_type = FDFS_HTTP_CHECK_ALIVE_TYPE_TCP;
char g_http_check_uri[128] = {0};
bool g_http_servers_dirty = false;
#endif

#if defined(DEBUG_FLAG) && defined(OS_LINUX)
char g_exe_name[256] = {0};
#endif
*/

int fdfs_dump_group_stat(FDFSGroupInfo *pGroup, char *buff, const int buffSize)
{
	char szLastSourceUpdate[32];
	char szLastSyncUpdate[32];
	char szSyncedTimestamp[32];
	int total_len;
	FDFSStorageDetail *pServer;
	FDFSStorageDetail *pServerEnd;
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppServerEnd;
	int i;
	int j;

	total_len += snprintf(buff, buffSize, 
		"group_name=%s\n"
		"dirty=%d\n"
		"free_mb="INT64_PRINTF_FORMAT"\n"
		"alloc_size=%d\n"
		"server count=%d\n"
		"active server count=%d\n"
		"storage_port=%d\n"
		"storage_http_port=%d\n"
		"current_read_server=%d\n"
		"current_write_server=%d\n"
		"store_path_count=%d\n"
		"subdir_count_per_path=%d\n" 
		"pStoreServer=%s\n" 
		"ref_count=%d\n"
		"chg_count=%d\n"
		"last_source_update=%s\n"
		"last_sync_update=%s\n",
		pGroup->group_name, 
		pGroup->dirty, 
		pGroup->free_mb, 
		pGroup->alloc_size, 
		pGroup->count, 
		pGroup->active_count, 
		pGroup->storage_port,
		pGroup->storage_http_port, 
		pGroup->current_read_server, 
		pGroup->current_write_server, 
		pGroup->store_path_count,
		pGroup->subdir_count_per_path,
		pGroup->pStoreServer != NULL ? pGroup->pStoreServer->ip_addr : "",
		*(pGroup->ref_count),
		pGroup->chg_count,
		formatDatetime(pGroup->last_source_update, 
			"%Y-%m-%d %H:%M:%S", 
			szLastSourceUpdate, sizeof(szLastSourceUpdate)),
		formatDatetime(pGroup->last_sync_update, 
			"%Y-%m-%d %H:%M:%S", 
			szLastSyncUpdate, sizeof(szLastSyncUpdate))
	);


	total_len += snprintf(buff + total_len, buffSize - total_len, 
		"total server count=%d\n", pGroup->count);
	pServerEnd = pGroup->all_servers + pGroup->count;
	for (pServer=pGroup->all_servers; pServer<pServerEnd; pServer++)
	{
		total_len += snprintf(buff + total_len, buffSize - total_len, 
			"\t%s\n", pServer->ip_addr);
	}

	total_len += snprintf(buff + total_len, buffSize - total_len, 
		"\nactive server count=%d\n", pGroup->active_count);
	ppServerEnd = pGroup->active_servers + pGroup->active_count;
	for (ppServer=pGroup->active_servers; ppServer<ppServerEnd; ppServer++)
	{
		total_len += snprintf(buff + total_len, buffSize - total_len, 
			"\t%s\n", (*ppServer)->ip_addr);
	}

#ifdef WITH_HTTPD
	total_len += snprintf(buff + total_len, buffSize - total_len, 
		"\nhttp active server count=%d\n"
		"current_http_server=%d\n", 
		pGroup->http_server_count,
		pGroup->current_http_server);

	ppServerEnd = pGroup->http_servers + pGroup->http_server_count;
	for (ppServer=pGroup->http_servers; ppServer<ppServerEnd; ppServer++)
	{
		total_len += snprintf(buff + total_len, buffSize - total_len, 
			"\t%s\n", (*ppServer)->ip_addr);
	}
#endif

	ppServerEnd = pGroup->sorted_servers + pGroup->count;
	for (ppServer=pGroup->sorted_servers; ppServer<ppServerEnd; ppServer++)
	{
		total_len += snprintf(buff + total_len, buffSize - total_len, 
				"\n");
		total_len += fdfs_dump_storage_stat(*ppServer, buff + total_len,
					 buffSize - total_len);
	}

	total_len += snprintf(buff + total_len, buffSize - total_len, 
			"\nsynced timestamp table:\n");
	for (i=0; i<pGroup->count; i++)
	{
	for (j=0; j<pGroup->count; j++)
	{
		if (i == j)
		{
			continue;
		}

		total_len += snprintf(buff + total_len, buffSize - total_len, 
				"\t%s => %s: %s\n", 
				pGroup->all_servers[i].ip_addr, 
				pGroup->all_servers[j].ip_addr,
				formatDatetime(pGroup->last_sync_timestamps[i][j],
					"%Y-%m-%d %H:%M:%S", 
					szSyncedTimestamp, 
					sizeof(szSyncedTimestamp))
				);
	}
	}

	total_len += snprintf(buff + total_len, buffSize - total_len, 
			"\n\n");
	return total_len;
}

int fdfs_dump_storage_stat(FDFSStorageDetail *pServer, 
		char *buff, const int buffSize)
{
	char szUpTime[32];
	char szLastHeartBeatTime[32];
	char szSrcUpdTime[32];
	char szSyncUpdTime[32];
	char szSyncedTimestamp[32];
	char szSyncUntilTimestamp[32];
	int i;
	int total_len;

	total_len = snprintf(buff, buffSize, 
		"ip_addr=%s\n"
		"version=%s\n"
		"status=%d\n"
		"dirty=%d\n"
		"domain_name=%s\n"
		"sync_src_server=%s\n"
		"sync_until_timestamp=%s\n"
		"up_time=%s\n"
		"total_mb="INT64_PRINTF_FORMAT" MB\n"
		"free_mb="INT64_PRINTF_FORMAT" MB\n"
		"changelog_offset="INT64_PRINTF_FORMAT"\n"
		"store_path_count=%d\n"
		"subdir_count_per_path=%d\n"
		"upload_priority=%d\n"
		"current_write_path=%d\n"
		"ref_count=%d\n"
		"chg_count=%d\n"
#ifdef WITH_HTTPD
		"http_check_last_errno=%d\n"
		"http_check_last_status=%d\n"
		"http_check_fail_count=%d\n"
		"http_check_error_info=%s\n"
#endif

		"total_upload_count="INT64_PRINTF_FORMAT"\n"
		"success_upload_count="INT64_PRINTF_FORMAT"\n"
		"total_set_meta_count="INT64_PRINTF_FORMAT"\n"
		"success_set_meta_count="INT64_PRINTF_FORMAT"\n"
		"total_delete_count="INT64_PRINTF_FORMAT"\n"
		"success_delete_count="INT64_PRINTF_FORMAT"\n"
		"total_download_count="INT64_PRINTF_FORMAT"\n"
		"success_download_count="INT64_PRINTF_FORMAT"\n"
		"total_get_meta_count="INT64_PRINTF_FORMAT"\n"
		"success_get_meta_count="INT64_PRINTF_FORMAT"\n"
		"total_create_link_count="INT64_PRINTF_FORMAT"\n"
		"success_create_link_count="INT64_PRINTF_FORMAT"\n"
		"total_delete_link_count="INT64_PRINTF_FORMAT"\n"
		"success_delete_link_count="INT64_PRINTF_FORMAT"\n"
		"last_source_update=%s\n"
		"last_sync_update=%s\n"
		"last_synced_timestamp=%s\n"
		"last_heart_beat_time=%s\n",
		pServer->ip_addr, 
		pServer->version, 
		pServer->status, 
		pServer->dirty, 
		pServer->domain_name, 
		pServer->psync_src_server != NULL ? 
		pServer->psync_src_server->ip_addr : "", 
		formatDatetime(pServer->sync_until_timestamp, 
			"%Y-%m-%d %H:%M:%S", 
			szSyncUntilTimestamp, sizeof(szSyncUntilTimestamp)),
		formatDatetime(pServer->up_time, 
			"%Y-%m-%d %H:%M:%S", 
			szUpTime, sizeof(szUpTime)),
		pServer->total_mb,
		pServer->free_mb, 
		pServer->changelog_offset, 
		pServer->store_path_count, 
		pServer->subdir_count_per_path, 
		pServer->upload_priority,
		pServer->current_write_path,
		*(pServer->ref_count),
		pServer->chg_count,
#ifdef WITH_HTTPD
		pServer->http_check_last_errno,
		pServer->http_check_last_status,
		pServer->http_check_fail_count,
		pServer->http_check_error_info,
#endif
		pServer->stat.total_upload_count,
		pServer->stat.success_upload_count,
		pServer->stat.total_set_meta_count,
		pServer->stat.success_set_meta_count,
		pServer->stat.total_delete_count,
		pServer->stat.success_delete_count,
		pServer->stat.total_download_count,
		pServer->stat.success_download_count,
		pServer->stat.total_get_meta_count,
		pServer->stat.success_get_meta_count,
		pServer->stat.total_create_link_count,
		pServer->stat.success_create_link_count,
		pServer->stat.total_delete_link_count,
		pServer->stat.success_delete_link_count,
		formatDatetime(pServer->stat.last_source_update, 
			"%Y-%m-%d %H:%M:%S", 
			szSrcUpdTime, sizeof(szSrcUpdTime)), 
		formatDatetime(pServer->stat.last_sync_update,
			"%Y-%m-%d %H:%M:%S", 
			szSyncUpdTime, sizeof(szSyncUpdTime)), 
		formatDatetime(pServer->stat.last_synced_timestamp, 
			"%Y-%m-%d %H:%M:%S", 
			szSyncedTimestamp, sizeof(szSyncedTimestamp)),
		formatDatetime(pServer->stat.last_heart_beat_time,
			"%Y-%m-%d %H:%M:%S", 
			szLastHeartBeatTime, sizeof(szLastHeartBeatTime))
	);

	for (i=0; i<pServer->store_path_count; i++)
	{
		total_len += snprintf(buff + total_len, buffSize - total_len, 
			"disk %d: total_mb="INT64_PRINTF_FORMAT" MB, "
			"free_mb="INT64_PRINTF_FORMAT" MB\n",
			i+1, pServer->path_total_mbs[i],
			pServer->path_free_mbs[i]);
	}

	return total_len;
}

