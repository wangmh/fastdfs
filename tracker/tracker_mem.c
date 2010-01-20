/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <pthread.h>
#include "fdfs_define.h"
#include "logger.h"
#include "fdfs_global.h"
#include "tracker_global.h"
#include "tracker_proto.h"
#include "tracker_mem.h"
#include "shared_func.h"

#define TRACKER_MEM_ALLOC_ONCE	2

static pthread_mutex_t mem_thread_lock;
static pthread_mutex_t mem_file_lock;

off_t g_changelog_fsize = 0; //storage server change log file size
static int changelog_fd = -1;  //storage server change log fd for write

static void tracker_mem_find_store_server(FDFSGroupInfo *pGroup);

int tracker_mem_pthread_lock()
{
	int result;
	if ((result=pthread_mutex_lock(&mem_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	return 0;
}

int tracker_mem_pthread_unlock()
{
	int result;
	if ((result=pthread_mutex_unlock(&mem_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	return 0;
}

static int tracker_mem_file_lock()
{
	int result;
	if ((result=pthread_mutex_lock(&mem_file_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	return 0;
}

static int tracker_mem_file_unlock()
{
	int result;
	if ((result=pthread_mutex_unlock(&mem_file_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	return 0;
}

static int tracker_write_to_changelog(FDFSGroupInfo *pGroup, \
		FDFSStorageDetail *pStorage, const char *pArg)
{
	char buff[256];
	int len;
	int result;

	tracker_mem_file_lock();

	len = snprintf(buff, sizeof(buff), "%d %s %s %d %s\n", \
		(int)time(NULL), pGroup->group_name, pStorage->ip_addr, \
		pStorage->status, pArg != NULL ? pArg : "");

	if (write(changelog_fd, buff, len) != len)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"write to file: %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, STORAGE_SERVERS_CHANGELOG_FILENAME, \
			result, strerror(result));

		tracker_mem_file_unlock();
		return result;
	}

	g_changelog_fsize += len;
	result = fsync(changelog_fd);
	if (result != 0)
	{
		result = errno != 0 ? errno : EIO;
		logError("file: "__FILE__", line: %d, " \
			"call fsync of file: %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, STORAGE_SERVERS_CHANGELOG_FILENAME, \
			result, strerror(result));
	}

	tracker_mem_file_unlock();

	return result;
}

static int tracker_malloc_storage_path_mbs(FDFSStorageDetail *pStorage, \
		const int store_path_count)
{
	int alloc_bytes;

	if (store_path_count <= 0)
	{
		return 0;
	}

	alloc_bytes = sizeof(int64_t) * store_path_count;

	pStorage->path_total_mbs = (int64_t *)malloc(alloc_bytes);
	if (pStorage->path_total_mbs == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, alloc_bytes, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	pStorage->path_free_mbs = (int64_t *)malloc(alloc_bytes);
	if (pStorage->path_free_mbs == NULL)
	{
		free(pStorage->path_total_mbs);
		pStorage->path_total_mbs = NULL;

		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, alloc_bytes, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	memset(pStorage->path_total_mbs, 0, alloc_bytes);
	memset(pStorage->path_free_mbs, 0, alloc_bytes);

	return 0;
}

static int tracker_realloc_storage_path_mbs(FDFSStorageDetail *pStorage, \
		const int old_store_path_count, const int new_store_path_count)
{
	int alloc_bytes;
	int copy_bytes;
	int64_t *new_path_total_mbs;
	int64_t *new_path_free_mbs;

	if (new_store_path_count <= 0)
	{
		return EINVAL;
	}

	alloc_bytes = sizeof(int64_t) * new_store_path_count;

	new_path_total_mbs = (int64_t *)malloc(alloc_bytes);
	if (new_path_total_mbs == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, alloc_bytes, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	new_path_free_mbs = (int64_t *)malloc(alloc_bytes);
	if (new_path_free_mbs == NULL)
	{
		free(new_path_total_mbs);

		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, alloc_bytes, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	memset(new_path_total_mbs, 0, alloc_bytes);
	memset(new_path_free_mbs, 0, alloc_bytes);

	if (old_store_path_count == 0)
	{
		pStorage->path_total_mbs = new_path_total_mbs;
		pStorage->path_free_mbs = new_path_free_mbs;

		return 0;
	}

	copy_bytes = (old_store_path_count < new_store_path_count ? \
		old_store_path_count : new_store_path_count) * sizeof(int64_t);
	memcpy(new_path_total_mbs, pStorage->path_total_mbs, copy_bytes);
	memcpy(new_path_free_mbs, pStorage->path_free_mbs, copy_bytes);

	free(pStorage->path_total_mbs);
	free(pStorage->path_free_mbs);

	pStorage->path_total_mbs = new_path_total_mbs;
	pStorage->path_free_mbs = new_path_free_mbs;

	return 0;
}

static int tracker_realloc_group_path_mbs(FDFSGroupInfo *pGroup, \
		const int new_store_path_count)
{
	FDFSStorageDetail *pStorage;
	FDFSStorageDetail *pEnd;
	int result;

	pEnd = pGroup->all_servers + pGroup->alloc_size;
	for (pStorage=pGroup->all_servers; pStorage<pEnd; pStorage++)
	{
		if ((result=tracker_realloc_storage_path_mbs(pStorage, \
			pGroup->store_path_count, new_store_path_count)) != 0)
		{
			return result;
		}
	}

	pGroup->store_path_count = new_store_path_count;

	return 0;
}

static int tracker_malloc_group_path_mbs(FDFSGroupInfo *pGroup)
{
	FDFSStorageDetail *pStorage;
	FDFSStorageDetail *pEnd;
	int result;

	pEnd = pGroup->all_servers + pGroup->alloc_size;
	for (pStorage=pGroup->all_servers; pStorage<pEnd; pStorage++)
	{
		if ((result=tracker_malloc_storage_path_mbs(pStorage, \
				pGroup->store_path_count)) != 0)
		{
			return result;
		}
	}

	return 0;
}

static int tracker_malloc_all_group_path_mbs()
{
	FDFSGroupInfo *pGroup;
	FDFSGroupInfo *pEnd;
	int result;

	pEnd = g_groups.groups + g_groups.alloc_size;
	for (pGroup=g_groups.groups; pGroup<pEnd; pGroup++)
	{
		if (pGroup->store_path_count == 0)
		{
			continue;
		}

		if ((result=tracker_malloc_group_path_mbs(pGroup)) != 0)
		{
			return result;
		}
	}

	return 0;
}

static int tracker_load_groups(const char *data_path)
{
#define STORAGE_DATA_GROUP_FIELDS	4

	FILE *fp;
	char szLine[256];
	char *fields[STORAGE_DATA_GROUP_FIELDS];
	int result;
	int col_count;
	TrackerClientInfo clientInfo;
	bool bInserted;

	if ((fp=fopen(STORAGE_GROUPS_LIST_FILENAME, "r")) == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"open file \"%s/%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, data_path, STORAGE_GROUPS_LIST_FILENAME, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	result = 0;
	while (fgets(szLine, sizeof(szLine), fp) != NULL)
	{
		if (*szLine == '\0')
		{
			continue;
		}

		col_count = splitEx(szLine, STORAGE_DATA_FIELD_SEPERATOR, \
			fields, STORAGE_DATA_GROUP_FIELDS);
		if (col_count != STORAGE_DATA_GROUP_FIELDS && \
			col_count != STORAGE_DATA_GROUP_FIELDS - 2)
		{
			logError("file: "__FILE__", line: %d, " \
				"the format of the file \"%s/%s\" is invalid", \
				__LINE__, data_path, \
				STORAGE_GROUPS_LIST_FILENAME);
			result = errno != 0 ? errno : EINVAL;
			break;
		}
	
		memset(&clientInfo, 0, sizeof(TrackerClientInfo));
		snprintf(clientInfo.group_name, sizeof(clientInfo.group_name),\
				"%s", trim(fields[0]));
		if ((result=tracker_mem_add_group(&clientInfo, \
				false, &bInserted)) != 0)
		{
			break;
		}

		if (!bInserted)
		{
			logError("file: "__FILE__", line: %d, " \
				"in the file \"%s/%s\", " \
				"group \"%s\" is duplicate", \
				__LINE__, data_path, \
				STORAGE_GROUPS_LIST_FILENAME, \
				clientInfo.group_name);
			result = errno != 0 ? errno : EEXIST;
			break;
		}

		clientInfo.pGroup->storage_port = atoi(trim(fields[1]));
		if (col_count == STORAGE_DATA_GROUP_FIELDS - 2)
		{  //version < V1.12
			clientInfo.pGroup->store_path_count = 0;
			clientInfo.pGroup->subdir_count_per_path = 0;
		}
		else
		{
			clientInfo.pGroup->store_path_count = \
				atoi(trim(fields[2]));
			clientInfo.pGroup->subdir_count_per_path = \
				atoi(trim(fields[3]));
		}
	}

	fclose(fp);
	return result;
}

static int tracker_locate_storage_sync_server(FDFSStorageSync *pStorageSyncs, \
		const int nStorageSyncCount, const bool bLoadFromFile)
{
	FDFSGroupInfo *pGroup;
	FDFSStorageDetail *pStorage;
	FDFSStorageSync *pSyncServer;
	FDFSStorageSync *pSyncEnd;

	pSyncEnd = pStorageSyncs + nStorageSyncCount;
	for (pSyncServer=pStorageSyncs; pSyncServer<pSyncEnd; pSyncServer++)
	{
		pGroup = tracker_mem_get_group(pSyncServer->group_name);
		if (pGroup == NULL)
		{
			continue;
		}

		pStorage=tracker_mem_get_storage(pGroup, pSyncServer->ip_addr);
		if (pStorage == NULL)
		{
			continue;
		}

		pStorage->psync_src_server = tracker_mem_get_storage(pGroup, \
			pSyncServer->sync_src_ip_addr);
		if (pStorage->psync_src_server == NULL)
		{
			char buff[MAX_PATH_SIZE+64];
			if (bLoadFromFile)
			{
				snprintf(buff, sizeof(buff), \
					"in the file \"%s/data/%s\", ", \
					g_base_path, \
					STORAGE_SERVERS_LIST_FILENAME);
			}
			else
			{
				buff[0] = '\0';
			}

			logError("file: "__FILE__", line: %d, " \
				"%sgroup_name: %s, storage server \"%s:%d\" " \
				"does not exist", \
				__LINE__, buff, pSyncServer->group_name, \
				pSyncServer->sync_src_ip_addr, \
				pGroup->storage_port);

			return ENOENT;
		}
	}

	return 0;
}

static int tracker_load_storages(const char *data_path)
{
#define STORAGE_DATA_SERVER_FIELDS	18

	FILE *fp;
	char szLine[256];
	char *fields[STORAGE_DATA_SERVER_FIELDS];
	char *psync_src_ip_addr;
	FDFSStorageSync *pStorageSyncs;
	int nStorageSyncSize;
	int nStorageSyncCount;
	int cols;
	int result;
	TrackerClientInfo clientInfo;
	bool bInserted;

	if ((fp=fopen(STORAGE_SERVERS_LIST_FILENAME, "r")) == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"open file \"%s/%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, data_path, STORAGE_SERVERS_LIST_FILENAME, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	nStorageSyncSize = 0;
	nStorageSyncCount = 0;
	pStorageSyncs = NULL;
	result = 0;
	while (fgets(szLine, sizeof(szLine), fp) != NULL)
	{
		if (*szLine == '\0')
		{
			continue;
		}

		cols = splitEx(szLine, STORAGE_DATA_FIELD_SEPERATOR, \
				fields, STORAGE_DATA_SERVER_FIELDS);
		if (cols != STORAGE_DATA_SERVER_FIELDS && \
		    cols != STORAGE_DATA_SERVER_FIELDS - 1)
		{
			logError("file: "__FILE__", line: %d, " \
				"the format of the file \"%s/%s\" is invalid" \
				", colums: %d != expect colums: %d or %d", \
				__LINE__, data_path, \
				STORAGE_SERVERS_LIST_FILENAME, \
				cols, STORAGE_DATA_SERVER_FIELDS, \
				STORAGE_DATA_SERVER_FIELDS - 1);
			result = EINVAL;
			break;
		}
	
		memset(&clientInfo, 0, sizeof(TrackerClientInfo));
		snprintf(clientInfo.group_name, sizeof(clientInfo.group_name),\
				"%s", trim(fields[0]));
		snprintf(clientInfo.ip_addr, sizeof(clientInfo.ip_addr),\
				"%s", trim(fields[1]));
		if ((clientInfo.pGroup=tracker_mem_get_group( \
				clientInfo.group_name)) == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"in the file \"%s/%s\", " \
				"group \"%s\" is not found", \
				__LINE__, data_path, \
				STORAGE_SERVERS_LIST_FILENAME, \
				clientInfo.group_name);
			result = errno != 0 ? errno : ENOENT;
			break;
		}

		if ((result=tracker_mem_add_storage(&clientInfo, \
				false, &bInserted)) != 0)
		{
			break;
		}

		if (!bInserted)
		{
			logError("file: "__FILE__", line: %d, " \
				"in the file \"%s/%s\", " \
				"storage \"%s\" is duplicate", \
				__LINE__, data_path, \
				STORAGE_SERVERS_LIST_FILENAME, \
				clientInfo.ip_addr);
			result = errno != 0 ? errno : EEXIST;
			break;
		}
		
		clientInfo.pStorage->status = atoi(trim_left(fields[2]));
		if (!((clientInfo.pStorage->status == \
				FDFS_STORAGE_STATUS_WAIT_SYNC) || \
			(clientInfo.pStorage->status == \
				FDFS_STORAGE_STATUS_SYNCING) || \
			(clientInfo.pStorage->status == \
				FDFS_STORAGE_STATUS_INIT)))
		{
			clientInfo.pStorage->status = \
				FDFS_STORAGE_STATUS_OFFLINE;
		}

		psync_src_ip_addr = trim(fields[3]);
		clientInfo.pStorage->sync_until_timestamp = atoi( \
					trim_left(fields[4]));
		clientInfo.pStorage->stat.total_upload_count = strtoll( \
					trim_left(fields[5]), NULL, 10);
		clientInfo.pStorage->stat.success_upload_count = strtoll( \
					trim_left(fields[6]), NULL, 10);
		clientInfo.pStorage->stat.total_set_meta_count = strtoll( \
					trim_left(fields[7]), NULL, 10);
		clientInfo.pStorage->stat.success_set_meta_count = strtoll( \
					trim_left(fields[8]), NULL, 10);
		clientInfo.pStorage->stat.total_delete_count = strtoll( \
					trim_left(fields[9]), NULL, 10);
		clientInfo.pStorage->stat.success_delete_count = strtoll( \
					trim_left(fields[10]), NULL, 10);
		clientInfo.pStorage->stat.total_download_count = strtoll( \
					trim_left(fields[11]), NULL, 10);
		clientInfo.pStorage->stat.success_download_count = strtoll( \
					trim_left(fields[12]), NULL, 10);
		clientInfo.pStorage->stat.total_get_meta_count = strtoll( \
					trim_left(fields[13]), NULL, 10);
		clientInfo.pStorage->stat.success_get_meta_count = strtoll( \
					trim_left(fields[14]), NULL, 10);
		clientInfo.pStorage->stat.last_source_update = atoi( \
					trim_left(fields[15]));
		clientInfo.pStorage->stat.last_sync_update = atoi( \
					trim_left(fields[16]));
		if (cols == STORAGE_DATA_SERVER_FIELDS)
		{
			clientInfo.pStorage->changelog_offset = strtoll( \
					trim_left(fields[17]), NULL, 10);
			if (clientInfo.pStorage->changelog_offset < 0)
			{
				clientInfo.pStorage->changelog_offset = 0;
			}
			if (clientInfo.pStorage->changelog_offset > \
				g_changelog_fsize)
			{
				clientInfo.pStorage->changelog_offset = \
					g_changelog_fsize;
			}
		}
		else
		{
			clientInfo.pStorage->changelog_offset = 0;
		}

		if (*psync_src_ip_addr == '\0')
		{
			continue;
		}

		if (nStorageSyncSize <= nStorageSyncCount)
		{
			nStorageSyncSize += 8;
			pStorageSyncs = (FDFSStorageSync *)realloc( \
				pStorageSyncs, \
				sizeof(FDFSStorageSync) * nStorageSyncSize);
			if (pStorageSyncs == NULL)
			{
				result = errno != 0 ? errno : ENOMEM;
				logError("file: "__FILE__", line: %d, " \
					"realloc %d bytes fail", __LINE__, \
					sizeof(FDFSStorageSync) * \
					nStorageSyncSize);
				break;
			}
		}

		strcpy(pStorageSyncs[nStorageSyncCount].group_name, \
			clientInfo.pGroup->group_name);
		strcpy(pStorageSyncs[nStorageSyncCount].ip_addr, \
			clientInfo.pStorage->ip_addr);
		snprintf(pStorageSyncs[nStorageSyncCount].sync_src_ip_addr, \
			IP_ADDRESS_SIZE, "%s", psync_src_ip_addr);

		nStorageSyncCount++;
	}

	fclose(fp);

	if (pStorageSyncs == NULL)
	{
		return result;
	}

	if (result != 0)
	{
		free(pStorageSyncs);
		return result;
	}

	result = tracker_locate_storage_sync_server(pStorageSyncs, \
			nStorageSyncCount, true);
	free(pStorageSyncs);
	return result;
}

static int tracker_load_sync_timestamps(const char *data_path)
{
#define STORAGE_SYNC_TIME_MAX_FIELDS	2 + FDFS_MAX_SERVERS_EACH_GROUP

	FILE *fp;
	char szLine[512];
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char previous_group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	char src_ip_addr[IP_ADDRESS_SIZE];
	char *fields[STORAGE_SYNC_TIME_MAX_FIELDS];
	FDFSGroupInfo *pGroup;
	FDFSGroupInfo *pEnd;
	int cols;
	int src_index;
	int dest_index;
	int curr_synced_timestamp;
	int result;

	if (!fileExists(STORAGE_SYNC_TIMESTAMP_FILENAME))
	{
		return 0;
	}

	if ((fp=fopen(STORAGE_SYNC_TIMESTAMP_FILENAME, "r")) == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"open file \"%s/%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, data_path, STORAGE_SYNC_TIMESTAMP_FILENAME, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	src_index = 0;
	pGroup = NULL;
	*previous_group_name = '\0';
	result = 0;
	while (fgets(szLine, sizeof(szLine), fp) != NULL)
	{
		if (*szLine == '\0' || *szLine == '\n')
		{
			continue;
		}

		if ((cols=splitEx(szLine, STORAGE_DATA_FIELD_SEPERATOR, \
			fields, STORAGE_SYNC_TIME_MAX_FIELDS)) <= 2)
		{
			logError("file: "__FILE__", line: %d, " \
				"the format of the file \"%s/%s\" is invalid" \
				", colums: %d <= 2", \
				__LINE__, data_path, \
				STORAGE_SYNC_TIMESTAMP_FILENAME, cols);
			result = errno != 0 ? errno : EINVAL;
			break;
		}
	
		snprintf(group_name, sizeof(group_name), \
				"%s", trim(fields[0]));
		snprintf(src_ip_addr, sizeof(src_ip_addr), \
				"%s", trim(fields[1]));
		if (strcmp(group_name, previous_group_name) != 0 || \
			pGroup == NULL)
		{
			if ((pGroup=tracker_mem_get_group(group_name)) == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"in the file \"%s/%s\", " \
					"group \"%s\" is not found", \
					__LINE__, data_path, \
					STORAGE_SYNC_TIMESTAMP_FILENAME, \
					group_name);
				result = errno != 0 ? errno : ENOENT;
				break;
			}

			strcpy(previous_group_name, group_name);
			src_index = 0;
		}
		
		if (src_index >= pGroup->count)
		{
			logError("file: "__FILE__", line: %d, " \
				"the format of the file \"%s/%s\" is invalid" \
				", group: %s, row count:%d > server count:%d",\
				__LINE__, data_path, \
				STORAGE_SYNC_TIMESTAMP_FILENAME, \
				group_name, src_index+1, pGroup->count);
			result = errno != 0 ? errno : EINVAL;
			break;
		}

		if (strcmp(pGroup->all_servers[src_index].ip_addr, \
			src_ip_addr) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"in data file: \"%s/%s\", " \
				"group: %s, src server ip: %s != %s",\
				__LINE__, data_path, \
				STORAGE_SYNC_TIMESTAMP_FILENAME, \
				group_name, src_ip_addr, \
				pGroup->all_servers[src_index].ip_addr);
			result = errno != 0 ? errno : EINVAL;
			break;
		}

		if (cols > pGroup->count + 2)
		{
			logError("file: "__FILE__", line: %d, " \
				"the format of the file \"%s/%s\" is invalid" \
				", group_name: %s, colums: %d > %d", \
				__LINE__, data_path, \
				STORAGE_SYNC_TIMESTAMP_FILENAME, \
				group_name, cols, pGroup->count + 2);
			result = errno != 0 ? errno : EINVAL;
			break;
		}

		for (dest_index=0; dest_index<cols-2; dest_index++)
		{
			pGroup->last_sync_timestamps[src_index][dest_index] = \
				atoi(trim_left(fields[2 + dest_index]));
		}

		src_index++;
	}

	fclose(fp);

	if (result != 0)
	{
		return result;
	}

	pEnd = g_groups.groups + g_groups.count;
	for (pGroup=g_groups.groups; pGroup<pEnd; pGroup++)
	{
		if (pGroup->count <= 1)
		{
			continue;
		}

		for (dest_index=0; dest_index<pGroup->count; dest_index++)
		{
			if (g_groups.store_server == FDFS_STORE_SERVER_ROUND_ROBIN)
			{
				int min_synced_timestamp;

				min_synced_timestamp = 0;
				for (src_index=0; src_index<pGroup->count; \
					src_index++)
				{
					if (src_index == dest_index)
					{
						continue;
					}

					curr_synced_timestamp = \
						pGroup->last_sync_timestamps \
							[src_index][dest_index];
					if (curr_synced_timestamp == 0)
					{
						continue;
					}

					if (min_synced_timestamp == 0)
					{
						min_synced_timestamp = \
							curr_synced_timestamp;
					}
					else if (curr_synced_timestamp < \
						min_synced_timestamp)
					{
						min_synced_timestamp = \
							curr_synced_timestamp;
					}
				}

				pGroup->all_servers[dest_index].stat. \
					last_synced_timestamp = min_synced_timestamp;
			}
			else
			{
				int max_synced_timestamp;

				max_synced_timestamp = 0;
				for (src_index=0; src_index<pGroup->count; \
					src_index++)
				{
					if (src_index == dest_index)
					{
						continue;
					}

					curr_synced_timestamp = \
						pGroup->last_sync_timestamps \
							[src_index][dest_index];
					if (curr_synced_timestamp > \
						max_synced_timestamp)
					{
						max_synced_timestamp = \
							curr_synced_timestamp;
					}
				}

				pGroup->all_servers[dest_index].stat. \
					last_synced_timestamp = max_synced_timestamp;
			}
		}
	}

	return result;
}

static int tracker_load_data()
{
	char data_path[MAX_PATH_SIZE];
	int result;

	snprintf(data_path, sizeof(data_path), "%s/data", g_base_path);
	if (!fileExists(data_path))
	{
		if (mkdir(data_path, 0755) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"mkdir \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, data_path, errno, strerror(errno));
			return errno != 0 ? errno : ENOENT;
		}
	}

	if (chdir(data_path) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"chdir \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, data_path, errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (!fileExists(STORAGE_GROUPS_LIST_FILENAME))
	{
		return 0;
	}

	if ((result=tracker_load_groups(data_path)) != 0)
	{
		return result;
	}

	if ((result=tracker_load_storages(data_path)) != 0)
	{
		return result;
	}

	if ((result=tracker_malloc_all_group_path_mbs()) != 0)
	{
		return result;
	}

	if ((result=tracker_load_sync_timestamps(data_path)) != 0)
	{
		return result;
	}

	return 0;
}

static int tracker_save_groups()
{
	char filename[MAX_PATH_SIZE];
	char buff[FDFS_GROUP_NAME_MAX_LEN + 128];
	int fd;
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppEnd;
	int result;
	int len;

	tracker_mem_file_lock();

	snprintf(filename, sizeof(filename), "%s/data/%s", \
		g_base_path, STORAGE_GROUPS_LIST_FILENAME);
	if ((fd=open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
	{
		tracker_mem_file_unlock();

		logError("file: "__FILE__", line: %d, " \
			"open \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filename, errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	result = 0;
	ppEnd = g_groups.sorted_groups + g_groups.count;
	for (ppGroup=g_groups.sorted_groups; ppGroup<ppEnd; ppGroup++)
	{
		len = sprintf(buff, "%s%c%d%c%d%c%d\n", \
				(*ppGroup)->group_name, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				(*ppGroup)->storage_port, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				(*ppGroup)->store_path_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				(*ppGroup)->subdir_count_per_path);
		if (write(fd, buff, len) != len)
		{
			logError("file: "__FILE__", line: %d, " \
				"write to file \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, filename, errno, strerror(errno));
			result = errno != 0 ? errno : EIO;
			break;
		}
	}

	close(fd);
	tracker_mem_file_unlock();

	return result;
}

int tracker_save_storages()
{
	char filename[MAX_PATH_SIZE];
	char buff[512];
	int fd;
	int len;
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppGroupEnd;
	FDFSStorageDetail *pStorage;
	FDFSStorageDetail *pStorageEnd;
	int result;

	tracker_mem_file_lock();

	snprintf(filename, sizeof(filename), "%s/data/%s", \
		g_base_path, STORAGE_SERVERS_LIST_FILENAME);
	if ((fd=open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
	{
		tracker_mem_file_unlock();

		logError("file: "__FILE__", line: %d, " \
			"open \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filename, errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	result = 0;
	ppGroupEnd = g_groups.sorted_groups + g_groups.count;
	for (ppGroup=g_groups.sorted_groups; \
		(ppGroup < ppGroupEnd) && (result == 0); ppGroup++)
	{
		pStorageEnd = (*ppGroup)->all_servers + (*ppGroup)->count;
		for (pStorage=(*ppGroup)->all_servers; \
			pStorage<pStorageEnd; pStorage++)
		{
			if (pStorage->status == FDFS_STORAGE_STATUS_DELETED)
			{
				continue;
			}

			len = sprintf(buff, \
				"%s%c" \
				"%s%c" \
				"%d%c" \
				"%s%c" \
				"%d%c" \
				INT64_PRINTF_FORMAT"%c" \
				INT64_PRINTF_FORMAT"%c" \
				INT64_PRINTF_FORMAT"%c" \
				INT64_PRINTF_FORMAT"%c" \
				INT64_PRINTF_FORMAT"%c" \
				INT64_PRINTF_FORMAT"%c" \
				INT64_PRINTF_FORMAT"%c" \
				INT64_PRINTF_FORMAT"%c" \
				INT64_PRINTF_FORMAT"%c" \
				INT64_PRINTF_FORMAT"%c" \
				"%d%c" \
				"%d%c" \
				OFF_PRINTF_FORMAT"\n", \
				(*ppGroup)->group_name, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->ip_addr, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->status, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				(pStorage->psync_src_server != NULL ? \
				pStorage->psync_src_server->ip_addr : ""), 	
				STORAGE_DATA_FIELD_SEPERATOR, \
				(int)pStorage->sync_until_timestamp, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->stat.total_upload_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->stat.success_upload_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->stat.total_set_meta_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->stat.success_set_meta_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->stat.total_delete_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->stat.success_delete_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->stat.total_download_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->stat.success_download_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->stat.total_get_meta_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->stat.success_get_meta_count, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				(int)(pStorage->stat.last_source_update), \
				STORAGE_DATA_FIELD_SEPERATOR, \
				(int)(pStorage->stat.last_sync_update), \
				STORAGE_DATA_FIELD_SEPERATOR, \
				pStorage->changelog_offset \
	 		    );

			if (write(fd, buff, len) != len)
			{
				logError("file: "__FILE__", line: %d, " \
					"write to file \"%s\" fail, " \
					"errno: %d, error info: %s", \
					__LINE__, filename, \
					errno, strerror(errno));
				result = errno != 0 ? errno : EIO;
				break;
			}
		}
	}

	close(fd);
	tracker_mem_file_unlock();

	return result;
}

int tracker_save_sync_timestamps()
{
	char filename[MAX_PATH_SIZE];
	char buff[512];
	int fd;
	int len;
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppGroupEnd;
	int **last_sync_timestamps;
	int i;
	int k;
	int result;

	snprintf(filename, sizeof(filename), "%s/data/%s", \
		g_base_path, STORAGE_SYNC_TIMESTAMP_FILENAME);
	if ((fd=open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filename, errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	result = 0;
	ppGroupEnd = g_groups.sorted_groups + g_groups.count;
	for (ppGroup=g_groups.sorted_groups; \
		(ppGroup < ppGroupEnd) && (result == 0); ppGroup++)
	{
		last_sync_timestamps = (*ppGroup)->last_sync_timestamps;
		for (i=0; i<(*ppGroup)->count; i++)
		{
			if ((*ppGroup)->all_servers[i].status == \
				FDFS_STORAGE_STATUS_DELETED)
			{
				continue;
			}

			len = sprintf(buff, "%s%c%s", (*ppGroup)->group_name, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				(*ppGroup)->all_servers[i].ip_addr);
			for (k=0; k<(*ppGroup)->count; k++)
			{
				if ((*ppGroup)->all_servers[k].status == \
					FDFS_STORAGE_STATUS_DELETED)
				{
					continue;
				}

				len += sprintf(buff + len, "%c%d", \
					STORAGE_DATA_FIELD_SEPERATOR, \
					last_sync_timestamps[i][k]);
			}
			*(buff + len) = '\n';
			len++;

			if (write(fd, buff, len) != len)
			{
				logError("file: "__FILE__", line: %d, " \
					"write to file \"%s\" fail, " \
					"errno: %d, error info: %s", \
					__LINE__, filename, \
					errno, strerror(errno));
				result = errno != 0 ? errno : EIO;
				break;
			}
		}
	}

	close(fd);
	return result;
}

static int tracker_open_changlog_file()
{
	char filename[MAX_PATH_SIZE];

	snprintf(filename, sizeof(filename), "%s/data/%s", \
		g_base_path, STORAGE_SERVERS_CHANGELOG_FILENAME);
	changelog_fd = open(filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
	if (changelog_fd < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filename, errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	g_changelog_fsize = lseek(changelog_fd, 0, SEEK_END);
        if (g_changelog_fsize < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"lseek file \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filename, errno, strerror(errno));
		return errno != 0 ? errno : EIO;
	}

	return 0;
}

int tracker_mem_init()
{
	FDFSGroupInfo *pGroup;
	FDFSGroupInfo *pEnd;
	int *ref_count;
	int result;

	if ((result=init_pthread_lock(&mem_thread_lock)) != 0)
	{
		return result;
	}

	if ((result=init_pthread_lock(&mem_file_lock)) != 0)
	{
		return result;
	}

	if ((result=tracker_open_changlog_file()) != 0)
	{
		return result;
	}
	
	g_groups.alloc_size = TRACKER_MEM_ALLOC_ONCE;
	g_groups.count = 0;
	g_groups.current_write_group = 0;
	g_groups.pStoreGroup = NULL;
	g_groups.groups = (FDFSGroupInfo *)malloc( \
			sizeof(FDFSGroupInfo) * g_groups.alloc_size);
	if (g_groups.groups == NULL)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, program exit!", \
			__LINE__, sizeof(FDFSGroupInfo) * g_groups.alloc_size);
		return errno != 0 ? errno : ENOMEM;
	}

	memset(g_groups.groups, 0, \
		sizeof(FDFSGroupInfo) * g_groups.alloc_size);
	ref_count = (int *)malloc(sizeof(int));
	if (ref_count == NULL)
	{
		free(g_groups.groups);
		g_groups.groups = NULL;

		logCrit("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, program exit!", \
			__LINE__, sizeof(int));
		return errno != 0 ? errno : ENOMEM;
	}

	*ref_count = 0;
	pEnd = g_groups.groups + g_groups.alloc_size;
	for (pGroup=g_groups.groups; pGroup<pEnd; pGroup++)
	{
		pGroup->ref_count = ref_count;
	}

	g_groups.sorted_groups = (FDFSGroupInfo **) \
			malloc(sizeof(FDFSGroupInfo *) * g_groups.alloc_size);
	if (g_groups.sorted_groups == NULL)
	{
		free(ref_count);
		free(g_groups.groups);
		g_groups.groups = NULL;

		logCrit("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, program exit!", __LINE__, \
			sizeof(FDFSGroupInfo *) * g_groups.alloc_size);
		return errno != 0 ? errno : ENOMEM;
	}

	memset(g_groups.sorted_groups, 0, \
		sizeof(FDFSGroupInfo *) * g_groups.alloc_size);

	if ((result=tracker_load_data()) != 0)
	{
		return result;
	}

	/*
	if (g_groups.store_lookup == FDFS_STORE_LOOKUP_SPEC_GROUP)
	{
		g_groups.pStoreGroup = tracker_mem_get_group( \
					g_groups.store_group);
	}
	*/

	return 0;
}

static void tracker_free_last_sync_timestamps(int **last_sync_timestamps, \
		const int alloc_size)
{
	int i;

	if (last_sync_timestamps != NULL)
	{
		for (i=0; i<alloc_size; i++)
		{
			if (last_sync_timestamps[i] != NULL)
			{
				free(last_sync_timestamps[i]);
				last_sync_timestamps[i] = NULL;
			}
		}

		free(last_sync_timestamps);
	}
}

static int **tracker_malloc_last_sync_timestamps(const int alloc_size, \
		int *err_no)
{
	int **results;
	int i;

	results = (int **)malloc(sizeof(int *) * alloc_size);
	if (results == NULL)
	{
		*err_no = errno != 0 ? errno : ENOMEM;
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", __LINE__, \
			sizeof(int *) * alloc_size);
		return NULL;
	}

	memset(results, 0, sizeof(int *) * alloc_size);
	for (i=0; i<alloc_size; i++)
	{
		results[i] = (int *)malloc(sizeof(int) * alloc_size);
		if (results[i] == NULL)
		{
			*err_no = errno != 0 ? errno : ENOMEM;
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail", __LINE__, \
				sizeof(int) * alloc_size);

			tracker_free_last_sync_timestamps(results, alloc_size);
			return NULL;
		}

		memset(results[i], 0, sizeof(int) * alloc_size);
	}

	*err_no = 0;
	return results;
}

static int tracker_mem_init_group(FDFSGroupInfo *pGroup)
{
	int *ref_count;
	FDFSStorageDetail *pServer;
	FDFSStorageDetail *pEnd;
	int err_no;

	pGroup->alloc_size = TRACKER_MEM_ALLOC_ONCE;
	pGroup->count = 0;
	pGroup->all_servers = (FDFSStorageDetail *) \
			malloc(sizeof(FDFSStorageDetail) * pGroup->alloc_size);
	if (pGroup->all_servers == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", __LINE__, \
			sizeof(FDFSStorageDetail) * pGroup->alloc_size);
		return errno != 0 ? errno : ENOMEM;
	}

	memset(pGroup->all_servers, 0, \
		sizeof(FDFSStorageDetail) * pGroup->alloc_size);

	pGroup->sorted_servers = (FDFSStorageDetail **) \
		malloc(sizeof(FDFSStorageDetail *) * pGroup->alloc_size);
	if (pGroup->sorted_servers == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", __LINE__, \
			sizeof(FDFSStorageDetail *) * pGroup->alloc_size);
		return errno != 0 ? errno : ENOMEM;
	}
	memset(pGroup->sorted_servers, 0, \
		sizeof(FDFSStorageDetail *) * pGroup->alloc_size);

	pGroup->active_servers = (FDFSStorageDetail **) \
		malloc(sizeof(FDFSStorageDetail *) * pGroup->alloc_size);
	if (pGroup->active_servers == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", __LINE__, \
			sizeof(FDFSStorageDetail *) * pGroup->alloc_size);
		return errno != 0 ? errno : ENOMEM;
	}
	memset(pGroup->active_servers, 0, \
		sizeof(FDFSStorageDetail *) * pGroup->alloc_size);

	ref_count = (int *)malloc(sizeof(int));
	if (ref_count == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", \
			__LINE__, sizeof(int));
		return errno != 0 ? errno : ENOMEM;
	}

	*ref_count = 0;
	pEnd = pGroup->all_servers + pGroup->alloc_size;
	for (pServer=pGroup->all_servers; pServer<pEnd; pServer++)
	{
		pServer->ref_count = ref_count;
	}

	pGroup->last_sync_timestamps = tracker_malloc_last_sync_timestamps( \
			pGroup->alloc_size, &err_no);
	return err_no;
}

static void tracker_mem_free_group(FDFSGroupInfo *pGroup)
{
	if (pGroup->sorted_servers != NULL)
	{
		free(pGroup->sorted_servers);
		pGroup->sorted_servers = NULL;
	}

	if (pGroup->active_servers != NULL)
	{
		free(pGroup->active_servers);
		pGroup->active_servers = NULL;
	}

	if (pGroup->all_servers != NULL)
	{
		free(pGroup->all_servers[0].ref_count);
		free(pGroup->all_servers);
		pGroup->all_servers = NULL;
	}

	tracker_free_last_sync_timestamps(pGroup->last_sync_timestamps, \
				pGroup->alloc_size);
	pGroup->last_sync_timestamps = NULL;

}

int tracker_mem_destroy()
{
	FDFSGroupInfo *pGroup;
	FDFSGroupInfo *pEnd;
	int result;

	if (g_groups.groups == NULL)
	{
		result = 0;
	}
	else
	{
		result = tracker_save_storages();
		result += tracker_save_sync_timestamps();

		pEnd = g_groups.groups + g_groups.count;
		for (pGroup=g_groups.groups; pGroup<pEnd; pGroup++)
		{
			tracker_mem_free_group(pGroup);
		}

		if (g_groups.sorted_groups != NULL)
		{
			free(g_groups.sorted_groups);
		}

		if (g_groups.groups[0].ref_count != NULL)
		{
			free(g_groups.groups[0].ref_count);
		}

		free(g_groups.groups);
		g_groups.groups = NULL;
	}

	if (changelog_fd >= 0)
	{
		close(changelog_fd);
		changelog_fd = -1;
	}

	if (pthread_mutex_destroy(&mem_thread_lock) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_destroy fail", \
			__LINE__);
	}

	if (pthread_mutex_destroy(&mem_file_lock) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_destroy fail", \
			__LINE__);
	}

	return result;
}

int tracker_mem_realloc_groups()
{
	FDFSGroupInfo *old_groups;
	FDFSGroupInfo **old_sorted_groups;
	FDFSGroupInfo *new_groups;
	FDFSGroupInfo **new_sorted_groups;
	int new_size;
	FDFSGroupInfo *pGroup;
	FDFSGroupInfo *pEnd;
	FDFSGroupInfo **ppSrcGroup;
	FDFSGroupInfo **ppDestGroup;
	FDFSGroupInfo **ppEnd;
	int *new_ref_count;

	new_size = g_groups.alloc_size + TRACKER_MEM_ALLOC_ONCE;
	new_groups = (FDFSGroupInfo *)malloc(sizeof(FDFSGroupInfo) * new_size);
	if (new_groups == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", \
			__LINE__, sizeof(FDFSGroupInfo) * new_size);
		return errno != 0 ? errno : ENOMEM;
	}

	new_sorted_groups = (FDFSGroupInfo **)malloc( \
			sizeof(FDFSGroupInfo *) * new_size);
	if (new_sorted_groups == NULL)
	{
		free(new_groups);
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", \
			__LINE__, sizeof(FDFSGroupInfo *) * new_size);
		return errno != 0 ? errno : ENOMEM;
	}

	new_ref_count = (int *)malloc(sizeof(int));
	if (new_ref_count == NULL)
	{
		free(new_groups);
		free(new_sorted_groups);
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", \
			__LINE__, sizeof(int));
		return errno != 0 ? errno : ENOMEM;
	}

	memset(new_groups, 0, sizeof(FDFSGroupInfo) * new_size);
	memcpy(new_groups, g_groups.groups, \
		sizeof(FDFSGroupInfo) * g_groups.count);

	memset(new_sorted_groups, 0, sizeof(FDFSGroupInfo *) * new_size);
	ppDestGroup = new_sorted_groups;
	ppEnd = g_groups.sorted_groups + g_groups.count;
	for (ppSrcGroup=g_groups.sorted_groups; ppSrcGroup<ppEnd; ppSrcGroup++)
	{
		*ppDestGroup++ = new_groups + (*ppSrcGroup - g_groups.groups);
	}

	*new_ref_count = 0;
	pEnd = new_groups + new_size;
	for (pGroup=new_groups; pGroup<pEnd; pGroup++)
	{
		pGroup->ref_count = new_ref_count;
	}

	old_groups = g_groups.groups;
	old_sorted_groups = g_groups.sorted_groups;
	g_groups.alloc_size = new_size;
	g_groups.groups = new_groups;
	g_groups.sorted_groups = new_sorted_groups;

	if (g_groups.store_lookup == FDFS_STORE_LOOKUP_SPEC_GROUP)
	{
		g_groups.pStoreGroup = tracker_mem_get_group( \
					g_groups.store_group);
	}

	sleep(1);

	if (*(old_groups[0].ref_count) <= 0)
	{
		free(old_groups[0].ref_count);
		free(old_groups);
	}
	else
	{
		pEnd = old_groups + g_groups.count;
		for (pGroup=old_groups; pGroup<pEnd; pGroup++)
		{
			pGroup->dirty = true;
		}
	}

	free(old_sorted_groups);

	return 0;
}

int tracker_get_group_file_count(FDFSGroupInfo *pGroup)
{
	int count;
	FDFSStorageDetail *pServer;
	FDFSStorageDetail *pServerEnd;

	count = 0;
	pServerEnd = pGroup->all_servers + pGroup->count;
	for (pServer=pGroup->all_servers; pServer<pServerEnd; pServer++)
	{
		count += pServer->stat.success_upload_count - \
				pServer->stat.success_delete_count;
	}

	return count;
}

int tracker_get_group_success_upload_count(FDFSGroupInfo *pGroup)
{
	int count;
	FDFSStorageDetail *pServer;
	FDFSStorageDetail *pServerEnd;

	count = 0;
	pServerEnd = pGroup->all_servers + pGroup->count;
	for (pServer=pGroup->all_servers; pServer<pServerEnd; pServer++)
	{
		count += pServer->stat.success_upload_count;
	}

	return count;
}

FDFSStorageDetail *tracker_get_group_sync_src_server(FDFSGroupInfo *pGroup, \
			FDFSStorageDetail *pDestServer)
{
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppServerEnd;

	ppServerEnd = pGroup->active_servers + pGroup->active_count;
	for (ppServer=pGroup->active_servers; ppServer<ppServerEnd; ppServer++)
	{
		if (strcmp((*ppServer)->ip_addr, pDestServer->ip_addr) == 0)
		{
			continue;
		}

		return *ppServer;
	}

	return NULL;
}

static int tracker_mem_realloc_store_servers(FDFSGroupInfo *pGroup, \
		const int inc_count)
{
	int result;
	FDFSStorageDetail *old_servers;
	FDFSStorageDetail **old_sorted_servers;
	FDFSStorageDetail **old_active_servers;
	int **old_last_sync_timestamps;
	FDFSStorageDetail *new_servers;
	FDFSStorageDetail **new_sorted_servers;
	FDFSStorageDetail **new_active_servers;
	int **new_last_sync_timestamps;
	int *new_ref_count;
	int old_size;
	int new_size;
	FDFSStorageDetail *pServer;
	FDFSStorageDetail *pServerEnd;
	FDFSStorageDetail **ppDestServer;
	FDFSStorageDetail **ppSrcServer;
	FDFSStorageDetail **ppServerEnd;
	FDFSStorageSync *pStorageSyncs;
	int nStorageSyncSize;
	int nStorageSyncCount;
	int err_no;
	int i;
	
	new_size = pGroup->alloc_size + inc_count + TRACKER_MEM_ALLOC_ONCE;
	new_servers = (FDFSStorageDetail *) \
		malloc(sizeof(FDFSStorageDetail) * new_size);
	if (new_servers == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", \
			__LINE__, sizeof(FDFSStorageDetail) * new_size);
		return errno != 0 ? errno : ENOMEM;
	}

	new_sorted_servers = (FDFSStorageDetail **) \
		malloc(sizeof(FDFSStorageDetail *) * new_size);
	if (new_sorted_servers == NULL)
	{
		free(new_servers);
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", \
			__LINE__, sizeof(FDFSStorageDetail *) * new_size);
		return errno != 0 ? errno : ENOMEM;
	}

	new_active_servers = (FDFSStorageDetail **) \
		malloc(sizeof(FDFSStorageDetail *) * new_size);
	if (new_active_servers == NULL)
	{
		free(new_servers);
		free(new_sorted_servers);

		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", \
			__LINE__, sizeof(FDFSStorageDetail *) * new_size);
		return errno != 0 ? errno : ENOMEM;
	}

	memset(new_servers, 0, sizeof(FDFSStorageDetail) * new_size);
	memset(new_sorted_servers, 0, sizeof(FDFSStorageDetail *) * new_size);
	memset(new_active_servers, 0, sizeof(FDFSStorageDetail *) * new_size);
	memcpy(new_servers, pGroup->all_servers, \
		sizeof(FDFSStorageDetail) * pGroup->count);
	if (pGroup->store_path_count > 0)
	{
		for (i=pGroup->count; i<new_size; i++)
		{
			result=tracker_malloc_storage_path_mbs(new_servers+i, \
				pGroup->store_path_count);
			if (result != 0)
			{
				free(new_servers);
				free(new_sorted_servers);

				return result;
			}
		}
	}

	ppServerEnd = pGroup->sorted_servers + pGroup->count;
	ppDestServer = new_sorted_servers;
	for (ppSrcServer=pGroup->sorted_servers; ppSrcServer<ppServerEnd;
		ppSrcServer++)
	{
		*ppDestServer++ = new_servers + 
				  (*ppSrcServer - pGroup->all_servers);
	}

	ppServerEnd = pGroup->active_servers + pGroup->active_count;
	ppDestServer = new_active_servers;
	for (ppSrcServer=pGroup->active_servers; ppSrcServer<ppServerEnd;
		ppSrcServer++)
	{
		*ppDestServer++ = new_servers + 
				  (*ppSrcServer - pGroup->all_servers);
	}

	new_ref_count = (int *)malloc(sizeof(int));
	if (new_ref_count == NULL)
	{
		free(new_servers);
		free(new_sorted_servers);
		free(new_active_servers);

		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", \
			__LINE__, sizeof(int));
		return errno != 0 ? errno : ENOMEM;
	}
	*new_ref_count = 0;
	pServerEnd = new_servers + new_size;
	for (pServer=new_servers; pServer<pServerEnd; pServer++)
	{
		pServer->ref_count = new_ref_count;
	}

	new_last_sync_timestamps = tracker_malloc_last_sync_timestamps( \
		new_size, &err_no);
	if (new_last_sync_timestamps == NULL)
	{
		free(new_servers);
		free(new_sorted_servers);
		free(new_active_servers);
		free(new_ref_count);

		return err_no;
	}
	for (i=0; i<pGroup->alloc_size; i++)
	{
		memcpy(new_last_sync_timestamps[i],  \
			pGroup->last_sync_timestamps[i], \
			sizeof(int) *  pGroup->alloc_size);
	}

	old_size = pGroup->alloc_size;
	old_servers = pGroup->all_servers;
	old_sorted_servers = pGroup->sorted_servers;
	old_active_servers = pGroup->active_servers;
	old_last_sync_timestamps = pGroup->last_sync_timestamps;

	pGroup->alloc_size = new_size;
	pGroup->all_servers = new_servers;
	pGroup->sorted_servers = new_sorted_servers;
	pGroup->active_servers = new_active_servers;
	pGroup->last_sync_timestamps = new_last_sync_timestamps;

	tracker_mem_find_store_server(pGroup);

	nStorageSyncSize = 0;
	nStorageSyncCount = 0;
	pStorageSyncs = NULL;

	result = 0;
	pServerEnd = new_servers + pGroup->count;
	for (pServer=new_servers; pServer<pServerEnd; pServer++)
	{
		if (pServer->psync_src_server == NULL)
		{
			continue;
		}

		if (nStorageSyncSize <= nStorageSyncCount)
		{
			nStorageSyncSize += 8;
			pStorageSyncs = (FDFSStorageSync *)realloc( \
				pStorageSyncs, \
				sizeof(FDFSStorageSync) * nStorageSyncSize);
			if (pStorageSyncs == NULL)
			{
				result = errno != 0 ? errno : ENOMEM;
				logError("file: "__FILE__", line: %d, " \
					"realloc %d bytes fail", __LINE__, \
					sizeof(FDFSStorageSync) * \
					nStorageSyncSize);
				break;
			}
		}

		strcpy(pStorageSyncs[nStorageSyncCount].group_name, \
			pGroup->group_name);
		strcpy(pStorageSyncs[nStorageSyncCount].ip_addr, \
			pServer->ip_addr);
		strcpy(pStorageSyncs[nStorageSyncCount].sync_src_ip_addr, \
			pServer->psync_src_server->ip_addr);
		nStorageSyncCount++;
	}

	if (pStorageSyncs != NULL)
	{
		result = tracker_locate_storage_sync_server( \
				pStorageSyncs, nStorageSyncCount, false);

		free(pStorageSyncs);
	}

	sleep(1);

	if (*(old_servers[0].ref_count) <= 0)
	{
		free(old_servers[0].ref_count);
		free(old_servers);
	}
	else
	{
		pServerEnd = old_servers + pGroup->count;
		for (pServer=old_servers; pServer<pServerEnd; pServer++)
		{
			pServer->dirty = true;
		}
	}
	
	free(old_sorted_servers);
	free(old_active_servers);
	tracker_free_last_sync_timestamps(old_last_sync_timestamps, \
				old_size);

	return result;
}

static int tracker_mem_cmp_by_group_name(const void *p1, const void *p2)
{
	return strcmp((*((FDFSGroupInfo **)p1))->group_name,
			(*((FDFSGroupInfo **)p2))->group_name);
}

static int tracker_mem_cmp_by_ip_addr(const void *p1, const void *p2)
{
	return strcmp((*((FDFSStorageDetail **)p1))->ip_addr,
			(*((FDFSStorageDetail **)p2))->ip_addr);
}

static void tracker_mem_insert_into_sorted_servers( \
		FDFSStorageDetail *pTargetServer,   \
		FDFSStorageDetail **sorted_servers, const int count)
{
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppEnd;

	ppEnd = sorted_servers + count;
	for (ppServer=ppEnd; ppServer>sorted_servers; ppServer--)
	{
		if (strcmp(pTargetServer->ip_addr, \
			   (*(ppServer-1))->ip_addr) > 0)
		{
			*ppServer = pTargetServer;
			return;
		}
		else
		{
			*ppServer = *(ppServer-1);
		}
	}

	*ppServer = pTargetServer;
}

static void tracker_mem_insert_into_sorted_groups( \
		FDFSGroupInfo *pTargetGroup)
{
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppEnd;

	ppEnd = g_groups.sorted_groups + g_groups.count;
	for (ppGroup=ppEnd; ppGroup > g_groups.sorted_groups; ppGroup--)
	{
		if (strcmp(pTargetGroup->group_name, \
			   (*(ppGroup-1))->group_name) > 0)
		{
			*ppGroup = pTargetGroup;
			return;
		}
		else
		{
			*ppGroup = *(ppGroup-1);
		}
	}

	*ppGroup = pTargetGroup;
}

FDFSGroupInfo *tracker_mem_get_group(const char *group_name)
{
	FDFSGroupInfo target_groups;
	FDFSGroupInfo *pTargetGroups;
	FDFSGroupInfo **ppGroup;

	memset(&target_groups, 0, sizeof(target_groups));
	strcpy(target_groups.group_name, group_name);
	pTargetGroups = &target_groups;
	ppGroup = (FDFSGroupInfo **)bsearch(&pTargetGroups, \
			g_groups.sorted_groups, \
			g_groups.count, sizeof(FDFSGroupInfo *), \
			tracker_mem_cmp_by_group_name);

	if (ppGroup != NULL)
	{
		return *ppGroup;
	}
	else
	{
		return NULL;
	}
}

int tracker_mem_add_group(TrackerClientInfo *pClientInfo, \
			const bool bIncRef, bool *bInserted)
{
	FDFSGroupInfo *pGroup;
	int result;

	*bInserted = false;
	pGroup = tracker_mem_get_group(pClientInfo->group_name);
	if (pGroup != NULL)
	{
		//printf("g_groups.count=%d, found %s\n", g_groups.count, pClientInfo->group_name);
	}
	else
	{
		if ((result=pthread_mutex_lock(&mem_thread_lock)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_lock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			return result;
		}

		result = 0;
		do
		{
			if (g_groups.count >= g_groups.alloc_size)
			{
				result = tracker_mem_realloc_groups();
				if (result != 0)
				{
					break;
				}
			}

			pGroup = g_groups.groups + g_groups.count;
			result = tracker_mem_init_group(pGroup);
			if (result != 0)
			{
				break;
			}

			strcpy(pGroup->group_name, pClientInfo->group_name);
			tracker_mem_insert_into_sorted_groups(pGroup);
			g_groups.count++;

			if ((g_groups.store_lookup == \
				FDFS_STORE_LOOKUP_SPEC_GROUP) && \
				(g_groups.pStoreGroup == NULL) && \
				(strcmp(g_groups.store_group, \
					pGroup->group_name) == 0))
			{
				g_groups.pStoreGroup = pGroup;
			}

		} while (0);

		if (pthread_mutex_unlock(&mem_thread_lock) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_unlock fail", \
				__LINE__);
			return result;
		}

		if (result != 0)
		{
			return result;
		}

		*bInserted = true;
	}

	if (bIncRef)
	{
		++(*(pGroup->ref_count));
		//printf("group ref_count=%d\n", *(pGroup->ref_count));
	}
	pClientInfo->pGroup = pGroup;
	pClientInfo->pAllocedGroups = g_groups.groups;
	return 0;
}

FDFSStorageDetail *tracker_mem_get_active_storage(FDFSGroupInfo *pGroup, \
				const char *ip_addr)
{
	FDFSStorageDetail target_storage;
	FDFSStorageDetail *pTargetStorage;
	FDFSStorageDetail **ppStorageServer;

	if (ip_addr == NULL)
	{
		return NULL;
	}

	memset(&target_storage, 0, sizeof(target_storage));
	strcpy(target_storage.ip_addr, ip_addr);
	pTargetStorage = &target_storage;
	ppStorageServer = (FDFSStorageDetail **)bsearch(&pTargetStorage, \
			pGroup->active_servers, \
			pGroup->active_count, \
			sizeof(FDFSStorageDetail *), \
			tracker_mem_cmp_by_ip_addr);
	if (ppStorageServer != NULL)
	{
		return *ppStorageServer;
	}
	else
	{
		return NULL;
	}
}

FDFSStorageDetail *tracker_mem_get_storage(FDFSGroupInfo *pGroup, \
				const char *ip_addr)
{
	FDFSStorageDetail target_storage;
	FDFSStorageDetail *pTargetStorage;
	FDFSStorageDetail **ppStorageServer;

	memset(&target_storage, 0, sizeof(target_storage));
	strcpy(target_storage.ip_addr, ip_addr);
	pTargetStorage = &target_storage;
	ppStorageServer = (FDFSStorageDetail **)bsearch(&pTargetStorage, \
			pGroup->sorted_servers, \
			pGroup->count, \
			sizeof(FDFSStorageDetail *), \
			tracker_mem_cmp_by_ip_addr);
	if (ppStorageServer != NULL)
	{
		return *ppStorageServer;
	}
	else
	{
		return NULL;
	}
}

int tracker_mem_delete_storage(FDFSGroupInfo *pGroup, const char *ip_addr)
{
	FDFSStorageDetail *pStorageServer;
	FDFSStorageDetail *pServer;
	FDFSStorageDetail *pEnd;

	pStorageServer = tracker_mem_get_storage(pGroup, ip_addr);
	if (pStorageServer == NULL || pStorageServer->status == \
		FDFS_STORAGE_STATUS_IP_CHANGED)
	{
		return ENOENT;
	}

	if (pStorageServer->status == FDFS_STORAGE_STATUS_ONLINE || \
	    pStorageServer->status == FDFS_STORAGE_STATUS_ACTIVE)
	{
		return EBUSY;
	}

	if (pStorageServer->status == FDFS_STORAGE_STATUS_DELETED)
	{
		return EALREADY;
	}

	pEnd = pGroup->all_servers + pGroup->count;
	for (pServer=pGroup->all_servers; pServer<pEnd; pServer++)
	{
		if (pServer->psync_src_server != NULL && \
		strcmp(pServer->psync_src_server->ip_addr, ip_addr) == 0)
		{
			pServer->psync_src_server = NULL;
		}
	}

	pStorageServer->status = FDFS_STORAGE_STATUS_DELETED;
	pGroup->chg_count++;

	tracker_write_to_changelog(pGroup, pStorageServer, NULL);
	return 0;
}

int tracker_mem_storage_ip_changed(FDFSGroupInfo *pGroup, \
		const char *old_storage_ip, const char *new_storage_ip)
{
	FDFSStorageDetail *pStorageServer;
	FDFSStorageDetail *pServer;
	FDFSStorageDetail *pEnd;

	pStorageServer = tracker_mem_get_storage(pGroup, old_storage_ip);
	if (pStorageServer == NULL || pStorageServer->status == \
		FDFS_STORAGE_STATUS_DELETED)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, old storage server: %s not exists", \
			__LINE__, new_storage_ip, old_storage_ip);
		return ENOENT;
	}

	if (pStorageServer->status == FDFS_STORAGE_STATUS_ONLINE || \
	    pStorageServer->status == FDFS_STORAGE_STATUS_ACTIVE)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, old storage server: %s is online", \
			__LINE__, new_storage_ip, old_storage_ip);
		return EBUSY;
	}

	if (pStorageServer->status == FDFS_STORAGE_STATUS_IP_CHANGED)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, old storage server: %s " \
			"'s ip address already changed", \
			__LINE__, new_storage_ip, old_storage_ip);
		return EALREADY;
	}

	pEnd = pGroup->all_servers + pGroup->count;
	for (pServer=pGroup->all_servers; pServer<pEnd; pServer++)
	{
		if (pServer->psync_src_server != NULL && \
		strcmp(pServer->psync_src_server->ip_addr, old_storage_ip) == 0)
		{
			pServer->psync_src_server = NULL;
		}
	}

	pStorageServer->status = FDFS_STORAGE_STATUS_IP_CHANGED;
	pGroup->chg_count++;

	tracker_write_to_changelog(pGroup, pStorageServer, new_storage_ip);
	return 0;
}

int tracker_mem_add_storage(TrackerClientInfo *pClientInfo, \
			const bool bIncRef, bool *bInserted)
{
	FDFSStorageDetail *pStorageServer;
	int result;

	*bInserted = false;
	pStorageServer = tracker_mem_get_storage(pClientInfo->pGroup, \
				pClientInfo->ip_addr);
	if (pStorageServer != NULL)
	{
		if (pStorageServer->status == FDFS_STORAGE_STATUS_DELETED)
		{
			logError("file: "__FILE__", line: %d, " \
				"storage ip: %s already deleted, you can " \
				"restart the tracker servers to reset.", \
				__LINE__, pClientInfo->ip_addr);
			return EAGAIN;
		}
		//printf("pGroup->count=%d, found %s\n", pClientInfo->pGroup->count, pClientInfo->ip_addr);
	}
	else
	{
		//printf("pGroup->count=%d, not found %s\n", pClientInfo->pGroup->count, pClientInfo->ip_addr);
		if ((result=pthread_mutex_lock(&mem_thread_lock)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_lock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			return result;
		}

		result = 0;
		do
		{
			if (pClientInfo->pGroup->count >= \
				pClientInfo->pGroup->alloc_size)
			{
				result = tracker_mem_realloc_store_servers( \
						pClientInfo->pGroup, 1);
				if (result != 0)
				{
					break;
				}
			}

			pStorageServer = pClientInfo->pGroup->all_servers \
					 + pClientInfo->pGroup->count;
			memcpy(pStorageServer->ip_addr, pClientInfo->ip_addr,
				IP_ADDRESS_SIZE);

			tracker_mem_insert_into_sorted_servers( \
				pStorageServer, \
				pClientInfo->pGroup->sorted_servers, \
				pClientInfo->pGroup->count);
			pClientInfo->pGroup->count++;
			pClientInfo->pGroup->chg_count++;
		} while (0);

		if (pthread_mutex_unlock(&mem_thread_lock) != 0)
		{
			logError("file: "__FILE__", line: %d, "   \
				"call pthread_mutex_unlock fail", \
				__LINE__);
		}

		if (result != 0)
		{
			return result;
		}

		*bInserted = true;
	}

	if (bIncRef)
	{
		++(*(pStorageServer->ref_count));
	}

	pClientInfo->pStorage = pStorageServer;
	pClientInfo->pAllocedStorages = pClientInfo->pGroup->all_servers;
	return 0;
}

int tracker_mem_add_group_and_storage(TrackerClientInfo *pClientInfo, \
		const int store_path_count, const int subdir_count_per_path, \
		const int upload_priority, const time_t up_time, \
		const char *version, const bool bIncRef, const bool init_flag)
{
	int result;
	bool bGroupInserted;
	bool bStorageInserted;
	FDFSStorageDetail *pStorageServer;
	FDFSStorageDetail *pServer;
	FDFSStorageDetail *pEnd;

	if ((result=tracker_mem_add_group(pClientInfo, bIncRef, \
			&bGroupInserted)) != 0)
	{
		return result;
	}

	if (bGroupInserted)
	{
		if ((result=tracker_save_groups()) != 0)
		{
			return result;
		}
	}

	if (pClientInfo->pGroup->storage_port == 0)
	{
		pClientInfo->pGroup->storage_port = pClientInfo->storage_port;
		if ((result=tracker_save_groups()) != 0)
		{
			return result;
		}
	}
	else
	{
		if (pClientInfo->pGroup->storage_port !=  \
			pClientInfo->storage_port)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, port %d is not same " \
				"in the group \"%s\", group port is %d", \
				__LINE__, pClientInfo->ip_addr, \
				pClientInfo->storage_port, \
				pClientInfo->group_name, \
				pClientInfo->pGroup->storage_port);
			return EINVAL;
		}
	}

	if (pClientInfo->pGroup->storage_http_port == 0)
	{
		pClientInfo->pGroup->storage_http_port = \
			pClientInfo->storage_http_port;
	}
	else
	{
		if (pClientInfo->pGroup->storage_http_port !=  \
			pClientInfo->storage_http_port)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, http port %d is not same " \
				"in the group \"%s\", group http port is %d", \
				__LINE__, pClientInfo->ip_addr, \
				pClientInfo->storage_http_port, \
				pClientInfo->group_name, \
				pClientInfo->pGroup->storage_http_port);
			return EINVAL;
		}
	}
	
	if ((result=tracker_mem_add_storage(pClientInfo, bIncRef, \
			&bStorageInserted)) != 0)
	{
		return result;
	}

	pStorageServer = pClientInfo->pStorage;
	pStorageServer->store_path_count = store_path_count;
	pStorageServer->subdir_count_per_path = subdir_count_per_path;
	pStorageServer->upload_priority = upload_priority;
	pStorageServer->up_time = up_time;
	snprintf(pStorageServer->version, sizeof(pStorageServer->version), \
		"%s", version);

	if (pClientInfo->pGroup->store_path_count == 0)
	{
		pClientInfo->pGroup->store_path_count = store_path_count;
		if ((result=tracker_malloc_group_path_mbs( \
				pClientInfo->pGroup)) != 0)
		{
			return result;
		}
		if ((result=tracker_save_groups()) != 0)
		{
			return result;
		}
	}
	else
	{
		if (pClientInfo->pGroup->store_path_count != store_path_count)
		{
			pEnd = pClientInfo->pGroup->all_servers + \
				pClientInfo->pGroup->count;
			for (pServer=pClientInfo->pGroup->all_servers; \
				pServer<pEnd; pServer++)
			{
				if (pServer->store_path_count!=store_path_count)
				{
					break;
				}
			}

			if (pServer == pEnd)  //all servers are same, adjust
			{
				if ((result=tracker_realloc_group_path_mbs( \
			 	    pClientInfo->pGroup, store_path_count))!=0)
				{
					return result;
				}

				if ((result=tracker_save_groups()) != 0)
				{
					return result;
				}
			}
			else
			{
				logError("file: "__FILE__", line: %d, " \
				"client ip: %s, store_path_count %d is not " \
				"same in the group \"%s\", " \
				"group store_path_count is %d", \
				__LINE__, pClientInfo->ip_addr, \
				store_path_count, pClientInfo->group_name, \
				pClientInfo->pGroup->store_path_count);

				return EINVAL;
			}
		}
	}

	if (pClientInfo->pGroup->subdir_count_per_path == 0)
	{
		pClientInfo->pGroup->subdir_count_per_path = \
				subdir_count_per_path;
		if ((result=tracker_save_groups()) != 0)
		{
			return result;
		}
	}
	else
	{
		if (pClientInfo->pGroup->subdir_count_per_path != \
				subdir_count_per_path)
		{
			pEnd = pClientInfo->pGroup->all_servers + \
				pClientInfo->pGroup->count;
			for (pServer=pClientInfo->pGroup->all_servers; \
				pServer<pEnd; pServer++)
			{
				if (pServer->subdir_count_per_path != \
					subdir_count_per_path)
				{
					break;
				}
			}

			if (pServer == pEnd)  //all servers are same, adjust
			{
				pClientInfo->pGroup->subdir_count_per_path = \
						subdir_count_per_path;
				if ((result=tracker_save_groups()) != 0)
				{
					return result;
				}
			}
			else
			{
				logError("file: "__FILE__", line: %d, " \
				"client ip: %s, subdir_count_per_path %d is " \
				"not same in the group \"%s\", " \
				"group subdir_count_per_path is %d", \
				__LINE__, pClientInfo->ip_addr, \
				subdir_count_per_path, pClientInfo->group_name,\
				pClientInfo->pGroup->subdir_count_per_path);

				return EINVAL;
			}
		}
	}

	if (bStorageInserted || init_flag)
	{
		pStorageServer->status = FDFS_STORAGE_STATUS_INIT;
		if ((result=tracker_save_storages()) != 0)
		{
			return result;
		}
	}
	else if (!((pStorageServer->status == FDFS_STORAGE_STATUS_WAIT_SYNC)||\
		(pStorageServer->status == FDFS_STORAGE_STATUS_SYNCING) || \
		(pStorageServer->status == FDFS_STORAGE_STATUS_INIT) || \
		(pStorageServer->status == FDFS_STORAGE_STATUS_DELETED) || \
		(pStorageServer->status == FDFS_STORAGE_STATUS_ACTIVE)))
	{
		pStorageServer->status = FDFS_STORAGE_STATUS_ONLINE;
	}

	logDebug("file: "__FILE__", line: %d, " \
		"storage server %s::%s join in", \
		__LINE__, pClientInfo->pGroup->group_name, \
		pStorageServer->ip_addr);

	return 0;
}

int tracker_mem_sync_storages(FDFSGroupInfo *pGroup, \
		FDFSStorageBrief *briefServers, const int server_count)
{
	int result;
	FDFSStorageBrief *pServer;
	FDFSStorageBrief *pEnd;
	FDFSStorageDetail target_storage;
	FDFSStorageDetail *pTargetStorage;
	FDFSStorageDetail *pStorageServer;
	FDFSStorageDetail **ppFound;

	if ((result=pthread_mutex_lock(&mem_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	result = 0;
	do
	{
		if (pGroup->count + server_count >= pGroup->alloc_size)
		{
			result = tracker_mem_realloc_store_servers( \
					pGroup, server_count);
			if (result != 0)
			{
				break;
			}
		}

		memset(&target_storage, 0, sizeof(target_storage));
		pStorageServer = pGroup->all_servers + pGroup->count;
		pEnd = briefServers + server_count;
		for (pServer=briefServers; pServer<pEnd; pServer++)
		{
			pServer->ip_addr[IP_ADDRESS_SIZE-1] = '\0';
			if (pServer->status == FDFS_STORAGE_STATUS_NONE)
			{
				pServer->status = FDFS_STORAGE_STATUS_DELETED;
			}

			memcpy(target_storage.ip_addr, pServer->ip_addr, \
				IP_ADDRESS_SIZE);
			pTargetStorage = &target_storage;
			if ((ppFound=(FDFSStorageDetail **)bsearch( \
				&pTargetStorage, \
				pGroup->sorted_servers, \
				pGroup->count, \
				sizeof(FDFSStorageDetail *), \
				tracker_mem_cmp_by_ip_addr)) != NULL)
			{
				if ((*ppFound)->status == \
					FDFS_STORAGE_STATUS_ACTIVE)
				{
					continue;
				}

				if ((*ppFound)->status == \
					FDFS_STORAGE_STATUS_OFFLINE && \
					(pServer->status == \
					FDFS_STORAGE_STATUS_WAIT_SYNC || \
					pServer->status == \
					FDFS_STORAGE_STATUS_SYNCING))
				{
					(*ppFound)->status = pServer->status;
					pGroup->chg_count++;
					continue;
				}

				if (((pServer->status > (*ppFound)->status) && \
					(((*ppFound)->status == \
					FDFS_STORAGE_STATUS_INIT) || \
					((*ppFound)->status == \
					FDFS_STORAGE_STATUS_WAIT_SYNC) || \
					((*ppFound)->status == \
					FDFS_STORAGE_STATUS_SYNCING))) || \
					(pServer->status != (*ppFound)->status \
					 && (pServer->status == \
						FDFS_STORAGE_STATUS_DELETED)))
				{
					(*ppFound)->status = pServer->status;
					pGroup->chg_count++;
				}
			}
			else if (pServer->status == FDFS_STORAGE_STATUS_DELETED)
			{
				//ignore deleted storage server
			}
			else
			{
				pStorageServer->status = pServer->status;
				memcpy(pStorageServer->ip_addr, \
					pServer->ip_addr, IP_ADDRESS_SIZE);

				tracker_mem_insert_into_sorted_servers( \
					pStorageServer, pGroup->sorted_servers,\
					pGroup->count);

				pStorageServer++;
				pGroup->count++;
			}
		}
	} while (0);

	if (pthread_mutex_unlock(&mem_thread_lock) != 0)
	{
		logError("file: "__FILE__", line: %d, "   \
			"call pthread_mutex_unlock fail", \
			__LINE__);
	}

	return result;
}

static void tracker_mem_find_store_server(FDFSGroupInfo *pGroup)
{
	if (pGroup->active_count == 0)
	{
		pGroup->pStoreServer = NULL;
		return;
	}

	if (g_groups.store_server == FDFS_STORE_SERVER_FIRST_BY_PRI)
	{
		FDFSStorageDetail **ppEnd;
		FDFSStorageDetail **ppServer;
		FDFSStorageDetail *pMinPriServer;

		pMinPriServer = *(pGroup->active_servers);
		ppEnd = pGroup->active_servers + pGroup->active_count;
		for (ppServer=pGroup->active_servers+1; ppServer<ppEnd; \
			ppServer++)
		{
			if ((*ppServer)->upload_priority < \
				pMinPriServer->upload_priority)
			{
				pMinPriServer = *ppServer;
			}
		}

		pGroup->pStoreServer = pMinPriServer;
	}
	else
	{
		pGroup->pStoreServer = *(pGroup->active_servers);
	}
}

int tracker_mem_deactive_store_server(FDFSGroupInfo *pGroup,
			FDFSStorageDetail *pTargetServer) 
{
	int result;
	FDFSStorageDetail **ppStorageServer;
	FDFSStorageDetail **ppEnd;
	FDFSStorageDetail **ppServer;

	if ((result=pthread_mutex_lock(&mem_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	ppStorageServer = (FDFSStorageDetail **)bsearch( \
			&pTargetServer, \
			pGroup->active_servers, \
			pGroup->active_count,   \
			sizeof(FDFSStorageDetail *), \
			tracker_mem_cmp_by_ip_addr);
	if (ppStorageServer != NULL)
	{
		ppEnd = pGroup->active_servers + pGroup->active_count - 1;
		for (ppServer=ppStorageServer; ppServer<ppEnd; ppServer++)
		{
			*ppServer = *(ppServer+1);
		}

		pGroup->active_count--;
		pGroup->chg_count++;
		if (pGroup->current_write_server >= pGroup->active_count)
		{
			pGroup->current_write_server = 0;
		}
		if (pGroup->current_read_server >= pGroup->active_count)
		{
			pGroup->current_read_server = 0;
		}
	}

	tracker_mem_find_store_server(pGroup);

	if ((result=pthread_mutex_unlock(&mem_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	return 0;
}

int tracker_mem_active_store_server(FDFSGroupInfo *pGroup, \
			FDFSStorageDetail *pTargetServer) 
{
	int result;
	FDFSStorageDetail **ppStorageServer;

	if ((pTargetServer->status == FDFS_STORAGE_STATUS_WAIT_SYNC) || \
		(pTargetServer->status == FDFS_STORAGE_STATUS_SYNCING) || \
		(pTargetServer->status == FDFS_STORAGE_STATUS_INIT))
	{
		return 0;
	}

	if (pTargetServer->status == FDFS_STORAGE_STATUS_DELETED)
	{
		logError("file: "__FILE__", line: %d, " \
			"storage ip: %s already deleted, you can " \
			"restart the tracker servers to reset.", \
			__LINE__, pTargetServer->ip_addr);
		return EAGAIN;
	}

	pTargetServer->status = FDFS_STORAGE_STATUS_ACTIVE;

	ppStorageServer = (FDFSStorageDetail **)bsearch(&pTargetServer, \
			pGroup->active_servers, \
			pGroup->active_count,   \
			sizeof(FDFSStorageDetail *), \
			tracker_mem_cmp_by_ip_addr);
	if (ppStorageServer != NULL)
	{
		return 0;
	}

	if ((result=pthread_mutex_lock(&mem_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	ppStorageServer = (FDFSStorageDetail **)bsearch(&pTargetServer, \
			pGroup->active_servers, \
			pGroup->active_count,   \
			sizeof(FDFSStorageDetail *), \
			tracker_mem_cmp_by_ip_addr);
	if (ppStorageServer == NULL)
	{
		tracker_mem_insert_into_sorted_servers( \
			pTargetServer, pGroup->active_servers, \
			pGroup->active_count);
		pGroup->active_count++;
		pGroup->chg_count++;

		logDebug("file: "__FILE__", line: %d, " \
			"storage server %s::%s now active", \
			__LINE__, pGroup->group_name, \
			pTargetServer->ip_addr);
	}

	tracker_mem_find_store_server(pGroup);

	if ((result=pthread_mutex_unlock(&mem_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	return 0;
}

int tracker_mem_offline_store_server(TrackerClientInfo *pClientInfo)
{
	if (pClientInfo->pGroup == NULL || pClientInfo->pStorage == NULL)
	{
		return 0;
	}

	pClientInfo->pStorage->up_time = 0;
	if ((pClientInfo->pStorage->status == \
			FDFS_STORAGE_STATUS_WAIT_SYNC) || \
		(pClientInfo->pStorage->status == \
			FDFS_STORAGE_STATUS_SYNCING) || \
		(pClientInfo->pStorage->status == \
			FDFS_STORAGE_STATUS_INIT) || \
		(pClientInfo->pStorage->status == \
			FDFS_STORAGE_STATUS_DELETED))
	{
		return 0;
	}

	logDebug("file: "__FILE__", line: %d, " \
		"storage server %s::%s offline", \
		__LINE__, pClientInfo->pGroup->group_name, \
		pClientInfo->pStorage->ip_addr);

	pClientInfo->pStorage->status = FDFS_STORAGE_STATUS_OFFLINE;
	return tracker_mem_deactive_store_server(pClientInfo->pGroup, \
				pClientInfo->pStorage);
}

FDFSStorageDetail *tracker_get_writable_storage(FDFSGroupInfo *pStoreGroup)
{
	int write_server_index;
	if (g_groups.store_server == FDFS_STORE_SERVER_ROUND_ROBIN)
	{
		write_server_index = pStoreGroup->current_write_server++;
		if (pStoreGroup->current_write_server >= \
				pStoreGroup->active_count)
		{
			pStoreGroup->current_write_server = 0;
		}

		if (write_server_index >= pStoreGroup->active_count)
		{
			write_server_index = 0;
		}
		return  *(pStoreGroup->active_servers + write_server_index);
	}
	else //use the first server
	{
		return pStoreGroup->pStoreServer;
	}
}

int tracker_mem_get_storage_by_filename(const byte cmd, const char *group_name,\
	const char *filename, const int filename_len, FDFSGroupInfo **ppGroup, \
	FDFSStorageDetail **ppStoreServers, int *server_count)
{
	char name_buff[64];
	char szIpAddr[IP_ADDRESS_SIZE];
	FDFSStorageDetail *pStoreSrcServer;
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppServerEnd;
	int file_timestamp;
	int decoded_len;
	int storage_ip;
	int read_server_index;
	struct in_addr ip_addr;
	time_t current_time;

	*server_count = 0;
	*ppGroup = tracker_mem_get_group(group_name);
	if (*ppGroup == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"invalid group_name: %s", \
			__LINE__, group_name);
		return ENOENT;
	}

	if ((*ppGroup)->active_count == 0)
	{
		return ENOENT;
	}

	//file generated by version < v1.12
	if (filename_len < 32 + (FDFS_FILE_EXT_NAME_MAX_LEN + 1))
	{
		storage_ip = INADDR_NONE;
		file_timestamp = 0;
	}
	else //file generated by version >= v1.12
	{
		base64_decode_auto(&g_base64_context, (char *)filename + \
			FDFS_FILE_PATH_LEN, FDFS_FILENAME_BASE64_LENGTH, \
			name_buff, &decoded_len);
		storage_ip = ntohl(buff2int(name_buff));
		file_timestamp = buff2int(name_buff+sizeof(int));
	}

	/*
	logInfo("storage_ip=%d, file_timestamp=%d\n",storage_ip,file_timestamp);
	*/

	memset(szIpAddr, 0, sizeof(szIpAddr));
	if (cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE)
	{
		if (g_groups.download_server == \
				FDFS_DOWNLOAD_SERVER_SOURCE_FIRST)
		{
			memset(&ip_addr, 0, sizeof(ip_addr));
			ip_addr.s_addr = storage_ip;
			pStoreSrcServer=tracker_mem_get_active_storage(\
				*ppGroup, inet_ntop(AF_INET, &ip_addr, \
				szIpAddr, sizeof(szIpAddr)));
			if (pStoreSrcServer != NULL)
			{
				ppStoreServers[(*server_count)++] = \
						 pStoreSrcServer;
				return 0;
			}
		}

		//round robin
		read_server_index = (*ppGroup)->current_read_server;
		if (read_server_index >= (*ppGroup)->active_count)
		{
			read_server_index = 0;
		}
		ppStoreServers[(*server_count)++]=*((*ppGroup)->active_servers \
				+ read_server_index);

		/*
		//logInfo("filename=%s, pStorageServer ip=%s, " \
		"file_timestamp=%d, " \
		"last_synced_timestamp=%d\n", filename, \
		ppStoreServers[0]->ip_addr, file_timestamp, \
		(int)ppStoreServers[0]->stat.last_synced_timestamp);
		 */
		do
		{
			if ((ppStoreServers[0]->stat.last_synced_timestamp > \
				file_timestamp) || \
			(ppStoreServers[0]->stat.last_synced_timestamp + 1 == \
			 file_timestamp&&time(NULL)-file_timestamp>60)\
			|| (storage_ip == INADDR_NONE \
			&& g_groups.store_server == FDFS_STORE_SERVER_ROUND_ROBIN))
			{
				break;
			}

			if (storage_ip == INADDR_NONE)
			{
				ppStoreServers[0] = (*ppGroup)->pStoreServer;
				break;
			}

			memset(&ip_addr, 0, sizeof(ip_addr));
			ip_addr.s_addr = storage_ip;
			inet_ntop(AF_INET, &ip_addr, \
				szIpAddr, sizeof(szIpAddr));
			if (strcmp(szIpAddr, ppStoreServers[0]->ip_addr) == 0)
			{
				break;
			}

			if (g_groups.download_server == \
					FDFS_DOWNLOAD_SERVER_ROUND_ROBIN)
			{  //avoid search again
				pStoreSrcServer=tracker_mem_get_active_storage(
					*ppGroup, szIpAddr);
				if (pStoreSrcServer != NULL)
				{
					ppStoreServers[0] = pStoreSrcServer;
					break;
				}
			}

			if (g_groups.store_server != \
				FDFS_DOWNLOAD_SERVER_ROUND_ROBIN)
			{
				ppStoreServers[0] = (*ppGroup)->pStoreServer;
				break;
			}
		} while (0);

		(*ppGroup)->current_read_server++;
		if ((*ppGroup)->current_read_server >= (*ppGroup)->active_count)
		{
			(*ppGroup)->current_read_server = 0;
		}
	}
	else if (cmd == TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE)
	{
		if (storage_ip != INADDR_NONE)
		{
			memset(&ip_addr, 0, sizeof(ip_addr));
			ip_addr.s_addr = storage_ip;
			pStoreSrcServer=tracker_mem_get_active_storage(\
					*ppGroup, inet_ntop(AF_INET, &ip_addr, \
					szIpAddr, sizeof(szIpAddr)));
			if (pStoreSrcServer != NULL)
			{
				ppStoreServers[(*server_count)++] = \
							 pStoreSrcServer;
				return 0;
			}
		}

		ppStoreServers[0] = tracker_get_writable_storage(*ppGroup);
		*server_count = ppStoreServers[0] != NULL ? 1 : 0;
	}
	else //TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ALL
	{
		memset(szIpAddr, 0, sizeof(szIpAddr));
		if (storage_ip != INADDR_NONE)
		{
			memset(&ip_addr, 0, sizeof(ip_addr));
			ip_addr.s_addr = storage_ip;
			inet_ntop(AF_INET, &ip_addr,szIpAddr,sizeof(szIpAddr));
		}

		current_time = time(NULL);
		ppServerEnd = (*ppGroup)->active_servers + \
				(*ppGroup)->active_count;

		for (ppServer=(*ppGroup)->active_servers; \
				ppServer<ppServerEnd; ppServer++)
		{
			if (((*ppServer)->stat.last_synced_timestamp > \
				file_timestamp) || \
			((*ppServer)->stat.last_synced_timestamp + 1 ==\
			 file_timestamp&&current_time-file_timestamp>60)\
				|| (storage_ip == INADDR_NONE \
					&& g_groups.store_server == \
					FDFS_STORE_SERVER_ROUND_ROBIN)
				|| strcmp((*ppServer)->ip_addr, szIpAddr) == 0)
			{
				ppStoreServers[(*server_count)++] = *ppServer;
			}
		}

		if (*server_count == 0)
		{
			ppStoreServers[(*server_count)++] = \
				 (*ppGroup)->pStoreServer;
		}
	}

	return *server_count > 0 ? 0 : ENOENT;
}

int tracker_mem_check_alive(void *arg)
{
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppServerEnd;
	FDFSGroupInfo *pGroup;
	FDFSGroupInfo *pGroupEnd;
	FDFSStorageDetail *deactiveServers[FDFS_MAX_SERVERS_EACH_GROUP];
	int deactiveCount;
	time_t current_time;

	current_time = time(NULL);
	pGroupEnd = g_groups.groups + g_groups.count;
	for (pGroup=g_groups.groups; pGroup<pGroupEnd; pGroup++)
	{
	deactiveCount = 0;
	ppServerEnd = pGroup->active_servers + pGroup->active_count;
	for (ppServer=pGroup->active_servers; ppServer<ppServerEnd; ppServer++)
	{
		if (current_time - (*ppServer)->stat.last_heart_beat_time > \
			g_check_active_interval)
		{
			deactiveServers[deactiveCount] = *ppServer;
			deactiveCount++;
			if (deactiveCount >= FDFS_MAX_SERVERS_EACH_GROUP)
			{
				break;
			}
		}
	}

	if (deactiveCount == 0)
	{
		continue;
	}

	ppServerEnd = deactiveServers + deactiveCount;
	for (ppServer=deactiveServers; ppServer<ppServerEnd; ppServer++)
	{
	(*ppServer)->status = FDFS_STORAGE_STATUS_OFFLINE;
	tracker_mem_deactive_store_server(pGroup, *ppServer);
	logInfo("ip=%s idle too long, status change to offline!", (*ppServer)->ip_addr);
	}
	}

	return 0;
}

