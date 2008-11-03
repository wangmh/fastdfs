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
#include "tracker_mem.h"
#include "shared_func.h"

static pthread_mutex_t mem_thread_lock;

#define STORAGE_GROUPS_LIST_FILENAME	"storage_groups.dat"
#define STORAGE_SERVERS_LIST_FILENAME	"storage_servers.dat"
#define STORAGE_SYNC_TIMESTAMP_FILENAME	"storage_sync_timestamp.dat"
#define STORAGE_DATA_FIELD_SEPERATOR	','

#define TRACKER_MEM_ALLOC_ONCE	5

static int tracker_load_groups(const char *data_path)
{
#define STORAGE_DATA_GROUP_FIELDS	2

	FILE *fp;
	char szLine[256];
	char *fields[STORAGE_DATA_GROUP_FIELDS];
	int result;
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

		if (splitEx(szLine, STORAGE_DATA_FIELD_SEPERATOR, \
			fields, STORAGE_DATA_GROUP_FIELDS) != \
				STORAGE_DATA_GROUP_FIELDS)
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
	}

	fclose(fp);
	return result;
}

static int tracker_locate_storage_sync_server(FDFSStorageSync *pStorageSyncs, \
		const int nStorageSyncCount, const bool bLoadFromFile)
{
	FDFSStorageSync *pSyncServer;
	FDFSStorageSync *pSyncEnd;

	pSyncEnd = pStorageSyncs + nStorageSyncCount;
	for (pSyncServer=pStorageSyncs; pSyncServer<pSyncEnd; pSyncServer++)
	{
		pSyncServer->pStorage->psync_src_server = \
			tracker_mem_get_storage(pSyncServer->pGroup, \
			pSyncServer->sync_src_ip_addr);
		if (pSyncServer->pStorage->psync_src_server == NULL)
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
				__LINE__, buff, \
				pSyncServer->pGroup->group_name, \
				pSyncServer->sync_src_ip_addr, \
				pSyncServer->pGroup->storage_port);

			return ENOENT;
		}
	}

	return 0;
}

static int tracker_load_storages(const char *data_path)
{
#define STORAGE_DATA_SERVER_FIELDS	17

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

		if ((cols=splitEx(szLine, STORAGE_DATA_FIELD_SEPERATOR, \
			fields, STORAGE_DATA_SERVER_FIELDS)) != \
				STORAGE_DATA_SERVER_FIELDS)
		{
			logError("file: "__FILE__", line: %d, " \
				"the format of the file \"%s/%s\" is invalid" \
				", colums: %d != expect colums: %d", \
				__LINE__, data_path, \
				STORAGE_SERVERS_LIST_FILENAME, \
				cols, STORAGE_DATA_SERVER_FIELDS);
			result = errno != 0 ? errno : EINVAL;
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

		pStorageSyncs[nStorageSyncCount].pGroup = clientInfo.pGroup;
		pStorageSyncs[nStorageSyncCount].pStorage = clientInfo.pStorage;
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
	int min_synced_timestamp;
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
			min_synced_timestamp = 0;
			for (src_index=0; src_index<pGroup->count; src_index++)
			{
				if (src_index == dest_index)
				{
					continue;
				}

				curr_synced_timestamp = \
					pGroup->last_sync_timestamps \
							[src_index][dest_index];
				if (min_synced_timestamp == 0)
				{
				min_synced_timestamp = curr_synced_timestamp;
				}
				else if (curr_synced_timestamp < \
					min_synced_timestamp)
				{
				min_synced_timestamp = curr_synced_timestamp;
				}
			}

			pGroup->all_servers[dest_index].last_synced_timestamp =\
					min_synced_timestamp;
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

	if ((result=tracker_load_sync_timestamps(data_path)) != 0)
	{
		return result;
	}

	return 0;
}

static int tracker_save_groups()
{
	char filname[MAX_PATH_SIZE];
	char buff[FDFS_GROUP_NAME_MAX_LEN + 16];
	int fd;
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppEnd;
	int result;
	int len;

	snprintf(filname, sizeof(filname), "%s/data/%s", \
		g_base_path, STORAGE_GROUPS_LIST_FILENAME);
	if ((fd=open(filname, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filname, errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	result = 0;
	ppEnd = g_groups.sorted_groups + g_groups.count;
	for (ppGroup=g_groups.sorted_groups; ppGroup<ppEnd; ppGroup++)
	{
		len = sprintf(buff, "%s%c%d\n", (*ppGroup)->group_name, \
				STORAGE_DATA_FIELD_SEPERATOR, \
				(*ppGroup)->storage_port);
		if (write(fd, buff, len) != len)
		{
			logError("file: "__FILE__", line: %d, " \
				"write to file \"%s\" fail, " \
				"errno: %d, error info: %s", \
				__LINE__, filname, errno, strerror(errno));
			result = errno != 0 ? errno : EIO;
			break;
		}
	}

	close(fd);
	return result;
}

int tracker_save_storages()
{
	char filname[MAX_PATH_SIZE];
	char buff[256];
	int fd;
	int len;
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppGroupEnd;
	FDFSStorageDetail *pStorage;
	FDFSStorageDetail *pStorageEnd;
	int result;

	snprintf(filname, sizeof(filname), "%s/data/%s", \
		g_base_path, STORAGE_SERVERS_LIST_FILENAME);
	if ((fd=open(filname, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filname, errno, strerror(errno));
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
				"%d\n", \
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
				(int)(pStorage->stat.last_sync_update) \
	 		    );

			if (write(fd, buff, len) != len)
			{
				logError("file: "__FILE__", line: %d, " \
					"write to file \"%s\" fail, " \
					"errno: %d, error info: %s", \
					__LINE__, filname, \
					errno, strerror(errno));
				result = errno != 0 ? errno : EIO;
				break;
			}
		}
	}

	close(fd);
	return result;
}

int tracker_save_sync_timestamps()
{
	char filname[MAX_PATH_SIZE];
	char buff[512];
	int fd;
	int len;
	FDFSGroupInfo **ppGroup;
	FDFSGroupInfo **ppGroupEnd;
	int **last_sync_timestamps;
	int i;
	int k;
	int result;

	snprintf(filname, sizeof(filname), "%s/data/%s", \
		g_base_path, STORAGE_SYNC_TIMESTAMP_FILENAME);
	if ((fd=open(filname, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open \"%s\" fail, " \
			"errno: %d, error info: %s", \
			__LINE__, filname, errno, strerror(errno));
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
					__LINE__, filname, \
					errno, strerror(errno));
				result = errno != 0 ? errno : EIO;
				break;
			}
		}
	}

	close(fd);
	return result;
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

	if (pthread_mutex_destroy(&mem_thread_lock) != 0)
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

int tracker_mem_realloc_store_server(FDFSGroupInfo *pGroup, const int inc_count)
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

		pStorageSyncs[nStorageSyncCount].pGroup = pGroup;
		pStorageSyncs[nStorageSyncCount].pStorage = pServer;
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
		while (1)
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

			break;
		}

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
	if (pStorageServer == NULL)
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
			pServer->psync_src_server->ip_addr[0] = '\0';
		}
	}

	pStorageServer->status = FDFS_STORAGE_STATUS_DELETED;
	pGroup->version++;
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
		while (1)
		{
			if (pClientInfo->pGroup->count >= \
				pClientInfo->pGroup->alloc_size)
			{
				result = tracker_mem_realloc_store_server( \
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
			pClientInfo->pGroup->version++;
			break;
		}

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
		//printf("group: %s, pStorageServer->ref_count=%d\n", pClientInfo->pGroup->group_name, *(pStorageServer->ref_count));
	}
	pClientInfo->pStorage = pStorageServer;
	pClientInfo->pAllocedStorages = pClientInfo->pGroup->all_servers;
	return 0;
}

int tracker_mem_add_group_and_storage(TrackerClientInfo *pClientInfo, \
			const bool bIncRef)
{
	int result;
	bool bGroupInserted;
	bool bStorageInserted;

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

	if ((result=tracker_mem_add_storage(pClientInfo, bIncRef, \
			&bStorageInserted)) != 0)
	{
		return result;
	}
	if (bStorageInserted)
	{
		pClientInfo->pStorage->status = FDFS_STORAGE_STATUS_INIT;
		if ((result=tracker_save_storages()) != 0)
		{
			return result;
		}
	}
	else
	{
		pClientInfo->pStorage->status = FDFS_STORAGE_STATUS_ONLINE;
	}

	return 0;
}

int tracker_mem_sync_storages(TrackerClientInfo *pClientInfo, \
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
	while (1)
	{
		if (pClientInfo->pGroup->count + server_count >= \
			pClientInfo->pGroup->alloc_size)
		{
			result = tracker_mem_realloc_store_server( \
					pClientInfo->pGroup, server_count);
			if (result != 0)
			{
				break;
			}
		}

		memset(&target_storage, 0, sizeof(target_storage));
		pStorageServer = pClientInfo->pGroup->all_servers \
					 + pClientInfo->pGroup->count;
		pEnd = briefServers + server_count;
		for (pServer=briefServers; pServer<pEnd; pServer++)
		{
			pServer->ip_addr[IP_ADDRESS_SIZE-1] = '\0';
			memcpy(target_storage.ip_addr, pServer->ip_addr, \
				IP_ADDRESS_SIZE);
			pTargetStorage = &target_storage;
			if ((ppFound=(FDFSStorageDetail **)bsearch( \
				&pTargetStorage, \
				pClientInfo->pGroup->sorted_servers, \
				pClientInfo->pGroup->count, \
				sizeof(FDFSStorageDetail *), \
				tracker_mem_cmp_by_ip_addr)) != NULL)
			{
				if (((pServer->status > (*ppFound)->status) && \
					(((*ppFound)->status == \
					FDFS_STORAGE_STATUS_WAIT_SYNC) || \
					((*ppFound)->status == \
					FDFS_STORAGE_STATUS_SYNCING))) || \
					(pServer->status != (*ppFound)->status \
					 && pServer->status == \
						FDFS_STORAGE_STATUS_DELETED))
				{
					(*ppFound)->status = pServer->status;
					pClientInfo->pGroup->version++;
				}

				continue;
			}

			pStorageServer->status = pServer->status;
			memcpy(pStorageServer->ip_addr, pServer->ip_addr, \
				IP_ADDRESS_SIZE);

			tracker_mem_insert_into_sorted_servers( \
				pStorageServer, \
				pClientInfo->pGroup->sorted_servers, \
				pClientInfo->pGroup->count);

			pStorageServer++;
			pClientInfo->pGroup->count++;
		}

		break;
	}

	if (pthread_mutex_unlock(&mem_thread_lock) != 0)
	{
		logError("file: "__FILE__", line: %d, "   \
			"call pthread_mutex_unlock fail", \
			__LINE__);
	}

	return result;
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
		pGroup->version++;
		if (pGroup->current_write_server >= pGroup->active_count)
		{
			pGroup->current_write_server = 0;
		}
		if (pGroup->current_read_server >= pGroup->active_count)
		{
			pGroup->current_read_server = 0;
		}
	}

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
		pGroup->version++;
	}

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

	pClientInfo->pStorage->status = FDFS_STORAGE_STATUS_OFFLINE;
	return tracker_mem_deactive_store_server(pClientInfo->pGroup, \
				pClientInfo->pStorage);
}

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

