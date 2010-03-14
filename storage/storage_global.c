/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include "logger.h"
#include "sockopt.h"
#include "shared_func.h"
#include "storage_global.h"

char **g_store_paths = NULL;
int g_path_count = 0;
int g_subdir_count_per_path = DEFAULT_DATA_DIR_COUNT_PER_PATH;

int g_server_port = FDFS_STORAGE_SERVER_DEF_PORT;
int g_http_port = 80;
int g_max_connections = DEFAULT_MAX_CONNECTONS;
//int g_max_write_thread_count = 2;
int g_file_distribute_path_mode = FDFS_FILE_DIST_PATH_ROUND_ROBIN;
int g_file_distribute_rotate_count = FDFS_FILE_DIST_DEFAULT_ROTATE_COUNT;
int g_fsync_after_written_bytes = -1;

int g_dist_path_index_high = 0; //current write to high path
int g_dist_path_index_low = 0;  //current write to low path
int g_dist_write_file_count = 0;  //current write file count

int g_storage_count = 0;
FDFSStorageServer g_storage_servers[FDFS_MAX_SERVERS_EACH_GROUP];
FDFSStorageServer *g_sorted_storages[FDFS_MAX_SERVERS_EACH_GROUP];

char g_group_name[FDFS_GROUP_NAME_MAX_LEN + 1] = {0};
int g_tracker_reporter_count = 0;
int g_heart_beat_interval  = STORAGE_BEAT_DEF_INTERVAL;
int g_stat_report_interval = STORAGE_REPORT_DEF_INTERVAL;

int g_sync_wait_usec = STORAGE_DEF_SYNC_WAIT_MSEC;
int g_sync_interval = 0; //unit: milliseconds
TimeInfo g_sync_start_time = {0, 0};
TimeInfo g_sync_end_time = {23, 59};
bool g_sync_part_time = false;
int g_sync_log_buff_interval = SYNC_LOG_BUFF_DEF_INTERVAL;
int g_sync_binlog_buff_interval = SYNC_BINLOG_BUFF_DEF_INTERVAL;

FDFSStorageStat g_storage_stat;
int g_stat_change_count = 1;
int g_sync_change_count = 0;

int g_storage_join_time = 0;
bool g_sync_old_done = false;
char g_sync_src_ip_addr[IP_ADDRESS_SIZE] = {0};
int g_sync_until_timestamp = 0;

int g_local_host_ip_count = 0;
char g_local_host_ip_addrs[STORAGE_MAX_LOCAL_IP_ADDRS * \
				IP_ADDRESS_SIZE];

char g_tracker_client_ip[IP_ADDRESS_SIZE] = {0}; //storage ip as tracker client
char g_last_storage_ip[IP_ADDRESS_SIZE] = {0};	 //the last storage ip address

int g_allow_ip_count = 0;
in_addr_t *g_allow_ip_addrs = NULL;
bool g_check_file_duplicate = false;
char g_key_namespace[FDHT_MAX_NAMESPACE_LEN+1] = {0};
int g_namespace_len = 0;

struct base64_context g_base64_context;

char g_run_by_group[32] = {0};
char g_run_by_user[32] = {0};

char g_bind_addr[IP_ADDRESS_SIZE] = {0};
bool g_client_bind_addr = true;
bool g_storage_ip_changed_auto_adjust = false;
bool g_thread_kill_done = false;
char g_if_alias_prefix[STORAGE_IF_ALIAS_PREFIX_MAX_SIZE] = {0};

int g_thread_stack_size = 512 * 1024;
int g_upload_priority = DEFAULT_UPLOAD_PRIORITY;
time_t g_up_time = 0;

#ifdef WITH_HTTPD
FDFSHTTPParams g_http_params;
int g_http_trunk_size = 64 * 1024;
#endif

#if defined(DEBUG_FLAG) && defined(OS_LINUX)
char g_exe_name[256] = {0};
#endif

int storage_cmp_by_ip_addr(const void *p1, const void *p2)
{
	return strcmp((*((FDFSStorageServer **)p1))->server.ip_addr,
		(*((FDFSStorageServer **)p2))->server.ip_addr);
}

bool is_local_host_ip(const char *client_ip)
{
	char *p;
	char *pEnd;

	pEnd = g_local_host_ip_addrs + \
		IP_ADDRESS_SIZE * g_local_host_ip_count;
	for (p=g_local_host_ip_addrs; p<pEnd; p+=IP_ADDRESS_SIZE)
	{
		if (strcmp(client_ip, p) == 0)
		{
			return true;
		}
	}

	return false;
}

int insert_into_local_host_ip(const char *client_ip)
{
	if (is_local_host_ip(client_ip))
	{
		return 0;
	}

	if (g_local_host_ip_count >= STORAGE_MAX_LOCAL_IP_ADDRS)
	{
		return -1;
	}

	strcpy(g_local_host_ip_addrs + \
		IP_ADDRESS_SIZE * g_local_host_ip_count, \
		client_ip);
	g_local_host_ip_count++;
	return 1;
}

void load_local_host_ip_addrs()
{
#define STORAGE_MAX_ALIAS_PREFIX_COUNT   4
	char ip_addresses[STORAGE_MAX_LOCAL_IP_ADDRS][IP_ADDRESS_SIZE];
	int count;
	int k;
	char *if_alias_prefixes[STORAGE_MAX_ALIAS_PREFIX_COUNT];
	int alias_count;

	insert_into_local_host_ip("127.0.0.1");

	memset(if_alias_prefixes, 0, sizeof(if_alias_prefixes));
	if (*g_if_alias_prefix == '\0')
	{
		alias_count = 0;
	}
	else
	{
		alias_count = splitEx(g_if_alias_prefix, ',', \
			if_alias_prefixes, STORAGE_MAX_ALIAS_PREFIX_COUNT);
	}

	if (gethostaddrs(if_alias_prefixes, alias_count, ip_addresses, \
			STORAGE_MAX_LOCAL_IP_ADDRS, &count) != 0)
	{
		return;
	}

	for (k=0; k<count; k++)
	{
		insert_into_local_host_ip(ip_addresses[k]);
	}

	//print_local_host_ip_addrs();
}

void print_local_host_ip_addrs()
{
	char *p;
	char *pEnd;

	printf("local_host_ip_count=%d\n", g_local_host_ip_count);
	pEnd = g_local_host_ip_addrs + \
		IP_ADDRESS_SIZE * g_local_host_ip_count;
	for (p=g_local_host_ip_addrs; p<pEnd; p+=IP_ADDRESS_SIZE)
	{
		printf("%d. %s\n", (int)((p-g_local_host_ip_addrs)/ \
				IP_ADDRESS_SIZE)+1, p);
	}

	printf("\n");
}

