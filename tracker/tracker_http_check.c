#include <sys/types.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include "logger.h"
#include "fdfs_global.h"
#include "tracker_global.h"
#include "tracker_mem.h"
#include "tracker_proto.h"
#include "http_func.h"
#include "tracker_http_check.h"

static pthread_t http_check_tid;

static void *http_check_entrance(void *arg)
{
	FDFSGroupInfo *pGroup;
	FDFSGroupInfo *pGroupEnd;
	FDFSStorageDetail **ppServer;
	FDFSStorageDetail **ppServerEnd;
	char url[256];
	char *content;
	int content_len;
	int http_status;
	int server_count;
	int result;

	g_http_servers_dirty = false;
	while (g_continue_flag)
	{
	if (g_http_servers_dirty)
	{
		g_http_servers_dirty = false;
	}
	else
	{
		sleep(g_http_check_interval);
	}

	pGroupEnd = g_groups.groups + g_groups.count;
	for (pGroup=g_groups.groups; g_continue_flag && (!g_http_servers_dirty)\
		&& pGroup<pGroupEnd; pGroup++)
        {
	server_count = 0;
	ppServerEnd = pGroup->active_servers + pGroup->active_count;
	for (ppServer=pGroup->active_servers; g_continue_flag && \
		(!g_http_servers_dirty) && ppServer<ppServerEnd; ppServer++)
	{
		sprintf(url, "http://%s:%d%s", (*ppServer)->ip_addr, \
			pGroup->storage_http_port, g_http_check_uri);

		result = get_url_content(url, g_network_timeout, &http_status, \
        			&content, &content_len);

		logInfo("file: "__FILE__", line: %d, " \
			"url=%s, result=%d, http_status=%d", \
			__LINE__, url, result, http_status);

		if (result == 0 && http_status == 200)
		{
			*(pGroup->http_servers + server_count) = *ppServer;
			server_count++;
		}
	}

	if (pGroup->http_server_count != server_count)
	{
		logDebug("file: "__FILE__", line: %d, " \
			"HTTP server count change from %d to %d", \
			__LINE__, pGroup->http_server_count, server_count);

		pGroup->http_server_count = server_count;
	}
	}
	}

	return NULL;
}

int tracker_http_check_start()
{
	int result;

	if (g_http_check_interval <= 0)
	{
		return 0;
	}

	if ((result=pthread_create(&http_check_tid, NULL, \
			http_check_entrance, NULL)) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"create thread failed, errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	return 0;
}

int tracker_http_check_stop()
{
	if (g_http_check_interval <= 0)
	{
		return 0;
	}

	return pthread_kill(http_check_tid, SIGINT);
}

