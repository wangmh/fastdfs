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
	int result;

	pGroupEnd = g_groups.groups + g_groups.count;
	for (pGroup=g_groups.groups; pGroup<pGroupEnd; pGroup++)
        {
	ppServerEnd = pGroup->active_servers + pGroup->active_count;
	for (ppServer=pGroup->active_servers; ppServer<ppServerEnd; ppServer++)
	{
		(*ppServer)
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

int tracker_http_check_kill()
{
	if (g_http_check_interval <= 0)
	{
		return 0;
	}

	return pthread_kill(http_check_tid, SIGINT);
}

