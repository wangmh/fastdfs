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
#include "sockopt.h"
#include "fdfs_global.h"
#include "pthread_func.h"
#include "tracker_global.h"
#include "tracker_mem.h"

static int relationship_select_leader()
{
	return 0;
}

static int relationship_ping_leader()
{
	return 0;
}

static void *relationship_thread_entrance(void* arg)
{
	while (g_continue_flag)
	{
		if (g_tracker_servers.servers != NULL)
		{
			if (g_tracker_servers.leader_index < 0)
			{
				relationship_select_leader();
			}
			else
			{
				relationship_ping_leader();
			}
		}

		if (g_last_tracker_servers != NULL)
		{
			tracker_mem_file_lock();

			free(g_last_tracker_servers);
			g_last_tracker_servers = NULL;

			tracker_mem_file_unlock();
		}

		sleep(1);
	}

	return NULL;
}

int tracker_relationship_init()
{
	int result;
	pthread_t tid;
	pthread_attr_t thread_attr;

	if ((result=init_pthread_attr(&thread_attr, g_thread_stack_size)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_attr fail, program exit!", __LINE__);
		return result;
	}

	if ((result=pthread_create(&tid, &thread_attr, \
			relationship_thread_entrance, NULL)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"create thread failed, errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	pthread_attr_destroy(&thread_attr);

	return 0;
}

int tracker_relationship_destroy()
{
	return 0;
}

