/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include "shared_func.h"
#include "logger.h"
#include "fdfs_global.h"
#include "fdfs_base64.h"
#include "sockopt.h"
#include "sched_thread.h"
#include "tracker_types.h"
#include "tracker_mem.h"
#include "tracker_service.h"
#include "tracker_global.h"
#include "tracker_func.h"

#ifdef WITH_HTTPD
#include "tracker_httpd.h"
#endif

bool bTerminateFlag = false;

void sigQuitHandler(int sig);
void sigHupHandler(int sig);
void sigUsrHandler(int sig);

#define SCHEDULE_ENTRIES_COUNT 2

int main(int argc, char *argv[])
{
	char *conf_filename;
	char bind_addr[IP_ADDRESS_SIZE];
	int result;
	int sock;
	pthread_t *tids;
	pthread_t schedule_tid;
	struct sigaction act;
	ScheduleEntry scheduleEntries[SCHEDULE_ENTRIES_COUNT];
	ScheduleArray scheduleArray;
	
	if (argc < 2)
	{
		printf("Usage: %s <config_file>\n", argv[0]);
		return 1;
	}

	conf_filename = argv[1];
	memset(bind_addr, 0, sizeof(bind_addr));
	if ((result=tracker_load_from_conf_file(conf_filename, \
			bind_addr, sizeof(bind_addr))) != 0)
	{
		return result;
	}

	base64_init_ex(&g_base64_context, 0, '-', '_', '.');
	if ((result=set_rand_seed()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"set_rand_seed fail, program exit!", __LINE__);
		return result;
	}

	if ((result=tracker_mem_init()) != 0)
	{
		return result;
	}
	
	sock = socketServer(bind_addr, g_server_port, &result);
	if (sock < 0)
	{
		return result;
	}

	if ((result=tcpsetserveropt(sock, g_network_timeout)) != 0)
	{
		return result;
	}

	daemon_init(true);
	umask(0);
	
	if ((result=init_pthread_lock( \
			&g_tracker_thread_lock)) != 0)
	{
		return result;
	}
	
	g_tracker_thread_count = 0;

	memset(&act, 0, sizeof(act));
	sigemptyset(&act.sa_mask);

	act.sa_handler = sigUsrHandler;
	if(sigaction(SIGUSR1, &act, NULL) < 0 || \
		sigaction(SIGUSR2, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}

	act.sa_handler = sigHupHandler;
	if(sigaction(SIGHUP, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}
	
	act.sa_handler = SIG_IGN;
	if(sigaction(SIGPIPE, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}

	act.sa_handler = sigQuitHandler;
	if(sigaction(SIGINT, &act, NULL) < 0 || \
		sigaction(SIGTERM, &act, NULL) < 0 || \
		sigaction(SIGABRT, &act, NULL) < 0 || \
		sigaction(SIGQUIT, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}

#ifdef WITH_HTTPD
	if (!g_http_params.disabled)
	{
		if ((result=tracker_httpd_start(bind_addr)) != 0)
		{
			return result;
		}
	}
#endif

	if ((result=set_run_by(g_run_by_group, g_run_by_user)) != 0)
	{
		return result;
	}

	scheduleArray.entries = scheduleEntries;
	scheduleArray.count = SCHEDULE_ENTRIES_COUNT;

	memset(scheduleEntries, 0, sizeof(scheduleEntries));
	scheduleEntries[0].id = 1;
	scheduleEntries[0].time_base.hour = TIME_NONE;
	scheduleEntries[0].time_base.minute = TIME_NONE;
	scheduleEntries[0].interval = g_sync_log_buff_interval;
	scheduleEntries[0].task_func = log_sync_func;
	scheduleEntries[0].func_args = NULL;

	scheduleEntries[1].id = 2;
	scheduleEntries[1].time_base.hour = TIME_NONE;
	scheduleEntries[1].time_base.minute = TIME_NONE;
	scheduleEntries[1].interval = g_check_active_interval;
	scheduleEntries[1].task_func = tracker_mem_check_alive;
	scheduleEntries[1].func_args = NULL;
	if ((result=sched_start(&scheduleArray, &schedule_tid, \
		g_thread_stack_size)) != 0)
	{
		return result;
	}

	tids = (pthread_t *)malloc(sizeof(pthread_t) * g_max_connections);
	if (tids == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}

	g_tracker_thread_count = g_max_connections;
	if ((result=create_work_threads(&g_tracker_thread_count, \
		tracker_thread_entrance, (void *)sock, tids, \
		g_thread_stack_size)) != 0)
	{
		free(tids);
		return result;
	}

	log_set_cache(true);

	bTerminateFlag = false;
	while (g_continue_flag)
	{
		if (bTerminateFlag)
		{
			pthread_kill(schedule_tid, SIGINT);
			kill_work_threads(tids, g_max_connections);
			g_continue_flag = false;

			break;
		}

		sleep(1);
	}

	while ((g_tracker_thread_count != 0) || g_schedule_flag)
	{
		sleep(1);
	}
	
	tracker_mem_destroy();

	pthread_mutex_destroy(&g_tracker_thread_lock);
	
	free(tids);

	logInfo("exit nomally.\n");
	log_destory();
	
	return 0;
}

void sigQuitHandler(int sig)
{
	if (!bTerminateFlag)
	{
		bTerminateFlag = true;
		logCrit("file: "__FILE__", line: %d, " \
			"catch signal %d, program exiting...", \
			__LINE__, sig);
	}
}

void sigHupHandler(int sig)
{
}

void sigUsrHandler(int sig)
{
	/*
	logInfo("current thread count=%d, " \
		"mo count=%d, success count=%d", g_tracker_thread_count, \nMoCount, nSuccMoCount);
	*/
}

