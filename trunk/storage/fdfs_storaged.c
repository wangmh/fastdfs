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
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include "shared_func.h"
#include "pthread_func.h"
#include "logger.h"
#include "fdfs_global.h"
#include "ini_file_reader.h"
#include "sockopt.h"
#include "tracker_types.h"
#include "tracker_client_thread.h"
#include "storage_global.h"
#include "storage_func.h"
#include "storage_sync.h"
#include "storage_service.h"
#include "base64.h"
#include "sched_thread.h"

#ifdef WITH_HTTPD
#include "storage_httpd.h"
#endif

#if defined(DEBUG_FLAG) 

#if defined(OS_LINUX)
#include "linux_stack_trace.h"

static bool bSegmentFault = false;
#endif

#include "storage_dump.h"
#endif

static bool bTerminateFlag = false;

static void sigQuitHandler(int sig);
static void sigHupHandler(int sig);
static void sigUsrHandler(int sig);

#if defined(DEBUG_FLAG)

#if defined(OS_LINUX)
static void sigSegvHandler(int signum, siginfo_t *info, void *ptr);
#endif

static void sigDumpHandler(int sig);
#endif

#define SCHEDULE_ENTRIES_COUNT 3

int main(int argc, char *argv[])
{
	char *conf_filename;
	
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

	log_init();
	g_up_time = time(NULL);

#if defined(DEBUG_FLAG) && defined(OS_LINUX)
	if (getExeAbsoluteFilename(argv[0], g_exe_name, \
		sizeof(g_exe_name)) == NULL)
	{
		log_destroy();
		return errno != 0 ? errno : ENOENT;
	}
#endif

	conf_filename = argv[1];
	memset(g_bind_addr, 0, sizeof(g_bind_addr));
	if ((result=storage_func_init(conf_filename, \
			g_bind_addr, sizeof(g_bind_addr))) != 0)
	{
		log_destroy();
		return result;
	}

	sock = socketServer(g_bind_addr, g_server_port, &result);
	if (sock < 0)
	{
		log_destroy();
		return result;
	}

	if ((result=tcpsetserveropt(sock, g_fdfs_network_timeout)) != 0)
	{
		log_destroy();
		return result;
	}

	if ((result=storage_sync_init()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"storage_sync_init fail, program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	if ((result=tracker_report_init()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"tracker_report_init fail, program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	if ((result=storage_service_init()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"storage_service_init fail, program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	base64_init_ex(&g_base64_context, 0, '-', '_', '.');
	if ((result=set_rand_seed()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"set_rand_seed fail, program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	daemon_init(true);
	umask(0);

	if (dup2(g_log_context.log_fd, STDOUT_FILENO) < 0 || \
		dup2(g_log_context.log_fd, STDERR_FILENO) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call dup2 fail, errno: %d, error info: %s, " \
			"program exit!", __LINE__, errno, strerror(errno));
		g_continue_flag = false;
		return errno;
	}

	memset(&act, 0, sizeof(act));
	sigemptyset(&act.sa_mask);

	act.sa_handler = sigUsrHandler;
	if(sigaction(SIGUSR1, &act, NULL) < 0 || \
		sigaction(SIGUSR2, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}

	act.sa_handler = sigHupHandler;
	if(sigaction(SIGHUP, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}
	
	act.sa_handler = SIG_IGN;
	if(sigaction(SIGPIPE, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
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
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}

#if defined(DEBUG_FLAG)

#if defined(OS_LINUX)
	memset(&act, 0, sizeof(act));
        act.sa_sigaction = sigSegvHandler;
        act.sa_flags = SA_SIGINFO;
        if (sigaction(SIGSEGV, &act, NULL) < 0 || \
        	sigaction(SIGABRT, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}
#endif

	memset(&act, 0, sizeof(act));
	sigemptyset(&act.sa_mask);
	act.sa_handler = sigDumpHandler;
	if(sigaction(SIGUSR1, &act, NULL) < 0 || \
		sigaction(SIGUSR2, &act, NULL) < 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}
#endif

#ifdef WITH_HTTPD
	if (!g_http_params.disabled)
	{
		if ((result=storage_httpd_start(g_bind_addr)) != 0)
		{
			logCrit("file: "__FILE__", line: %d, " \
				"storage_httpd_start fail, " \
				"program exit!", __LINE__);
			return result;
		}
	}
#endif

	if ((result=tracker_report_thread_start()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"tracker_report_thread_start fail, " \
			"program exit!", __LINE__);
		g_continue_flag = false;
		storage_func_destroy();
		log_destroy();
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
	scheduleEntries[0].func_args = &g_log_context;

	scheduleEntries[1].id = 2;
	scheduleEntries[1].time_base.hour = TIME_NONE;
	scheduleEntries[1].time_base.minute = TIME_NONE;
	scheduleEntries[1].interval = g_sync_binlog_buff_interval;
	scheduleEntries[1].task_func = fdfs_binlog_sync_func;
	scheduleEntries[1].func_args = NULL;

	scheduleEntries[2].id = 3;
	scheduleEntries[2].time_base.hour = TIME_NONE;
	scheduleEntries[2].time_base.minute = TIME_NONE;
	scheduleEntries[2].interval = g_sync_stat_file_interval;
	scheduleEntries[2].task_func = fdfs_stat_file_sync_func;
	scheduleEntries[2].func_args = NULL;
	if ((result=sched_start(&scheduleArray, &schedule_tid, \
			g_thread_stack_size, &g_continue_flag)) != 0)
	{
		log_destroy();
		return result;
	}

	if ((result=set_run_by(g_run_by_group, g_run_by_user)) != 0)
	{
		log_destroy();
		return result;
	}

	tids = (pthread_t *)malloc(sizeof(pthread_t) * g_max_connections);
	if (tids == NULL)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"malloc fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}
	memset(tids, 0, sizeof(pthread_t) * g_max_connections);

	g_storage_thread_count = g_max_connections;
	if ((result=create_work_threads(&g_storage_thread_count, \
		storage_thread_entrance, (void *)(long)sock, tids, \
		g_thread_stack_size)) != 0)
	{
		free(tids);
		log_destroy();
		return result;
	}

	log_set_cache(true);

	g_thread_kill_done = false;
	bTerminateFlag = false;
	while (g_continue_flag)
	{
		sleep(1);

		if (bTerminateFlag)
		{
			g_continue_flag = false;

			if (g_schedule_flag)
			{
				pthread_kill(schedule_tid, SIGINT);
			}
			kill_tracker_report_threads();
			kill_storage_sync_threads();
			kill_work_threads(tids, g_max_connections);
			g_thread_kill_done = true;

			break;
		}
	}

	while (g_storage_thread_count != 0 || \
		g_tracker_reporter_count > 0 || \
		g_storage_sync_thread_count > 0 || \
		g_schedule_flag)
	{

#if defined(DEBUG_FLAG) && defined(OS_LINUX)
		if (bSegmentFault)
		{
			sleep(5);
			break;
		}
#endif

		usleep(50000);
	}

	tracker_report_destroy();
	storage_service_destroy();
	storage_sync_destroy();
	storage_func_destroy();

	free(tids);

	logInfo("exit nomally.\n");
	log_destroy();
	
	return 0;
}

static void sigQuitHandler(int sig)
{
	if (!bTerminateFlag)
	{
		bTerminateFlag = true;
		logCrit("file: "__FILE__", line: %d, " \
			"catch signal %d, program exiting...", \
			__LINE__, sig);
	}
}

static void sigHupHandler(int sig)
{
	logInfo("file: "__FILE__", line: %d, " \
		"catch signal %d, ignore it", __LINE__, sig);
}

static void sigUsrHandler(int sig)
{
	logInfo("file: "__FILE__", line: %d, " \
		"catch signal %d, ignore it", __LINE__, sig);
}

#if defined(DEBUG_FLAG)

#if defined(OS_LINUX)
static void sigSegvHandler(int signum, siginfo_t *info, void *ptr)
{
	bSegmentFault = true;

	if (!bTerminateFlag)
	{
		bTerminateFlag = true;
		logCrit("file: "__FILE__", line: %d, " \
			"catch signal %d, program exiting...", \
			__LINE__, signum);
	
		signal_stack_trace_print(signum, info, ptr);
	}
}
#endif

static void sigDumpHandler(int sig)
{
	static bool bDumpFlag = false;
	char filename[256];

	if (bDumpFlag)
	{
		return;
	}

	bDumpFlag = true;

	snprintf(filename, sizeof(filename), 
		"%s/logs/storage_dump.log", g_fdfs_base_path);
	fdfs_dump_storage_global_vars_to_file(filename);

	bDumpFlag = false;
}
#endif

