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
#include "pthread_func.h"
#include "logger.h"
#include "fdfs_global.h"
#include "base64.h"
#include "sockopt.h"
#include "sched_thread.h"
#include "tracker_types.h"
#include "tracker_mem.h"
#include "tracker_service.h"
#include "tracker_global.h"
#include "tracker_proto.h"
#include "tracker_func.h"

#ifdef WITH_HTTPD
#include "tracker_httpd.h"
#include "tracker_http_check.h"
#endif

#if defined(DEBUG_FLAG)

/*
#if defined(OS_LINUX)
#include "linux_stack_trace.h"
static bool bSegmentFault = false;
#endif
*/

#include "tracker_dump.h"
#endif

static bool bTerminateFlag = false;
static bool bAcceptEndFlag = false;

static char bind_addr[IP_ADDRESS_SIZE];

static void sigQuitHandler(int sig);
static void sigHupHandler(int sig);
static void sigUsrHandler(int sig);
static void sigAlarmHandler(int sig);

#if defined(DEBUG_FLAG)
/*
#if defined(OS_LINUX)
static void sigSegvHandler(int signum, siginfo_t *info, void *ptr);
#endif
*/

static void sigDumpHandler(int sig);
#endif

#define SCHEDULE_ENTRIES_COUNT 2

int main(int argc, char *argv[])
{
	char *conf_filename;
	int result;
	int sock;
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

#if defined(DEBUG_FLAG) && defined(OS_LINUX)
	if (getExeAbsoluteFilename(argv[0], g_exe_name, \
		sizeof(g_exe_name)) == NULL)
	{
		log_destroy();
		return errno != 0 ? errno : ENOENT;
	}
#endif

	conf_filename = argv[1];
	memset(bind_addr, 0, sizeof(bind_addr));
	if ((result=tracker_load_from_conf_file(conf_filename, \
			bind_addr, sizeof(bind_addr))) != 0)
	{
		log_destroy();
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
		log_destroy();
		return result;
	}
	
	sock = socketServer(bind_addr, g_server_port, &result);
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

	if ((result=tracker_service_init()) != 0)
	{
		log_destroy();
		return result;
	}
	
	g_tracker_thread_count = 0;

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
/*
#if defined(OS_LINUX)
	memset(&act, 0, sizeof(act));
	sigemptyset(&act.sa_mask);
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
*/

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
		if ((result=tracker_httpd_start(bind_addr)) != 0)
		{
			logCrit("file: "__FILE__", line: %d, " \
				"tracker_httpd_start fail, program exit!", \
				__LINE__);
			return result;
		}

	}

	if ((result=tracker_http_check_start()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"tracker_http_check_start fail, " \
			"program exit!", __LINE__);
		return result;
	}
#endif

	if ((result=set_run_by(g_run_by_group, g_run_by_user)) != 0)
	{
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
	scheduleEntries[1].interval = g_check_active_interval;
	scheduleEntries[1].task_func = tracker_mem_check_alive;
	scheduleEntries[1].func_args = NULL;
	if ((result=sched_start(&scheduleArray, &schedule_tid, \
		g_thread_stack_size, &g_continue_flag)) != 0)
	{
		log_destroy();
		return result;
	}

	log_set_cache(true);

	bTerminateFlag = false;
	bAcceptEndFlag = false;

	tracker_accept_loop(sock);
	bAcceptEndFlag = true;
	if (g_schedule_flag)
	{
		pthread_kill(schedule_tid, SIGINT);
	}
	tracker_terminate_threads();

#ifdef WITH_HTTPD
	tracker_http_check_stop();
#endif

	while ((g_tracker_thread_count != 0) || g_schedule_flag)
	{

/*
#if defined(DEBUG_FLAG) && defined(OS_LINUX)
		if (bSegmentFault)
		{
			sleep(5);
			break;
		}
#endif
*/

		usleep(50000);
	}
	
	tracker_mem_destroy();
	tracker_service_destroy();
	
	logInfo("exit nomally.\n");
	log_destroy();
	
	return 0;
}

#if defined(DEBUG_FLAG)
/*
#if defined(OS_LINUX)
static void sigSegvHandler(int signum, siginfo_t *info, void *ptr)
{
	bSegmentFault = true;

	if (!bTerminateFlag)
	{
		set_timer(1, 1, sigAlarmHandler);

		bTerminateFlag = true;
		g_continue_flag = false;

		logCrit("file: "__FILE__", line: %d, " \
			"catch signal %d, program exiting...", \
			__LINE__, signum);
	
		signal_stack_trace_print(signum, info, ptr);
	}
}
#endif
*/

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
		"%s/logs/tracker_dump.log", g_fdfs_base_path);
	fdfs_dump_tracker_global_vars_to_file(filename);

	bDumpFlag = false;
}

#endif

static void sigQuitHandler(int sig)
{
	if (!bTerminateFlag)
	{
		set_timer(1, 1, sigAlarmHandler);

		bTerminateFlag = true;
		g_continue_flag = false;
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

static void sigAlarmHandler(int sig)
{
	TrackerServerInfo server;

	if (bAcceptEndFlag)
	{
		return;
	}

	logDebug("file: "__FILE__", line: %d, " \
		"signal server to quit...", __LINE__);

	if (*bind_addr != '\0')
	{
		strcpy(server.ip_addr, bind_addr);
	}
	else
	{
		strcpy(server.ip_addr, "127.0.0.1");
	}
	server.port = g_server_port;
	server.sock = -1;

	if (tracker_connect_server_ex(&server, 1) != 0)
	{
		return;
	}

	fdfs_quit(&server);
	tracker_disconnect_server(&server);

	logDebug("file: "__FILE__", line: %d, " \
		"signal server to quit done", __LINE__);
}

static void sigUsrHandler(int sig)
{
	logInfo("file: "__FILE__", line: %d, " \
		"catch signal %d, ignore it", __LINE__, sig);
}

