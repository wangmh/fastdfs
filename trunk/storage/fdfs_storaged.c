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
#include "fdfs_base64.h"

bool bReloadFlag = false;

static void sigQuitHandler(int sig);
static void sigHupHandler(int sig);
static void sigUsrHandler(int sig);
static int setRandSeed();

int main(int argc, char *argv[])
{
	char *conf_filename;
	char bind_addr[FDFS_IPADDR_SIZE];
	
	int result;
	int sock;
	pthread_t *tids;
	struct sigaction act;

	if (argc < 2)
	{
		printf("Usage: %s <config_file>\n", argv[0]);
		return 1;
	}

	conf_filename = argv[1];
	memset(bind_addr, 0, sizeof(bind_addr));
	if ((result=storage_load_from_conf_file(conf_filename, \
			bind_addr, sizeof(bind_addr))) != 0)
	{
		return result;
	}

	sock = socketServer(bind_addr, g_server_port, &result);
	if (sock < 0)
	{
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

	if ((result=storage_check_and_make_data_dirs()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"storage_check_and_make_data_dirs fail, " \
			"program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	if ((result=init_pthread_lock(&g_storage_thread_lock)) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	base64_init_ex(0, '.', '_', '-');
	if ((result=setRandSeed()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"setRandSeed fail, program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	if ((result=storage_open_storage_stat()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"storage_open_storage_stat fail, " \
			"program exit!", __LINE__);
		g_continue_flag = false;
		return result;
	}

	daemon_init(false);
	umask(0);
	
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
		sigaction(SIGQUIT, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}

	if ((result=tracker_report_thread_start()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"tracker_report_thread_start fail, " \
			"program exit!", __LINE__);
		g_continue_flag = false;
		storage_close_storage_stat();
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

	g_storage_thread_count = g_max_connections;
	if ((result=create_work_threads(&g_storage_thread_count, \
		storage_thread_entrance, (void *)sock, tids)) != 0)
	{
		free(tids);
		return result;
	}

	while (g_continue_flag)
	{
		sleep(1);
	}

	kill_work_threads(tids, g_max_connections);
	while (g_storage_thread_count != 0 || \
		g_tracker_reporter_count > 0 || \
		g_storage_sync_thread_count > 0)
	{
		sleep(1);
	}

	if ((result=tracker_report_destroy()) != 0)
	{
	}

	if (g_tracker_servers != NULL)
	{
		free(g_tracker_servers);
		g_tracker_servers = NULL;
	}

	pthread_mutex_destroy(&g_storage_thread_lock);
	
	storage_sync_destroy();
	storage_close_storage_stat();

	free(tids);

	logInfo("exit nomally.\n");
	
	return 0;
}

static void sigQuitHandler(int sig)
{
	if (g_continue_flag)
	{
		g_continue_flag = false;
		logCrit("file: "__FILE__", line: %d, " \
			"catch signal %d, program exiting...", \
			__LINE__, sig);
	}
}

static void sigHupHandler(int sig)
{
	bReloadFlag = true;
}

static void sigUsrHandler(int sig)
{
	/*
	logInfo("current thread count=%d, " \
		"mo count=%d, success count=%d", g_storage_thread_count, \
		nMoCount, nSuccMoCount);
	*/
}

static int setRandSeed()
{
	struct timeval tv;

	if (gettimeofday(&tv, NULL) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			 "call gettimeofday fail, " \
			 "errno=%d, error info: %s", \
			 __LINE__, errno, strerror(errno));
		return errno != 0 ? errno : EPERM;
	}

	srand(tv.tv_sec ^ tv.tv_usec);
	return 0;
}

