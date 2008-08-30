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

void sigQuitHandler(int sig);
void sigHupHandler(int sig);
void sigUsrHandler(int sig);
int setRandSeed();

int main(int argc, char *argv[])
{
	char *conf_filename;
	char bind_addr[FDFS_IPADDR_SIZE];
	pthread_attr_t thread_attr;
	int incomesock;
	
	int result;
	int sock;
	pthread_t tid;

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
	
	g_storage_thread_count = 0;
	if ((result=tracker_report_thread_start()) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"tracker_report_thread_start fail, " \
			"program exit!", __LINE__);
		g_continue_flag = false;
		storage_close_storage_stat();
		return result;
	}

	if ((result=init_pthread_attr(&thread_attr)) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"init_pthread_attr fail, program exit!", __LINE__);
		g_continue_flag = false;
		storage_close_storage_stat();
		return result;
	}

	signal(SIGHUP, sigHupHandler);
	signal(SIGUSR1, sigUsrHandler);
	signal(SIGUSR2, sigUsrHandler);
	signal(SIGINT, sigQuitHandler);
	signal(SIGTERM, sigQuitHandler);
	signal(SIGQUIT, sigQuitHandler);
	signal(SIGPIPE, SIG_IGN);
	
	while (g_continue_flag)
	{
		/*
		if (bReloadFlag)
		{			
			if (!storage_load_from_conf_file())
			{
				break;
			}
			
			bReloadFlag = false;
		}
		*/
	
		incomesock = nbaccept(sock, 1 * 60, &result);
		if(incomesock < 0) //error
		{
			if (result == ETIMEDOUT || result == EINTR || \
				result == EAGAIN)
			{
				continue;
			}
			
			if(result == EBADF)
			{
				logError("file: "__FILE__", line: %d, " \
					"accept failed, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
				break;
			}
			
			logError("file: "__FILE__", line: %d, " \
				"accept failed, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			continue;
		}
		
		if (pthread_mutex_lock(&g_storage_thread_lock) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_lock fail, " \
				"errno: %d, error info:%s.", \
				__LINE__, errno, strerror(errno));
		}
		if (g_storage_thread_count >= g_max_connections)
		{
			logError("file: "__FILE__", line: %d, " \
				"create thread failed, " \
				"current thread count %d exceed the limit %d", \
				__LINE__, \
				g_storage_thread_count + 1, g_max_connections);
			close(incomesock);
		}
		else
		{
			result = pthread_create(&tid, &thread_attr, \
				storage_thread_entrance, (void*)incomesock);
			if(result != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"create thread failed, " \
					"errno: %d, error info: %s", \
					__LINE__, errno, strerror(errno));
				close(incomesock);
			}
			else
			{
				g_storage_thread_count++;
			}
		}
		if (pthread_mutex_unlock(&g_storage_thread_lock) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_unlock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, errno, strerror(errno));
		}
	}

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

	pthread_attr_destroy(&thread_attr);
	pthread_mutex_destroy(&g_storage_thread_lock);
	
	storage_sync_destroy();
	storage_close_storage_stat();

	logInfo("exit nomally.\n");
	
	return 0;
}

void sigQuitHandler(int sig)
{
	if (g_continue_flag)
	{
		g_continue_flag = false;
		logCrit("file: "__FILE__", line: %d, " \
			"catch signal %d, program exiting...", \
			__LINE__, sig);
	}
}

void sigHupHandler(int sig)
{
	bReloadFlag = true;
}

void sigUsrHandler(int sig)
{
	/*
	logInfo("current thread count=%d, " \
		"mo count=%d, success count=%d", g_storage_thread_count, \
		nMoCount, nSuccMoCount);
	*/
}

int setRandSeed()
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

