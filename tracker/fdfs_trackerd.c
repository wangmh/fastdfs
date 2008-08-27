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
#include "sockopt.h"
#include "tracker_types.h"
#include "tracker_mem.h"
#include "tracker_service.h"
#include "tracker_global.h"
#include "tracker_func.h"

bool bReloadFlag = false;

void sigQuitHandler(int sig);
void sigHupHandler(int sig);
void sigUsrHandler(int sig);

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
	if ((result=tracker_load_from_conf_file(conf_filename, \
			bind_addr, sizeof(bind_addr))) != 0)
	{
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
	
	daemon_init(false);
	umask(0);
	
	if ((result=init_pthread_lock( \
			&g_tracker_thread_lock)) != 0)
	{
		return result;
	}
	
	g_tracker_thread_count = 0;
	if ((result=init_pthread_attr(&thread_attr)) != 0)
	{
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
			if (!retrieveConfInfo())
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
				"accept failed, errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			continue;
		}
		
		if (pthread_mutex_lock(&g_tracker_thread_lock) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_lock fail, " \
				"errno: %d, error info:%s.", \
				__LINE__, errno, strerror(errno));
		}
		if (g_tracker_thread_count >= g_max_connections)
		{
			logError("file: "__FILE__", line: %d, " \
				"create thread failed, " \
				"current thread count %d exceed the limit %d", \
				__LINE__, g_tracker_thread_count + 1, g_max_connections);
			close(incomesock);
		}
		else
		{
			result = pthread_create(&tid, &thread_attr, \
				tracker_thread_entrance, (void*)incomesock);
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
				g_tracker_thread_count++;
			}
		}
		if (pthread_mutex_unlock(&g_tracker_thread_lock) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_unlock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, errno, strerror(errno));
		}
	}

	while (g_tracker_thread_count != 0)
	{
		sleep(1);
	}
	
	//gc_destroy();
	tracker_mem_destroy();

	pthread_attr_destroy(&thread_attr);
	pthread_mutex_destroy(&g_tracker_thread_lock);
	
	logInfo("exit nomally.\n");
	
	return 0;
}

void sigQuitHandler(int sig)
{
	g_continue_flag = false;
}

void sigHupHandler(int sig)
{
	bReloadFlag = true;
}

void sigUsrHandler(int sig)
{
	/*
	logInfo("current thread count=%d, " \
		"mo count=%d, success count=%d", g_tracker_thread_count, \nMoCount, nSuccMoCount);
	*/
}

