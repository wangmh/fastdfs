#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "dfs_func.h"
#include "fdfs_client.h"

int dfs_init(const int proccess_index)
{
	int result;
	char *conf_filename;

#ifdef DEBUG  //for debug
	conf_filename = "/home/y/FastDFS/conf/storage.conf";
#else
	conf_filename = "/home/y/FastDFS/conf/storage.conf";
#endif

	if ((result=fdfs_client_init(conf_filename)) != 0)
	{
		return result;
	}


	return 0;
}

void dfs_destroy()
{
	fdfs_client_destroy();
}

static int downloadFileCallback(void *arg, const int64_t file_size, const char *data, \
                const int current_size)
{
	return 0;
}

int upload_file(const char *file_buff, const int file_size, char *file_id, char *storage_ip)
{
	int result;
	TrackerServerInfo *pTrackerServer;
	TrackerServerInfo storageServer;
	int store_path_index;

	pTrackerServer = tracker_get_connection();
	if (pTrackerServer == NULL)
	{
		return errno != 0 ? errno : ECONNREFUSED;
	}

	if ((result=tracker_query_storage_store(pTrackerServer, &storageServer,
			 &store_path_index)) != 0)
	{
		fdfs_quit(pTrackerServer);
		tracker_disconnect_server(pTrackerServer);
		return result;
	}

	if ((result=tracker_connect_server(&storageServer)) != 0)
	{
		fdfs_quit(pTrackerServer);
		tracker_disconnect_server(pTrackerServer);
		return result;
	}

	strcpy(storage_ip, storageServer.ip_addr);
	result = storage_upload_by_filebuff1(pTrackerServer, &storageServer, 
		store_path_index, file_buff, file_size, NULL, NULL, 0, "", file_id);

	fdfs_quit(pTrackerServer);
	tracker_disconnect_server(pTrackerServer);

	fdfs_quit(&storageServer);
	tracker_disconnect_server(&storageServer);

	return result;
}

int download_file(const char *file_id, int *file_size, char *storage_ip)
{
	int result;
	TrackerServerInfo *pTrackerServer;
	TrackerServerInfo storageServer;
	int64_t file_bytes;

	pTrackerServer = tracker_get_connection();
	if (pTrackerServer == NULL)
	{
		return errno != 0 ? errno : ECONNREFUSED;
	}

	if ((result=tracker_query_storage_fetch1(pTrackerServer, &storageServer, file_id)) != 0)
	{
		fdfs_quit(pTrackerServer);
		tracker_disconnect_server(pTrackerServer);
		return result;
	}

	if ((result=tracker_connect_server(&storageServer)) != 0)
	{
		fdfs_quit(pTrackerServer);
		tracker_disconnect_server(pTrackerServer);
		return result;
	}

	strcpy(storage_ip, storageServer.ip_addr);
	result = storage_download_file_ex1(pTrackerServer, &storageServer, file_id, 0, 0, downloadFileCallback, NULL, &file_bytes);
	*file_size = file_bytes;

	fdfs_quit(pTrackerServer);
	tracker_disconnect_server(pTrackerServer);

	fdfs_quit(&storageServer);
	tracker_disconnect_server(&storageServer);

	return result;
}

int delete_file(const char *file_id, char *storage_ip)
{
	int result;
	TrackerServerInfo *pTrackerServer;
	TrackerServerInfo storageServer;

	pTrackerServer = tracker_get_connection();
	if (pTrackerServer == NULL)
	{
		return errno != 0 ? errno : ECONNREFUSED;
	}

	if ((result=tracker_query_storage_update1(pTrackerServer, &storageServer, file_id)) != 0)
	{
		fdfs_quit(pTrackerServer);
		tracker_disconnect_server(pTrackerServer);
		return result;
	}

	if ((result=tracker_connect_server(&storageServer)) != 0)
	{
		fdfs_quit(pTrackerServer);
		tracker_disconnect_server(pTrackerServer);
		return result;
	}

	strcpy(storage_ip, storageServer.ip_addr);
	result = storage_delete_file1(pTrackerServer, &storageServer, file_id);

	fdfs_quit(pTrackerServer);
	tracker_disconnect_server(pTrackerServer);

	fdfs_quit(&storageServer);
	tracker_disconnect_server(&storageServer);

	return result;
}

