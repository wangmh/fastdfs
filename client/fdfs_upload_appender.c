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
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fdfs_client.h"
#include "logger.h"

int main(int argc, char *argv[])
{
	char *conf_filename;
	char *local_filename;
	char group_name[FDFS_GROUP_NAME_MAX_LEN + 1];
	TrackerServerInfo *pTrackerServer;
	int result;
	int store_path_index;
	TrackerServerInfo storageServer;
	char file_id[128];
	
	if (argc < 3)
	{
		printf("Usage: %s <config_filename> <local_filename>\n", argv[0]);
		return 1;
	}

	log_init();
	g_log_context.log_level = LOG_ERR;

	conf_filename = argv[1];
	if ((result=fdfs_client_init(conf_filename)) != 0)
	{
		return result;
	}

	pTrackerServer = tracker_get_connection();
	if (pTrackerServer == NULL)
	{
		fdfs_client_destroy();
		return errno != 0 ? errno : ECONNREFUSED;
	}


	if ((result=tracker_query_storage_store(pTrackerServer, \
	                &storageServer, &store_path_index)) != 0)
	{
		fdfs_client_destroy();
		fprintf(stderr, "tracker_query_storage fail, " \
			"error no: %d, error info: %s\n", \
			result, STRERROR(result));
		return result;
	}

	strcpy(group_name, "");
	local_filename = argv[2];
	result = storage_upload_appender_by_filename1(pTrackerServer, \
			&storageServer, store_path_index, \
			local_filename, NULL, \
			NULL, 0, group_name, file_id);
	if (result != 0)
	{
		fprintf(stderr, "upload file fail, " \
			"error no: %d, error info: %s\n", \
			result, STRERROR(result));

		if (storageServer.sock >= 0)
		{
			fdfs_quit(&storageServer);
		}
		tracker_disconnect_server(&storageServer);

		fdfs_quit(pTrackerServer);
		tracker_close_all_connections();
		fdfs_client_destroy();
		return result;
	}

	printf("%s\n", file_id);

	fdfs_quit(pTrackerServer);
	tracker_close_all_connections();
	fdfs_client_destroy();

	return 0;
}

