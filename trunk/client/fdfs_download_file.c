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
	TrackerServerInfo *pTrackerServer;
	int result;
	char file_id[128];
	int64_t file_size;
	
	if (argc < 3)
	{
		printf("Usage: %s <config_file> <remote file id> " \
			"[local filename]\n", argv[0]);
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

	snprintf(file_id, sizeof(file_id), "%s", argv[2]);

	if (argc >= 4)
	{
		local_filename = argv[3];
	}
	else
	{
		local_filename = strrchr(file_id, '/');
		if (local_filename != NULL)
		{
			local_filename++;  //skip /
		}
		else
		{
			local_filename = file_id;
		}
	}

	result = storage_download_file_to_file1( \
			pTrackerServer, NULL, \
			file_id, local_filename, &file_size);
	if (result != 0)
	{
		printf("download file fail, " \
			"error no: %d, error info: %s\n", \
			result, STRERROR(result));
	}

	fdfs_quit(pTrackerServer);
	tracker_close_all_connections();
	fdfs_client_destroy();

	return 0;
}

