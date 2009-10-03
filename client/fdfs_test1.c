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
#include "fdfs_global.h"
#include "fdfs_base64.h"
#include "fdfs_http_shared.h"
#include "sockopt.h"

int writeToFileCallback(void *arg, const int64_t file_size, const char *data, \
                const int current_size)
{
	if (arg == NULL)
	{
		return EINVAL;
	}

	if (fwrite(data, current_size, 1, (FILE *)arg) != 1)
	{
		return errno != 0 ? errno : EIO;
	}

	return 0;
}

int uploadFileCallback(void *arg, const int64_t file_size, int sock)
{
	char *filename;
	if (arg == NULL)
	{
		return EINVAL;
	}

	filename = (char *)arg;
	return tcpsendfile(sock, filename, file_size, g_network_timeout);
}

int main(int argc, char *argv[])
{
	char *conf_filename;
	char *local_filename;
	char *remote_filename;
	TrackerServerInfo *pTrackerServer;
	int result;
	TrackerServerInfo storageServer;
	char group_name[FDFS_GROUP_NAME_MAX_LEN];
	FDFSMetaData meta_list[32];
	int meta_count;
	int i;
	FDFSMetaData *pMetaList;
	char buff[13];
	char token[32 + 1];
	char file_id[128];
	char file_url[256];
	int len;
	int url_len;
	time_t ts;
        char *file_buff;
	int64_t file_size;
	char *operation;
	char *meta_buff;
	int store_path_index;
	struct base64_context context;

	base64_init_ex(&context, 0, '-', '_', '.');
	printf("This is FastDFS client test program v%d.%d\n" \
"\nCopyright (C) 2008, Happy Fish / YuQing\n" \
"\nFastDFS may be copied only under the terms of the GNU General\n" \
"Public License V3, which may be found in the FastDFS source kit.\n" \
"Please visit the FastDFS Home Page http://www.csource.org/ \n" \
"for more detail.\n\n" \
, g_version.major, g_version.minor);

	if (argc < 3)
	{
		printf("Usage: %s <config_file> <operation>\n" \
			"\toperation: upload, download, getmeta, setmeta, " \
			"delete and query_servers\n", argv[0]);
		return 1;
	}

	conf_filename = argv[1];
	operation = argv[2];
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

	local_filename = NULL;
	if (strcmp(operation, "upload") == 0)
	{
		int upload_type;
		char *file_ext_name;

		if (argc < 4)
		{
			printf("Usage: %s <config_file> upload " \
				"<local_filename> [FILE | BUFF | CALLBACK] \n",\
				argv[0]);
			fdfs_client_destroy();
			return EINVAL;
		}

		local_filename = argv[3];
		if (argc == 4)
		{
			upload_type = FDFS_UPLOAD_BY_FILE;
		}
		else
		{
			if (strcmp(argv[4], "BUFF") == 0)
			{
				upload_type = FDFS_UPLOAD_BY_BUFF;
			}
			else if (strcmp(argv[4], "CALLBACK") == 0)
			{
				upload_type = FDFS_UPLOAD_BY_CALLBACK;
			}
			else
			{
				upload_type = FDFS_UPLOAD_BY_FILE;
			}
		}

		if ((result=tracker_query_storage_store(pTrackerServer, \
		                &storageServer, &store_path_index)) != 0)
		{
			fdfs_client_destroy();
			printf("tracker_query_storage fail, " \
				"error no: %d, error info: %s\n", \
				result, strerror(result));
			return result;
		}

		printf("group_name=%s, ip_addr=%s, port=%d\n", \
			storageServer.group_name, \
			storageServer.ip_addr, \
			storageServer.port);

		if ((result=tracker_connect_server(&storageServer)) != 0)
		{
			fdfs_client_destroy();
			return result;
		}

		memset(&meta_list, 0, sizeof(meta_list));
		meta_count = 0;
		strcpy(meta_list[meta_count].name, "ext_name");
		strcpy(meta_list[meta_count].value, "jpg");
		meta_count++;
		strcpy(meta_list[meta_count].name, "width");
		strcpy(meta_list[meta_count].value, "160");
		meta_count++;
		strcpy(meta_list[meta_count].name, "height");
		strcpy(meta_list[meta_count].value, "80");
		meta_count++;
		strcpy(meta_list[meta_count].name, "file_size");
		strcpy(meta_list[meta_count].value, "115120");
		meta_count++;

		file_ext_name = strrchr(local_filename, '.');
		if (file_ext_name != NULL)
		{
			file_ext_name++;
		}
		strcpy(group_name, "");

		if (upload_type == FDFS_UPLOAD_BY_FILE)
		{
			printf("storage_upload_by_filename\n");
			result = storage_upload_by_filename1(pTrackerServer, \
				&storageServer, store_path_index, \
				local_filename, file_ext_name, \
				meta_list, meta_count, \
				group_name, file_id);
		}
		else if (upload_type == FDFS_UPLOAD_BY_BUFF)
		{
			char *file_content;
			printf("storage_upload_by_filebuff\n");
			if ((result=getFileContent(local_filename, \
					&file_content, &file_size)) == 0)
			{
			result = storage_upload_by_filebuff1(pTrackerServer, \
				&storageServer, store_path_index, \
				file_content, file_size, file_ext_name, \
				meta_list, meta_count, \
				group_name, file_id);
			free(file_content);
			}
		}
		else
		{
			struct stat stat_buf;

			printf("storage_upload_by_callback\n");
			if (stat(local_filename, &stat_buf) == 0 && \
				S_ISREG(stat_buf.st_mode))
			{
			file_size = stat_buf.st_size;
			result = storage_upload_by_callback1(pTrackerServer, \
				&storageServer, store_path_index, \
				uploadFileCallback, local_filename, \
				file_size, file_ext_name, \
				meta_list, meta_count, \
				group_name, file_id);
			}
		}

		if (result != 0)
		{
			printf("upload file fail, " \
				"error no: %d, error info: %s\n", \
				result, strerror(result));
			fdfs_quit(&storageServer);
			tracker_disconnect_server(&storageServer);
			fdfs_client_destroy();
			return result;
		}

		memset(buff, 0, sizeof(buff));
		remote_filename = strchr(file_id, FDFS_FILE_ID_SEPERATOR);
		if (remote_filename != NULL)
		{
			url_len = sprintf(file_url, "http://%s:%d/%s", \
					pTrackerServer->ip_addr, \
					g_tracker_server_http_port, file_id);
			if (g_anti_steal_token)
			{
				ts = time(NULL);
				fdfs_http_gen_token(&g_anti_steal_secret_key, \
					file_id, ts, token);
				sprintf(file_url + url_len, "?token=%s&ts=%d", \
					token, (int)ts);
			}

			remote_filename++;
			base64_decode_auto(&context, remote_filename + \
				FDFS_FILE_PATH_LEN, \
				strlen(remote_filename) - FDFS_FILE_PATH_LEN \
				- (FDFS_FILE_EXT_NAME_MAX_LEN + 1), buff, &len);
			printf("file_id=%s\n", file_id);
			printf("file timestamp=%d\n", \
				buff2int(buff + sizeof(int)));
			printf("file size="INT64_PRINTF_FORMAT"\n", \
				buff2long(buff+sizeof(int)*2));
			printf("file url: %s\n", file_url);
		}
	}
	else if (strcmp(operation, "download") == 0 || 
		strcmp(operation, "getmeta") == 0 ||
		strcmp(operation, "setmeta") == 0 ||
		strcmp(operation, "query_servers") == 0 ||
		strcmp(operation, "delete") == 0)
	{
		if (argc < 4)
		{
			printf("Usage: %s <config_file> %s " \
				"<file_id>\n", \
				argv[0], operation);
			fdfs_client_destroy();
			return EINVAL;
		}

		snprintf(file_id, sizeof(file_id), "%s", argv[3]);
		if (strcmp(operation, "query_servers") == 0)
		{
			TrackerServerInfo storageServers[FDFS_MAX_SERVERS_EACH_GROUP];
			int server_count;

			result = tracker_query_storage_list1(pTrackerServer, \
                		storageServers, FDFS_MAX_SERVERS_EACH_GROUP, \
                		&server_count, file_id);

			if (result != 0)
			{
				printf("tracker_query_storage_list1 fail, "\
					"group_name=%s, filename=%s, " \
					"error no: %d, error info: %s\n", \
					group_name, remote_filename, \
					result, strerror(result));
			}
			else
			{
				printf("server list (%d):\n", server_count);
				for (i=0; i<server_count; i++)
				{
					printf("\t%s:%d\n", \
						storageServers[i].ip_addr, \
						storageServers[i].port);
				}
				printf("\n");
			}

			fdfs_quit(pTrackerServer);
			fdfs_client_destroy();
			return result;
		}

		if ((result=tracker_query_storage_fetch1(pTrackerServer, \
       	       		&storageServer, file_id)) != 0)
		{
			fdfs_client_destroy();
			printf("tracker_query_storage_fetch fail, " \
				"file_id=%s, " \
				"error no: %d, error info: %s\n", \
				file_id, result, strerror(result));
			return result;
		}

		printf("storage=%s:%d\n", storageServer.ip_addr, \
			storageServer.port);

		if ((result=tracker_connect_server(&storageServer)) != 0)
		{
			fdfs_client_destroy();
			return result;
		}

		if (strcmp(operation, "download") == 0)
		{
			if (argc >= 5)
			{
				local_filename = argv[4];
				if (strcmp(local_filename, "CALLBACK") == 0)
				{
				FILE *fp;
				fp = fopen(local_filename, "wb");
				if (fp == NULL)
				{
					result = errno != 0 ? errno : EPERM;
					printf("open file \"%s\" fail, " \
						"errno: %d, error info: %s", \
						local_filename, result, \
						strerror(result));
				}
				else
				{
				result = storage_download_file_ex1( \
					pTrackerServer, &storageServer, \
					file_id, 0, 0, \
					writeToFileCallback, fp, &file_size);
				fclose(fp);
				}
				}
				else
				{
				result = storage_download_file_to_file1( \
					pTrackerServer, &storageServer, \
					file_id, \
					local_filename, &file_size);
				}
			}
			else
			{
				file_buff = NULL;
				if ((result=storage_download_file_to_buff1( \
					pTrackerServer, &storageServer, \
					file_id, \
					&file_buff, &file_size)) == 0)
				{
					local_filename = strrchr( \
							file_id, '/');
					if (local_filename != NULL)
					{
						local_filename++;  //skip /
					}
					else
					{
						local_filename=file_id;
					}

					result = writeToFile(local_filename, \
						file_buff, file_size);

					free(file_buff);
				}
			}

			if (result == 0)
			{
				printf("download file success, " \
					"file size="INT64_PRINTF_FORMAT", file save to %s\n", \
					 file_size, local_filename);
			}
			else
			{
				printf("download file fail, " \
					"error no: %d, error info: %s\n", \
					result, strerror(result));
			}
		}
		else if (strcmp(operation, "getmeta") == 0)
		{
			if ((result=storage_get_metadata1(pTrackerServer, \
				NULL, file_id, \
				&pMetaList, &meta_count)) == 0)
			{
				printf("get meta data success, " \
					"meta count=%d\n", meta_count);
				for (i=0; i<meta_count; i++)
				{
					printf("%s=%s\n", \
						pMetaList[i].name, \
						pMetaList[i].value);
				}

				free(pMetaList);
			}
			else
			{
				printf("getmeta fail, " \
					"error no: %d, error info: %s\n", \
					result, strerror(result));
			}
		}
		else if (strcmp(operation, "setmeta") == 0)
		{
			if (argc < 6)
			{
				printf("Usage: %s <config_file> %s " \
					"<file_id> " \
					"<op_flag> <metadata_list>\n" \
					"\top_flag: %c for overwrite, " \
					"%c for merge\n" \
					"\tmetadata_list: name1=value1," \
					"name2=value2,...\n", \
					argv[0], operation, \
					STORAGE_SET_METADATA_FLAG_OVERWRITE, \
					STORAGE_SET_METADATA_FLAG_MERGE);
				fdfs_client_destroy();
				return EINVAL;
			}

			meta_buff = strdup(argv[5]);
			if (meta_buff == NULL)
			{
				printf("Out of memory!\n");
				return ENOMEM;
			}

			pMetaList = fdfs_split_metadata_ex(meta_buff, \
					',', '=', &meta_count, &result);
			if (pMetaList == NULL)
			{
				printf("Out of memory!\n");
				free(meta_buff);
				return ENOMEM;
			}

			if ((result=storage_set_metadata1(pTrackerServer, \
				NULL, file_id, \
				pMetaList, meta_count, *argv[4])) == 0)
			{
				printf("set meta data success\n");
			}
			else
			{
				printf("setmeta fail, " \
					"error no: %d, error info: %s\n", \
					result, strerror(result));
			}

			free(meta_buff);
			free(pMetaList);
		}
		else if(strcmp(operation, "delete") == 0)
		{
			if ((result=storage_delete_file1(pTrackerServer, \
			NULL, file_id)) == 0)
			{
				printf("delete file success\n");
			}
			else
			{
				printf("delete file fail, " \
					"error no: %d, error info: %s\n", \
					result, strerror(result));
			}
		}
	}
	else
	{
		fdfs_client_destroy();
		printf("invalid operation: %s\n", operation);
		return EINVAL;
	}

	fdfs_quit(&storageServer);
	tracker_disconnect_server(&storageServer);

	fdfs_quit(pTrackerServer);

	tracker_close_all_connections();
	fdfs_client_destroy();

	return result;
}

