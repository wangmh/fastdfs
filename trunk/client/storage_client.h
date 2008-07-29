/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#ifndef STORAGE_CLIENT_H
#define STORAGE_CLIENT_H

#include "tracker_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
* upload file to storage server (by file name)
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*       local_filename: local filename to upload
*	meta_list: meta info array
*       meta_count: meta item count
*	group_name: return the group name to store the file
*	remote_filename: return the new created filename
* return: 0 success, !=0 fail, return the error code
**/
int storage_upload_by_filename(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *local_filename, \
			const FDFSMetaData *meta_list, \
			const int meta_count, \
			char *group_name, \
			char *remote_filename);

/**
* upload file to storage server (by file buff)
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*       file_buff: file content/buff
*       file_size: file size (bytes)
*	meta_list: meta info array
*       meta_count: meta item count
*	group_name: return the group name to store the file
*	remote_filename: return the new created filename
* return: 0 success, !=0 fail, return the error code
**/
int storage_upload_by_filebuff(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *file_buff, const int file_size, \
			const FDFSMetaData *meta_list, \
			const int meta_count, \
			char *group_name, \
			char *remote_filename);

/**
* delete file from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	group_name: the group name of storage server
*	filename: filename on storage server
* return: 0 success, !=0 fail, return the error code
**/
int storage_delete_file(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *group_name, const char *filename);

/**
* set metadata items to storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	group_name: the group name of storage server
*	filename: filename on storage server
*	meta_list: meta item array
*       meta_count: meta item count
*       op_flag:
*            # STORAGE_SET_METADATA_FLAG_OVERWRITE('O'): overwrite all old 
*				metadata items
*            # STORAGE_SET_METADATA_FLAG_MERGE ('M'): merge, insert when
*				the metadata item not exist, otherwise update it
* return: 0 success, !=0 fail, return the error code
**/
int storage_set_metadata(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *group_name, const char *filename, \
			FDFSMetaData *meta_list, const int meta_count, \
			const char op_flag);

/**
* download file from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	group_name: the group name of storage server
*	filename: filename on storage server
*       file_buff: return file content/buff, must be freed
*       file_size: return file size (bytes)
* return: 0 success, !=0 fail, return the error code
**/
int storage_download_file(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *group_name, const char *filename, \
			char **file_buff, int *file_size);

/**
* get all metadata items from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	group_name: the group name of storage server
*	filename: filename on storage server
*	meta_list: return meta info array, must be freed
*       meta_count: return meta item count
* return: 0 success, !=0 fail, return the error code
**/
int storage_get_metadata(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer,  \
			const char *group_name, const char *filename, \
			FDFSMetaData **meta_list, \
			int *meta_count);

#ifdef __cplusplus
}
#endif

#endif

