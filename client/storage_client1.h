/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#ifndef STORAGE_CLIENT1_H
#define STORAGE_CLIENT1_H

#include "tracker_types.h"
#include "storage_client.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
* upload file to storage server (by file name)
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*       store_path_index: the index of path on the storage server
*       local_filename: local filename to upload
*       file_ext_name: file ext name, not include dot(.), 
*                      if be NULL will abstract ext name from the local filename
*	meta_list: meta info array
*       meta_count: meta item count
*	file_id: return the new created file id (including group name and filename)
* return: 0 success, !=0 fail, return the error code
**/
int storage_upload_by_filename1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, const int store_path_index, \
		const char *local_filename, const char *file_ext_name, \
		const FDFSMetaData *meta_list, \
		const int meta_count, char *file_id);

/**
* upload file to storage server (by file buff)
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*       store_path_index: the index of path on the storage server
*       file_buff: file content/buff
*       file_size: file size (bytes)
*       file_ext_name: file ext name, not include dot(.), can be NULL
*	meta_list: meta info array
*       meta_count: meta item count
*	file_id: return the new created file id (including group name and filename)
* return: 0 success, !=0 fail, return the error code
**/
#define storage_upload_by_filebuff1(pTrackerServer, pStorageServer, \
		store_path_index, file_buff, \
		file_size, file_ext_name, meta_list, meta_count, file_id) \
	storage_do_upload_file1(pTrackerServer, pStorageServer, \
		store_path_index, false, file_buff, file_size, \
		file_ext_name, meta_list, meta_count, file_id)
int storage_do_upload_file1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const int store_path_index, const bool bFilename, \
		const char *file_buff, const int64_t file_size, \
		const char *file_ext_name, const FDFSMetaData *meta_list, \
		const int meta_count, char *file_id);

/**
* delete file from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	file_id: the file id to deleted (including group name and filename)
* return: 0 success, !=0 fail, return the error code
**/
int storage_delete_file1(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *file_id);

/**
* set metadata items to storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	file_id: the file id (including group name and filename)
*	meta_list: meta item array
*       meta_count: meta item count
*       op_flag:
*            # STORAGE_SET_METADATA_FLAG_OVERWRITE('O'): overwrite all old 
*				metadata items
*            # STORAGE_SET_METADATA_FLAG_MERGE ('M'): merge, insert when
*				the metadata item not exist, otherwise update it
* return: 0 success, !=0 fail, return the error code
**/
int storage_set_metadata1(TrackerServerInfo *pTrackerServer, \
			TrackerServerInfo *pStorageServer, \
			const char *file_id, \
			FDFSMetaData *meta_list, const int meta_count, \
			const char op_flag);

/**
* download file from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	file_id: the file id (including group name and filename)
*       file_buff: return file content/buff, must be freed
*       file_size: return file size (bytes)
* return: 0 success, !=0 fail, return the error code
**/
#define storage_download_file1(pTrackerServer, pStorageServer, file_id, \
			file_buff, file_size)  \
	storage_do_download_file1(pTrackerServer, pStorageServer, \
			FDFS_DOWNLOAD_TO_BUFF, file_id, \
			file_buff, NULL, file_size)

#define storage_download_file_to_buff1(pTrackerServer, pStorageServer, \
			file_id, file_buff, file_size)  \
	storage_do_download_file1(pTrackerServer, pStorageServer, \
			FDFS_DOWNLOAD_TO_BUFF, file_id, \
			file_buff, NULL, file_size)

int storage_do_download_file1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const int download_type, const char *file_id, \
		char **file_buff, void *arg, int64_t *file_size);

/**
* download file from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	file_id: the file id (including group name and filename)
*	local_filename: local filename to write
*       file_size: return file size (bytes)
* return: 0 success, !=0 fail, return the error code
**/
int storage_download_file_to_file1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *file_id, \
		const char *local_filename, int64_t *file_size);

/**
* get all metadata items from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	file_id: the file id (including group name and filename)
*	meta_list: return meta info array, must be freed
*       meta_count: return meta item count
* return: 0 success, !=0 fail, return the error code
**/
int storage_get_metadata1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer,  \
		const char *file_id, \
		FDFSMetaData **meta_list, int *meta_count);


/**
* download file from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	file_id: the file id (including group name and filename)
*	callback: callback function
*	arg: callback extra arguement
*       file_size: return file size (bytes)
* return: 0 success, !=0 fail, return the error code
**/
int storage_download_file_ex1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *file_id, \
		DownloadCallback callback, void *arg, int64_t *file_size);

/**
* query storage server to download file
* params:
*	pTrackerServer: tracker server
*	pStorageServer: return storage server
*	file_id: the file id (including group name and filename)
* return: 0 success, !=0 fail, return the error code
**/
int tracker_query_storage_fetch1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *file_id);
		
/**
* query storage server to update (delete file and set metadata)
* params:
*	pTrackerServer: tracker server
*	pStorageServer: return storage server
*	file_id: the file id (including group name and filename)
* return: 0 success, !=0 fail, return the error code
**/
int tracker_query_storage_update1(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *file_id);

#ifdef __cplusplus
}
#endif

#endif

