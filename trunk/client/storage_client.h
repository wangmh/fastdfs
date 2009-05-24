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

#define FDFS_DOWNLOAD_TO_BUFF   	1
#define FDFS_DOWNLOAD_TO_FILE   	2
#define FDFS_DOWNLOAD_TO_CALLBACK   	3

#define FDFS_FILE_ID_SEPERATOR		'/'

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
*	group_name: if not empty, specify the group name. 
	 	    return the group name to store the file
*	remote_filename: return the new created filename
* return: 0 success, !=0 fail, return the error code
**/
int storage_upload_by_filename(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, const int store_path_index, \
		const char *local_filename, const char *file_ext_name, \
		const FDFSMetaData *meta_list, const int meta_count, \
		char *group_name, char *remote_filename);

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
*	group_name: if not empty, specify the group name. 
		    return the group name to store the file
*	remote_filename: return the new created filename
* return: 0 success, !=0 fail, return the error code
**/
#define storage_upload_by_filebuff(pTrackerServer, pStorageServer, \
		store_path_index, file_buff, \
		file_size, file_ext_name, meta_list, meta_count, \
		group_name, remote_filename) \
	storage_do_upload_file(pTrackerServer, pStorageServer, \
		store_path_index, false, file_buff, file_size, \
		file_ext_name, meta_list, meta_count, \
		group_name, remote_filename)

int storage_do_upload_file(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, const int store_path_index, \
		const bool bFilename, \
		const char *file_buff, const int64_t file_size, \
		const char *file_ext_name, const FDFSMetaData *meta_list, \
		const int meta_count, char *group_name, char *remote_filename);

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
*	remote_filename: filename on storage server
*       file_buff: return file content/buff, must be freed
*       file_size: return file size (bytes)
* return: 0 success, !=0 fail, return the error code
**/
#define storage_download_file(pTrackerServer, pStorageServer, group_name, \
			remote_filename, file_buff, file_size)  \
	storage_do_download_file_ex(pTrackerServer, pStorageServer, \
			FDFS_DOWNLOAD_TO_BUFF, group_name, remote_filename, \
			0, 0, file_buff, NULL, file_size)

#define storage_download_file_to_buff(pTrackerServer, pStorageServer, \
			group_name, remote_filename, file_buff, file_size)  \
	storage_do_download_file_ex(pTrackerServer, pStorageServer, \
			FDFS_DOWNLOAD_TO_BUFF, group_name, remote_filename, \
			0, 0, file_buff, NULL, file_size)

#define storage_do_download_file(pTrackerServer, pStorageServer, \
		download_type, group_name, remote_filename, \
		file_buff, arg, file_size) \
	storage_do_download_file_ex(pTrackerServer, pStorageServer, \
		download_type, group_name, remote_filename, \
		0, 0, file_buff, arg, file_size);

/**
* download file from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*       download_type: FDFS_DOWNLOAD_TO_BUFF or FDFS_DOWNLOAD_TO_FILE 
*                      or FDFS_DOWNLOAD_TO_CALLBACK
*	group_name: the group name of storage server
*	remote_filename: filename on storage server
*       file_offset: the start offset to download
*       download_bytes: download bytes, 0 means from start offset to the file end
*       file_buff: return file content/buff, must be freed
*       arg: additional argument for callback(valid only when download_tyee
*                       is FDFS_DOWNLOAD_TO_CALLBACK), can be NULL
*       file_size: return file size (bytes)
* return: 0 success, !=0 fail, return the error code
**/
int storage_do_download_file_ex(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const int download_type, \
		const char *group_name, const char *remote_filename, \
		const int64_t file_offset, const int64_t download_bytes, \
		char **file_buff, void *arg, int64_t *file_size);

/**
* download file from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	group_name: the group name of storage server
*	remote_filename: filename on storage server
*	local_filename: local filename to write
*       file_size: return file size (bytes)
* return: 0 success, !=0 fail, return the error code
**/
int storage_download_file_to_file(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *group_name, const char *remote_filename, \
		const char *local_filename, int64_t *file_size);

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


/**
* Download file callback function prototype
* params:
*	arg: callback extra arguement
*       file_size: file size
*       data: temp buff, should not keep persistently
*	current_size: current data size
* return: 0 success, !=0 fail, should return the error code
**/
typedef int (*DownloadCallback) (void *arg, const int64_t file_size, \
		const char *data, const int current_size);

/**
* download file from storage server
* params:
*       pTrackerServer: tracker server
*       pStorageServer: storage server
*	group_name: the group name of storage server
*	remote_filename: filename on storage server
*       file_offset: the start offset to download
*       download_bytes: download bytes, 0 means from start offset to the file end
*	callback: callback function
*	arg: callback extra arguement
*       file_size: return file size (bytes)
* return: 0 success, !=0 fail, return the error code
**/
int storage_download_file_ex(TrackerServerInfo *pTrackerServer, \
		TrackerServerInfo *pStorageServer, \
		const char *group_name, const char *remote_filename, \
		const int64_t file_offset, const int64_t download_bytes, \
		DownloadCallback callback, void *arg, int64_t *file_size);

#ifdef __cplusplus
}
#endif

#endif

