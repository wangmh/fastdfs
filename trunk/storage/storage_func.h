/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//storage_func.h

#ifndef _STORAGE_FUNC_H_
#define _STORAGE_FUNC_H_

#define STORAGE_DATA_DIR_FORMAT		"%02X"
#define STORAGE_META_FILE_EXT		"-m"

#ifdef __cplusplus
extern "C" {
#endif

typedef char * (*get_filename_func)(const void *pArg, \
			char *full_filename);

int storage_write_to_fd(int fd, get_filename_func filename_func, \
		const void *pArg, const char *buff, const int len);
int storage_load_from_conf_file(const char *filename, \
		char *bind_addr, const int addr_size);

int storage_open_storage_stat();
int storage_close_storage_stat();
int storage_write_to_stat_file();

int storage_check_and_make_data_dirs();
int storage_write_to_sync_ini_file();

#ifdef __cplusplus
}
#endif

#endif
