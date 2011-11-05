/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//trunk_shared.h

#ifndef _TRUNK_SHARED_H_
#define _TRUNK_SHARED_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include "base64.h"
#include "common_define.h"
#include "ini_file_reader.h"
#include "fdfs_global.h"
#include "tracker_types.h"

#define FDFS_TRUNK_STATUS_FREE  0
#define FDFS_TRUNK_STATUS_HOLD  1

#define FDFS_TRUNK_FILE_TYPE_NONE     '\0'
#define FDFS_TRUNK_FILE_TYPE_REGULAR  'F'
#define FDFS_TRUNK_FILE_TYPE_LINK     'L'

#define FDFS_TRUNK_FILE_INFO_LEN  16

#define FDFS_TRUNK_FILE_FILE_TYPE_OFFSET	0
#define FDFS_TRUNK_FILE_ALLOC_SIZE_OFFSET	1
#define FDFS_TRUNK_FILE_FILE_SIZE_OFFSET	5
#define FDFS_TRUNK_FILE_FILE_CRC32_OFFSET	9
#define FDFS_TRUNK_FILE_FILE_MTIME_OFFSET  	13
#define FDFS_TRUNK_FILE_FILE_EXT_NAME_OFFSET	17
#define FDFS_TRUNK_FILE_HEADER_SIZE	(17 + FDFS_FILE_EXT_NAME_MAX_LEN + 1)

#define FDFS_TRUNK_FILENAME_LENGTH (FDFS_TRUE_FILE_PATH_LEN + \
		FDFS_FILENAME_BASE64_LENGTH + FDFS_TRUNK_FILE_INFO_LEN + \
		1 + FDFS_FILE_EXT_NAME_MAX_LEN)
#define TRUNK_CALC_SIZE(file_size) (FDFS_TRUNK_FILE_HEADER_SIZE + file_size)
#define TRUNK_FILE_START_OFFSET(trunkInfo) \
		(FDFS_TRUNK_FILE_HEADER_SIZE + trunkInfo.file.offset)

#define IS_TRUNK_FILE_BY_ID(trunkInfo) (trunkInfo.file.id > 0)

#define TRUNK_GET_FILENAME(file_id, filename) \
	sprintf(filename, "%06d", file_id)

#ifdef __cplusplus
extern "C" {
#endif

extern char **g_fdfs_store_paths; //file store paths
extern int g_fdfs_path_count;   //store path count
extern struct base64_context g_fdfs_base64_context;   //base64 context

typedef int (*stat_func)(const char *filename, struct stat *buf);

typedef struct tagFDFSTrunkHeader {
	char file_type;
	char formatted_ext_name[FDFS_FILE_EXT_NAME_MAX_LEN + 2];
	int alloc_size;
	int file_size;
	int crc32;
	int mtime;
} FDFSTrunkHeader;

typedef struct tagFDFSTrunkPathInfo {
	unsigned char store_path_index;   //store which path as Mxx
	unsigned char sub_path_high;      //high sub dir index, front part of HH/HH
	unsigned char sub_path_low;       //low sub dir index, tail part of HH/HH
} FDFSTrunkPathInfo;

typedef struct tagFDFSTrunkFileInfo {
	int id;      //trunk file id
	int offset;  //file offset
	int size;    //space size
} FDFSTrunkFileInfo;

typedef struct tagFDFSTrunkFullInfo {
	char status;  //normal or hold
	FDFSTrunkPathInfo path;
	FDFSTrunkFileInfo file;
} FDFSTrunkFullInfo;

int storage_load_paths_from_conf_file(IniContext *pItemContext);
void trunk_shared_init();

int storage_split_filename(const char *logic_filename, \
		int *filename_len, char *true_filename, char **ppStorePath);
int storage_split_filename_ex(const char *logic_filename, \
		int *filename_len, char *true_filename, int *store_path_index);

void trunk_file_info_encode(const FDFSTrunkFileInfo *pTrunkFile, char *str);
void trunk_file_info_decode(const char *str, FDFSTrunkFileInfo *pTrunkFile);

char *trunk_info_dump(const FDFSTrunkFullInfo *pTrunkInfo, char *buff, \
				const int buff_size);

char *trunk_header_dump(const FDFSTrunkHeader *pTrunkHeader, char *buff, \
				const int buff_size);

char *trunk_get_full_filename(const FDFSTrunkFullInfo *pTrunkInfo, \
		char *full_filename, const int buff_size);

void trunk_pack_header(const FDFSTrunkHeader *pTrunkHeader, char *buff);
void trunk_unpack_header(const char *buff, FDFSTrunkHeader *pTrunkHeader);

int trunk_file_get_content(const FDFSTrunkFullInfo *pTrunkInfo, \
		const int file_size, int *pfd, \
		char *buff, const int buff_size);

#define trunk_file_stat(store_path_index, true_filename, filename_len, \
			pStat, pTrunkInfo, pTrunkHeader) \
	trunk_file_stat_func(store_path_index, true_filename, filename_len, \
			stat, pStat, pTrunkInfo, pTrunkHeader, NULL)

#define trunk_file_lstat(store_path_index, true_filename, filename_len, \
			pStat, pTrunkInfo, pTrunkHeader) \
	trunk_file_do_lstat_func(store_path_index, true_filename, filename_len, \
			lstat, pStat, pTrunkInfo, pTrunkHeader, NULL)

#define trunk_file_stat_ex(store_path_index, true_filename, filename_len, \
			pStat, pTrunkInfo, pTrunkHeader, pfd) \
	trunk_file_stat_func(store_path_index, true_filename, filename_len, \
			stat, pStat, pTrunkInfo, pTrunkHeader, pfd)

int trunk_file_stat_func(const int store_path_index, const char *true_filename,\
	const int filename_len, stat_func statfunc, \
	struct stat *pStat, FDFSTrunkFullInfo *pTrunkInfo, \
	FDFSTrunkHeader *pTrunkHeader, int *pfd);

int trunk_file_do_lstat_func(const int store_path_index, \
	const char *true_filename, \
	const int filename_len, stat_func statfunc, \
	struct stat *pStat, FDFSTrunkFullInfo *pTrunkInfo, \
	FDFSTrunkHeader *pTrunkHeader, int *pfd);

#ifdef __cplusplus
}
#endif

#endif

