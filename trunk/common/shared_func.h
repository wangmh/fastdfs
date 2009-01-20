/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#ifndef SHARED_FUNC_H
#define SHARED_FUNC_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "common_define.h"
#include "ini_file_reader.h"

#ifdef __cplusplus
extern "C" {
#endif

char *toLowercase(char *src);
char *toUppercase(char *src);

char *formatDatetime(const time_t nTime, \
	const char *szDateFormat, \
	char *buff, const int buff_size);

int getCharLen(const char *s);
char *replaceCRLF2Space(char *s);

char *getAppAbsolutePath(const char *exeName, char *szAbsPath, \
				const int pathSize);

int getProccessCount(const char *progName, const bool bAllOwners);

int getUserProcIds(const char *progName, const bool bAllOwners, \
			int pids[], const int arrSize);

void daemon_init(bool bCloseFiles);

char *bin2hex(const char *s, const int len, char *szHexBuff);
char *hex2bin(const char *s, char *szBinBuff, int *nDestLen);
void printBuffHex(const char *s, const int len);
char int2base62(const int i);

void int2buff(const int n, char *buff);
int buff2int(const char *buff);
void long2buff(int64_t n, char *buff);
int64_t buff2long(const char *buff);

char *trim_left(char *pStr);
char *trim_right(char *pStr);
char *trim(char *pStr);

char *urlencode(const char *src, const int src_len, char *dest, int *dest_len);
char *urldecode(const char *src, const int src_len, char *dest, int *dest_len);

int getOccurCount(const char *src, const char seperator);
char **split(char *src, const char seperator, const int nMaxCols, \
		int *nColCount);
void freeSplit(char **p);

int splitEx(char *src, const char seperator, char **pCols, const int nMaxCols);
int my_strtok(char *src, const char *delim, char **pCols, const int nMaxCols);

bool fileExists(const char *filename);
bool isDir(const char *filename);
bool isFile(const char *filename);
bool is_filename_secure(const char *filename, const int len);
int fdfs_check_data_filename(const char *filename, const int len);
void load_log_level(IniItemInfo *items, const int nItemCount);
int load_allow_hosts(IniItemInfo *items, const int nItemCount, \
		in_addr_t **allow_ip_addrs, int *allow_ip_count);

int get_time_item_from_conf(IniItemInfo *items, const int nItemCount, \
		const char *item_name, TimeInfo *pTimeInfo, \
		const byte default_hour, const byte default_minute);

void chopPath(char *filePath);
int getFileContent(const char *filename, char **buff, off_t *file_size);
int writeToFile(const char *filename, const char *buff, const int file_size);
int fd_gets(int fd, char *buff, const int size, int once_bytes);

int init_pthread_lock(pthread_mutex_t *pthread_lock);
int init_pthread_attr(pthread_attr_t *pattr);

int set_rlimit(int resource, const rlim_t value);
int set_nonblock(int fd);

int set_run_by(const char *group_name, const char *username);
int cmp_by_ip_addr_t(const void *p1, const void *p2);

int create_work_threads(int *count, void *(*start_func)(void *), \
		void *arg, pthread_t *tids);
int kill_work_threads(pthread_t *tids, const int count);

int parse_bytes(char *pStr, const int default_unit_bytes, int64_t *bytes);

int set_rand_seed();

#ifdef __cplusplus
}
#endif

#endif
