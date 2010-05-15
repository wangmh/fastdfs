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
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "common_define.h"
#include "ini_file_reader.h"

#ifdef __cplusplus
extern "C" {
#endif

/** lowercase the string
 *  parameters:
 *  	src: input string, will be changed
 *  return: lowercased string
*/
char *toLowercase(char *src);

/** uppercase the string
 *  parameters:
 *  	src: input string, will be changed
 *  return: uppercased string
*/
char *toUppercase(char *src);


/** date format to string
 *  parameters:
 *  	nTime: unix timestamp
 *  	szDateFormat: date format, more detail man strftime
 *  	buff: store the formated result, can be NULL
 *  	buff_size: buffer size, max bytes can contain
 *  return: formated date string
*/
char *formatDatetime(const time_t nTime, \
	const char *szDateFormat, \
	char *buff, const int buff_size);

/** get character count, only support GB charset
 *  parameters:
 *  	s: the string
 *  return: character count
*/
int getCharLen(const char *s);

/** replace \r and \n to space
 *  parameters:
 *  	s: the string
 *  return: replaced string
*/
char *replaceCRLF2Space(char *s);

/** get the filename absolute path
 *  parameters:
 *  	fileame: the filename
 *  	szAbsPath: store the absolute path
 *  	pathSize: max bytes to contain
 *  return: absolute path, NULL for fail
*/
char *getAbsolutePath(const char *fileame, char *szAbsPath, \
				const int pathSize);

/** get the executable file absolute filename
 *  parameters:
 *  	exeFilename: the executable filename
 *  	szAbsFilename: store the absolute filename
 *  	maxSize: max bytes to contain
 *  return: absolute filename, NULL for fail
*/
char *getExeAbsoluteFilename(const char *exeFilename, char *szAbsFilename, \
		const int maxSize);

/** get running process count by program name such as fdfs_trackerd
 *  parameters:
 *  	progName: the program name
 * 	bAllOwners: false for only get my proccess count
 *  return: proccess count, >= 0 success, < 0 fail
*/
int getProccessCount(const char *progName, const bool bAllOwners);

/** get running process ids by program name such as fdfs_trackerd
 *  parameters:
 *  	progName: the program name
 * 	bAllOwners: false for only get my proccess count
 * 	pids: store the pids
 * 	arrSize: max pids
 *  return: proccess count, >= 0 success, < 0 fail
*/
int getUserProcIds(const char *progName, const bool bAllOwners, \
			int pids[], const int arrSize);

/** execute program, get it's output
 *  parameters:
 *  	command: the program
 * 	output: store ouput result
 * 	buff_size: output max size (bytes)
 *  return: error no, 0 success, != 0 fail
*/
int getExecResult(const char *command, char *output, const int buff_size);

/** daemon init
 *  parameters:
 *  	bCloseFiles: if close the stdin, stdout and stderr
 *  return: none
*/
void daemon_init(bool bCloseFiles);

/** convert buffer content to hex string such as 0B82A1
 *  parameters:
 *  	s: the buffer
 *  	len: the buffer length
 *  	szHexBuff: store the hex string (must have enough space)
 *  return: hex string (szHexBuff)
*/
char *bin2hex(const char *s, const int len, char *szHexBuff);

/** parse hex string to binary content
 *  parameters:
 *  	s: the hex string such as 8B04CD
 *  	szBinBuff: store the converted binary content(must have enough space)
 *  	nDestLen: store the converted content length
 *  return: converted binary content (szBinBuff)
*/
char *hex2bin(const char *s, char *szBinBuff, int *nDestLen);

/** print binary buffer as hex string
 *  parameters:
 *  	s: the buffer
 *  	len: the buffer length
 *  return: none
*/
void printBuffHex(const char *s, const int len);

/** 32 bits int convert to buffer (big-endian)
 *  parameters:
 *  	n: 32 bits int value
 *  	buff: the buffer, at least 4 bytes space, no tail \0
 *  return: none
*/
void int2buff(const int n, char *buff);

/** buffer convert to 32 bits int
 *  parameters:
 *  	buff: big-endian 4 bytes buffer
 *  return: 32 bits int value
*/
int buff2int(const char *buff);

/** long (64 bits) convert to buffer (big-endian)
 *  parameters:
 *  	n: 64 bits int value
 *  	buff: the buffer, at least 8 bytes space, no tail \0
 *  return: none
*/
void long2buff(int64_t n, char *buff);

/** buffer convert to 64 bits int
 *  parameters:
 *  	buff: big-endian 8 bytes buffer
 *  return: 64 bits int value
*/
int64_t buff2long(const char *buff);

/** trim leading spaces ( \t\r\n)
 *  parameters:
 *  	pStr: the string to trim
 *  return: trimed string porinter as pStr
*/
char *trim_left(char *pStr);

/** trim tail spaces ( \t\r\n)
 *  parameters:
 *  	pStr: the string to trim
 *  return: trimed string porinter as pStr
*/
char *trim_right(char *pStr);

/** trim leading and tail spaces ( \t\r\n)
 *  parameters:
 *  	pStr: the string to trim
 *  return: trimed string porinter as pStr
*/
char *trim(char *pStr);

/** copy string to BufferInfo
 *  parameters:
 *  	pBuff: the dest buffer
 *  	str: source string
 *  return: error no, 0 success, != 0 fail
*/
int buffer_strcpy(BufferInfo *pBuff, const char *str);

/** copy binary buffer to BufferInfo
 *  parameters:
 *  	pBuff: the dest buffer
 *  	buff: source buffer
 *  	len: source buffer length
 *  return: error no, 0 success, != 0 fail
*/
int buffer_memcpy(BufferInfo *pBuff, const char *buff, const int len);

/** url encode
 *  parameters:
 *  	src: the source stirng to encode
 *  	src_len: source string length
 *  	dest: store dest string
 *  	dest_len: store the dest string length
 *  return: error no, 0 success, != 0 fail
*/
char *urlencode(const char *src, const int src_len, char *dest, int *dest_len);

/** url decode
 *  parameters:
 *  	src: the source stirng to decode
 *  	src_len: source string length
 *  	dest: store dest string
 *  	dest_len: store the dest string length
 *  return: error no, 0 success, != 0 fail
*/
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
void load_log_level(IniContext *pIniContext);
void set_log_level(char *pLogLevel);
int load_allow_hosts(IniContext *pIniContext, \
		in_addr_t **allow_ip_addrs, int *allow_ip_count);

int get_time_item_from_conf(IniContext *pIniContext, \
		const char *item_name, TimeInfo *pTimeInfo, \
		const byte default_hour, const byte default_minute);

void chopPath(char *filePath);
int getFileContent(const char *filename, char **buff, int64_t *file_size);
int writeToFile(const char *filename, const char *buff, const int file_size);
int fd_gets(int fd, char *buff, const int size, int once_bytes);

int set_rlimit(int resource, const rlim_t value);
int set_nonblock(int fd);

int set_run_by(const char *group_name, const char *username);
int cmp_by_ip_addr_t(const void *p1, const void *p2);

int parse_bytes(char *pStr, const int default_unit_bytes, int64_t *bytes);

int set_rand_seed();

#ifdef __cplusplus
}
#endif

#endif
