/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdfs_define.h

#ifndef _DEFINE_H_
#define _DEFINE_H_

#include <pthread.h>

#ifdef WIN32

#include <windows.h>
#include <winsock.h>
typedef UINT in_addr_t;
#define FILE_SEPERATOR	"\\"
#define THREAD_ENTRANCE_FUNC_DECLARE  DWORD WINAPI
#define THREAD_RETURN_VALUE	 0
typedef DWORD (WINAPI *ThreadEntranceFunc)(LPVOID lpThreadParameter);
#else

#include <unistd.h>
#include <signal.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#define FILE_SEPERATOR	"/"
typedef int SOCKET;
#define closesocket     close
#define INVALID_SOCKET  -1
#define THREAD_ENTRANCE_FUNC_DECLARE  void *
typedef void *LPVOID;
#define THREAD_RETURN_VALUE	 NULL
typedef void * (*ThreadEntranceFunc)(LPVOID lpThreadParameter);

#endif

extern int pthread_mutexattr_settype(pthread_mutexattr_t *attr, int kind);
#ifdef OS_LINUX
#define PTHREAD_MUTEX_ERRORCHECK PTHREAD_MUTEX_ERRORCHECK_NP
#endif

//#define USE_SENDFILE

#define MAX_PATH_SIZE				256

#define FDFS_BASE_FILE_PATH			"/usr/local/FastDFS"
#define LOG_FILE_DIR				"logs"
#define CONF_FILE_DIR				"conf"
#define DEFAULT_NETWORK_TIMEOUT			30
#define FDFS_TRACKER_SERVER_DEF_PORT		22000
#define FDFS_STORAGE_SERVER_DEF_PORT		23000
#define FDFS_DEF_MAX_CONNECTONS			256
#define FDFS_DEF_STORAGE_RESERVED_MB		1024
#define TRACKER_ERROR_LOG_FILENAME      "trackerd"
#define STORAGE_ERROR_LOG_FILENAME      "storaged"

#define FDFS_IPADDR_SIZE	16

#define FDFS_RECORD_SEPERATOR	'\x01'
#define FDFS_FIELD_SEPERATOR	'\x02'

#ifndef true
typedef char  bool;
#define true  1
#define false 0
#endif

#ifndef byte
#define byte char
#endif

#ifndef ubyte
#define ubyte unsigned char
#endif

typedef struct tagIdName
{
	int id;
	char *name;
} IdName;

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*FreeDataFunc)(void *ptr);
typedef int (*CompareFunc)(void *p1, void *p2);

void sleepEx(int seconds);

#ifdef WIN32
#define strcasecmp	_stricmp
//#define sleep(x)    Sleep(1000 * (x))
int snprintf(char *s, const int size, const char *format, ...);
#endif

#ifdef __cplusplus
}
#endif

#endif
