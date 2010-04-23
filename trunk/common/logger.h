/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//logger.h
#ifndef LOGGER_H
#define LOGGER_H

#include <syslog.h>
#include "common_define.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct log_context
{
	int log_level;
	int log_fd;
	char *log_buff;
	char *pcurrent_buff;
	pthread_mutex_t log_thread_lock;
	bool log_to_cache;
} LogContext;

extern LogContext g_log_context;

#define log_init()  log_init_ex(&g_log_context)

#define log_set_prefix(base_path, filename_prefix) \
	log_set_prefix_ex(&g_log_context, base_path, filename_prefix)

#define log_set_filename(log_filename) \
	log_set_filename_ex(&g_log_context, log_filename)

#define log_set_cache(bLogCache)  log_set_cache_ex(&g_log_context, bLogCache)

#define log_destory()  log_destory_ex(&g_log_context)


int log_init_ex(LogContext *pContext);
int log_set_prefix_ex(LogContext *pContext, const char *base_path, \
		const char *filename_prefix);
int log_set_filename_ex(LogContext *pContext, const char *log_filename);
void log_set_cache_ex(LogContext *pContext, const bool bLogCache);
void log_destory_ex(LogContext *pContext);

void log_it_ex(LogContext *pContext, const int priority, \
		const char* format, ...);
void log_it_ex1(LogContext *pContext, const int priority, \
		const char *text, const int text_len);
int log_sync_func(void *args);

void logEmergEx(LogContext *pContext, const char* format, ...);
void logCritEx(LogContext *pContext, const char* format, ...);
void logAlertEx(LogContext *pContext, const char* format, ...);
void logErrorEx(LogContext *pContext, const char* format, ...);
void logWarningEx(LogContext *pContext, const char* format, ...);
void logNoticeEx(LogContext *pContext, const char* format, ...);
void logInfoEx(LogContext *pContext, const char* format, ...);
void logDebugEx(LogContext *pContext, const char* format, ...);

//#define LOG_FORMAT_CHECK

#ifdef LOG_FORMAT_CHECK  /*only for format check*/

#define logEmerg   printf
#define logCrit    printf
#define logAlert   printf
#define logError   printf
#define logWarning printf
#define logNotice  printf
#define logInfo    printf
#define logDebug   printf

#else

void logEmerg(const char* format, ...);
void logCrit(const char* format, ...);
void logAlert(const char* format, ...);
void logError(const char* format, ...);
void logWarning(const char* format, ...);
void logNotice(const char* format, ...);
void logInfo(const char* format, ...);
void logDebug(const char* format, ...);

#endif

#ifdef __cplusplus
}
#endif

#endif


