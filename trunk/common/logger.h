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

extern int g_log_level;
extern int g_log_fd;

int log_init();
int log_set_prefix(const char *base_path, const char *filename_prefix);
void log_set_cache(const bool bLogCache);
void log_destory();

void log_it(const int priority, const char* format, ...);
void log_it_ex(const int priority, const char *text, const int text_len);
int log_sync_func(void *args);

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


