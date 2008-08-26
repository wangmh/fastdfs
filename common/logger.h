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

#ifdef __cplusplus
extern "C" {
#endif

extern int g_log_level;
extern int g_log_fd;

int log_init(const char *filename_prefix);
void log_destory();

void log_it(const int priority, const char* format, ...);

void logError(const char* format, ...);
void logWarning(const char* format, ...);
void logInfo(const char* format, ...) ;

#ifdef __cplusplus
}
#endif

#endif


