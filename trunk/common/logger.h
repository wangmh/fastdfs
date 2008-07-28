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

#ifdef __cplusplus
extern "C" {
#endif

extern char g_error_file_prefix[64];

int check_and_mk_log_dir();
void logError(const char* format, ...) ;
void logErrorEx(const char* prefix, const char* format, ...);
void logInfo(const char* prefix, const char* format, ...) ;
void logDaily(const char* prefix, const char* format, ...) ;
void logMonthly(const char* prefix, const char* format, ...);

#ifdef __cplusplus
}
#endif

#endif


