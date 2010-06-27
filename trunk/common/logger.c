/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <pthread.h>
#include "shared_func.h"
#include "pthread_func.h"
#include "logger.h"

#ifndef LINE_MAX
#define LINE_MAX 2048
#endif

#define LOG_BUFF_SIZE    64 * 1024

LogContext g_log_context = {LOG_INFO, STDERR_FILENO};

static int log_fsync(LogContext *pContext, const bool bNeedLock);

static int check_and_mk_log_dir(const char *base_path)
{
	char data_path[MAX_PATH_SIZE];

	snprintf(data_path, sizeof(data_path), "%s/logs", base_path);
	if (!fileExists(data_path))
	{
		if (mkdir(data_path, 0755) != 0)
		{
			fprintf(stderr, "mkdir \"%s\" fail, " \
				"errno: %d, error info: %s", \
				data_path, errno, strerror(errno));
			return errno != 0 ? errno : EPERM;
		}
	}

	return 0;
}

int log_init_ex(LogContext *pContext)
{
	int result;

	pContext->log_level = LOG_INFO;
	pContext->log_fd = STDERR_FILENO;
	pContext->log_to_cache = false;

	pContext->log_buff = (char *)malloc(LOG_BUFF_SIZE);
	if (pContext->log_buff == NULL)
	{
		fprintf(stderr, "malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			LOG_BUFF_SIZE, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	pContext->pcurrent_buff = pContext->log_buff;

	if ((result=init_pthread_lock(&pContext->log_thread_lock)) != 0)
	{
		return result;
	}

	return 0;
}

int log_set_prefix_ex(LogContext *pContext, const char *base_path, \
		const char *filename_prefix)
{
	int result;
	char logfile[MAX_PATH_SIZE];

	if ((result=check_and_mk_log_dir(base_path)) != 0)
	{
		return result;
	}

	snprintf(logfile, MAX_PATH_SIZE, "%s/logs/%s.log", \
		base_path, filename_prefix);

	if ((pContext->log_fd = open(logfile, O_WRONLY | O_CREAT | O_APPEND, \
					0644)) < 0)
	{
		fprintf(stderr, "open log file \"%s\" to write fail, " \
			"errno: %d, error info: %s", \
			logfile, errno, strerror(errno));
		pContext->log_fd = STDERR_FILENO;
		return errno != 0 ? errno : EACCES;
	}

	return 0;
}

int log_set_filename_ex(LogContext *pContext, const char *log_filename)
{
	if ((pContext->log_fd = open(log_filename, O_WRONLY | O_CREAT | \
				O_APPEND, 0644)) < 0)
	{
		fprintf(stderr, "open log file \"%s\" to write fail, " \
			"errno: %d, error info: %s", \
			log_filename, errno, strerror(errno));
		pContext->log_fd = STDERR_FILENO;
		return errno != 0 ? errno : EACCES;
	}

	return 0;
}

void log_set_cache_ex(LogContext *pContext, const bool bLogCache)
{
	pContext->log_to_cache = bLogCache;
}

void log_destroy_ex(LogContext *pContext)
{
	if (pContext->log_fd >= 0 && pContext->log_fd != STDERR_FILENO)
	{
		log_fsync(pContext, true);

		close(pContext->log_fd);
		pContext->log_fd = STDERR_FILENO;

		pthread_mutex_destroy(&pContext->log_thread_lock);
	}

	if (pContext->log_buff != NULL)
	{
		free(pContext->log_buff);
		pContext->log_buff = NULL;
		pContext->pcurrent_buff = NULL;
	}
}

int log_sync_func(void *args)
{
	if (args == NULL)
	{
		return EINVAL;
	}

	return log_fsync((LogContext *)args, true);
}

static int log_fsync(LogContext *pContext, const bool bNeedLock)
{
	int result;
	int write_bytes;

	write_bytes = pContext->pcurrent_buff - pContext->log_buff;
	if (write_bytes == 0)
	{
		return 0;
	}

	result = 0;
	if (bNeedLock && (result=pthread_mutex_lock( \
			&pContext->log_thread_lock)) != 0)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	write_bytes = pContext->pcurrent_buff - pContext->log_buff;
	if (write(pContext->log_fd, pContext->log_buff, write_bytes) != \
		write_bytes)
	{
		result = errno != 0 ? errno : EIO;
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"call write fail, errno: %d, error info: %s\n",\
			 __LINE__, result, strerror(result));
	}

	if (pContext->log_fd != STDERR_FILENO)
	{
		if (fsync(pContext->log_fd) != 0)
		{
			result = errno != 0 ? errno : EIO;
			fprintf(stderr, "file: "__FILE__", line: %d, " \
				"call fsync fail, errno: %d, error info: %s\n",\
				 __LINE__, result, strerror(result));
		}
	}

	pContext->pcurrent_buff = pContext->log_buff;
	if (bNeedLock && (result=pthread_mutex_unlock( \
			&pContext->log_thread_lock)) != 0)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return result;
}

static void doLog(LogContext *pContext, const char *caption, \
		const char *text, const int text_len, const bool bNeedSync)
{
	time_t t;
	struct tm tm;
	int buff_len;
	int result;

	t = time(NULL);
	localtime_r(&t, &tm);
	if ((result=pthread_mutex_lock(&pContext->log_thread_lock)) != 0)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	if (text_len + 64 > LOG_BUFF_SIZE)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"log buff size: %d < log text length: %d ", \
			__LINE__, LOG_BUFF_SIZE, text_len + 64);
		pthread_mutex_unlock(&pContext->log_thread_lock);
		return;
	}

	if ((pContext->pcurrent_buff - pContext->log_buff) + text_len + 64 \
			> LOG_BUFF_SIZE)
	{
		log_fsync(pContext, false);
	}

	buff_len = sprintf(pContext->pcurrent_buff, \
			"[%04d-%02d-%02d %02d:%02d:%02d] %s - ", \
			tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, \
			tm.tm_hour, tm.tm_min, tm.tm_sec, caption);
	pContext->pcurrent_buff += buff_len;
	memcpy(pContext->pcurrent_buff, text, text_len);
	pContext->pcurrent_buff += text_len;
	*pContext->pcurrent_buff++ = '\n';

	if (!pContext->log_to_cache || bNeedSync)
	{
		log_fsync(pContext, false);
	}

	if ((result=pthread_mutex_unlock(&pContext->log_thread_lock)) != 0)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}
}

void log_it_ex1(LogContext *pContext, const int priority, \
		const char *text, const int text_len)
{
	bool bNeedSync;
	char *caption;

	switch(priority)
	{
		case LOG_DEBUG:
			bNeedSync = true;
			caption = "DEBUG";
			break;
		case LOG_INFO:
			bNeedSync = true;
			caption = "INFO";
			break;
		case LOG_NOTICE:
			bNeedSync = false;
			caption = "NOTICE";
			break;
		case LOG_WARNING:
			bNeedSync = false;
			caption = "WARNING";
			break;
		case LOG_ERR:
			bNeedSync = false;
			caption = "ERROR";
			break;
		case LOG_CRIT:
			bNeedSync = true;
			caption = "CRIT";
			break;
		case LOG_ALERT:
			bNeedSync = true;
			caption = "ALERT";
			break;
		case LOG_EMERG:
			bNeedSync = true;
			caption = "EMERG";
			break;
		default:
			bNeedSync = false;
			caption = "UNKOWN";
			break;
	}

	doLog(pContext, caption, text, text_len, bNeedSync);
}

void log_it_ex(LogContext *pContext, const int priority, const char *format, ...)
{
	bool bNeedSync;
	char text[LINE_MAX];
	char *caption;
	int len;

	va_list ap;
	va_start(ap, format);
	len = vsnprintf(text, sizeof(text), format, ap);
	va_end(ap);

	switch(priority)
	{
		case LOG_DEBUG:
			bNeedSync = true;
			caption = "DEBUG";
			break;
		case LOG_INFO:
			bNeedSync = true;
			caption = "INFO";
			break;
		case LOG_NOTICE:
			bNeedSync = false;
			caption = "NOTICE";
			break;
		case LOG_WARNING:
			bNeedSync = false;
			caption = "WARNING";
			break;
		case LOG_ERR:
			bNeedSync = false;
			caption = "ERROR";
			break;
		case LOG_CRIT:
			bNeedSync = true;
			caption = "CRIT";
			break;
		case LOG_ALERT:
			bNeedSync = true;
			caption = "ALERT";
			break;
		case LOG_EMERG:
			bNeedSync = true;
			caption = "EMERG";
			break;
		default:
			bNeedSync = false;
			caption = "UNKOWN";
			break;
	}

	doLog(pContext, caption, text, len, bNeedSync);
}


#define _DO_LOG(pContext, priority, caption, bNeedSync) \
	char text[LINE_MAX]; \
	int len; \
\
	if (pContext->log_level < priority) \
	{ \
		return; \
	} \
\
	{ \
	va_list ap; \
	va_start(ap, format); \
	len = vsnprintf(text, sizeof(text), format, ap);  \
	va_end(ap); \
	} \
\
	doLog(pContext, caption, text, len, bNeedSync); \


void logEmergEx(LogContext *pContext, const char *format, ...)
{
	_DO_LOG(pContext, LOG_EMERG, "EMERG", true)
}

void logAlertEx(LogContext *pContext, const char *format, ...)
{
	_DO_LOG(pContext, LOG_ALERT, "ALERT", true)
}

void logCritEx(LogContext *pContext, const char *format, ...)
{
	_DO_LOG(pContext, LOG_CRIT, "CRIT", true)
}

void logErrorEx(LogContext *pContext, const char *format, ...)
{
	_DO_LOG(pContext, LOG_ERR, "ERROR", false)
}

void logWarningEx(LogContext *pContext, const char *format, ...)
{
	_DO_LOG(pContext, LOG_WARNING, "WARNING", false)
}

void logNoticeEx(LogContext *pContext, const char *format, ...)
{
	_DO_LOG(pContext, LOG_NOTICE, "NOTICE", false)
}

void logInfoEx(LogContext *pContext, const char *format, ...)
{
	_DO_LOG(pContext, LOG_INFO, "INFO", true)
}

void logDebugEx(LogContext *pContext, const char *format, ...)
{
	_DO_LOG(pContext, LOG_DEBUG, "DEBUG", true)
}

#ifndef LOG_FORMAT_CHECK

void logEmerg(const char *format, ...)
{
	_DO_LOG((&g_log_context), LOG_EMERG, "EMERG", true)
}

void logAlert(const char *format, ...)
{
	_DO_LOG((&g_log_context), LOG_ALERT, "ALERT", true)
}

void logCrit(const char *format, ...)
{
	_DO_LOG((&g_log_context), LOG_CRIT, "CRIT", true)
}

void logError(const char *format, ...)
{
	_DO_LOG((&g_log_context), LOG_ERR, "ERROR", false)
}

void logWarning(const char *format, ...)
{
	_DO_LOG((&g_log_context), LOG_WARNING, "WARNING", false)
}

void logNotice(const char *format, ...)
{
	_DO_LOG((&g_log_context), LOG_NOTICE, "NOTICE", false)
}

void logInfo(const char *format, ...)
{
	_DO_LOG((&g_log_context), LOG_INFO, "INFO", true)
}

void logDebug(const char *format, ...)
{
	_DO_LOG((&g_log_context), LOG_DEBUG, "DEBUG", true)
}

#endif

