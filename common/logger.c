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
#include "fdfs_define.h"
#include "fdfs_global.h"
#include "shared_func.h"
#include "logger.h"

int g_log_level = LOG_INFO;
int g_log_fd = STDERR_FILENO;

static int check_and_mk_log_dir()
{
	char data_path[MAX_PATH_SIZE];

	snprintf(data_path, sizeof(data_path), "%s/logs", g_base_path);
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

int log_init(const char *filename_prefix)
{
	int result;
	char logfile[MAX_PATH_SIZE];

	if ((result=check_and_mk_log_dir()) != 0)
	{
		return result;
	}

	log_destory();

	snprintf(logfile, MAX_PATH_SIZE, "%s/logs/%s.log", \
		g_base_path, filename_prefix);

	if ((g_log_fd = open(logfile, O_WRONLY | O_CREAT | O_APPEND, 0644)) < 0)
	{
		fprintf(stderr, "open log file \"%s\" to write fail, " \
			"errno: %d, error info: %s", \
			logfile, errno, strerror(errno));
		g_log_fd = STDERR_FILENO;
		result = errno != 0 ? errno : EACCES;
	}
	else
	{
		result = 0;
	}

	return result;
}

void log_destory()
{
	if (g_log_fd >= 0 && g_log_fd != STDERR_FILENO)
	{
		close(g_log_fd);
		g_log_fd = STDERR_FILENO;
	}
}

static void doLog(const char *prefix, const char* text, const int text_len)
{
	time_t t;
	struct tm *pCurrentTime;
	char buff[64];
	int len;
	int prefix_len;
	struct flock lock;

	t = time(NULL);
	pCurrentTime = localtime(&t);
	len = strftime(buff, sizeof(buff), "[%Y-%m-%d %X] ", pCurrentTime);
	if (len > 0)
	{
		prefix_len = strlen(prefix);
		if (prefix_len > sizeof(buff) - 3 - len)
		{
			prefix_len = sizeof(buff) - 3 - len;
		}
		memcpy(buff+len, prefix, prefix_len);
		len += prefix_len;
		memcpy(buff+len, " - ", 3);
		len += 3;
	}

	if (g_log_fd != STDERR_FILENO)
	{
		lock.l_type = F_WRLCK;
		lock.l_whence = SEEK_SET;
		lock.l_start = 0;
		lock.l_len = 0;
		if (fcntl(g_log_fd, F_SETLKW, &lock) != 0)
		{
			fprintf(stderr, "file: "__FILE__", line: %d, " \
				"call fcntl fail, errno: %d, error info: %s\n",\
				 __LINE__, errno, strerror(errno));
		}
	}

	if (write(g_log_fd, buff, len) != len)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"call write fail, errno: %d, error info: %s\n",\
			 __LINE__, errno, strerror(errno));
	}

	if (write(g_log_fd, text, text_len) != text_len)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"call write fail, errno: %d, error info: %s\n",\
			 __LINE__, errno, strerror(errno));
	}

	if (write(g_log_fd, "\n", 1) != 1)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"call write fail, errno: %d, error info: %s\n",\
			 __LINE__, errno, strerror(errno));
	}

	if (g_log_fd != STDERR_FILENO)
	{
		lock.l_type = F_UNLCK;
		fcntl(g_log_fd, F_SETLKW, &lock);
	}

	if (fsync(g_log_fd) != 0)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"call fsync fail, errno: %d, error info: %s\n",\
			 __LINE__, errno, strerror(errno));
	}
}

void log_it(const int priority, const char* format, ...)
{
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
			caption = "DEBUG";
			break;
		case LOG_INFO:
			caption = "INFO";
			break;
		case LOG_NOTICE:
			caption = "NOTICE";
			break;
		case LOG_WARNING:
			caption = "WARNING";
			break;
		case LOG_ERR:
			caption = "ERROR";
			break;
		case LOG_CRIT:
			caption = "CRIT";
			break;
		case LOG_ALERT:
			caption = "ALERT";
			break;
		case LOG_EMERG:
			caption = "EMERG";
			break;
		default:
			caption = "UNKOWN";
			break;
	}

	doLog(caption, text, len);
}

#define _DO_LOG(priority, caption) \
	char text[LINE_MAX]; \
	int len; \
\
	if (g_log_level < priority) \
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
	doLog(caption, text, len); \


void logError(const char *format, ...)
{
	_DO_LOG(LOG_ERR, "ERROR")
}

void logWarning(const char *format, ...)
{
	_DO_LOG(LOG_WARNING, "WARNING")
}

void logInfo(const char *format, ...)
{
	_DO_LOG(LOG_INFO, "INFO")
}

