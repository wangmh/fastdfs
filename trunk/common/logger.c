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
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include "fdfs_define.h"
#include "fdfs_global.h"
#include "shared_func.h"
#include "logger.h"

char g_error_file_prefix[64] = {'\0'};

int check_and_mk_log_dir()
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
			return errno != 0 ? errno : ENOENT;
		}
	}

	return 0;
}

static void doLog(const char* prefix, const char* text)
{
	time_t t;
	struct tm *pCurrentTime;
	char dateBuffer[32];
	char logfile[MAX_PATH_SIZE];
	FILE *fp;
	int fd;
	struct flock lock;

	t = time(NULL);
	pCurrentTime = localtime(&t);
	strftime(dateBuffer, sizeof(dateBuffer), "[%Y-%m-%d %X]", pCurrentTime);

	if (*prefix != '\0')
	{
		snprintf(logfile, MAX_PATH_SIZE, "%s/logs/%s.log", \
				g_base_path, prefix);

		umask(0);
		if ((fp = fopen(logfile, "a")) == NULL)
		{
			fp = stderr;
		}
	}
	else
	{
		fp = stderr;
	}
	
	fd = fileno(fp);
	
	lock.l_type = F_WRLCK;
	lock.l_whence = SEEK_SET;
	lock.l_start = 0;
	lock.l_len = 10;
	if (fcntl(fd, F_SETLKW, &lock) == 0)
	{
		fprintf(fp, "%s %s\n", dateBuffer, text);
	}
	
	lock.l_type = F_UNLCK;
	fcntl(fd, F_SETLKW, &lock);
	if (fp != stderr)
	{
		fclose(fp);
	}
}

void periodLog( const char* prefix, const char *date_format, \
		const char *szDatetimeFormat, const char* text )
{
	time_t t;
	struct tm *pCurrentTime;
	char dateBuffer[32];
	char logfile[MAX_PATH_SIZE];
	FILE *fp;
	int fd;
	struct flock lock;
	
	t = time(NULL);
	pCurrentTime = localtime(&t);
	strftime((char *)dateBuffer, sizeof(dateBuffer), date_format, pCurrentTime);
	snprintf(logfile, MAX_PATH_SIZE, "%s/logs/%s%s.log", g_base_path, prefix, dateBuffer);
	strftime((char *)dateBuffer, sizeof(dateBuffer), szDatetimeFormat, pCurrentTime);

	umask(0);
	if (( fp=fopen(logfile,"a")) == NULL)
	{
		return;
	}
	
	fd = fileno(fp);
	
	lock.l_type = F_WRLCK;
	lock.l_whence = SEEK_SET;
	lock.l_start = 0;
	lock.l_len = 10;
	if (fcntl(fd, F_SETLKW, &lock) == 0)
	{
		fprintf(fp, "%s %s\n", dateBuffer, text);
	}
	
	lock.l_type = F_UNLCK;
	fcntl(fd, F_SETLKW, &lock);

	fclose(fp);
}

void logError(const char* format, ...)
{
	char logBuffer[LINE_MAX];
	va_list ap;
	va_start(ap, format);
	vsnprintf(logBuffer, sizeof(logBuffer), format, ap);    
	doLog(g_error_file_prefix, logBuffer);
	va_end(ap);
}

void logErrorEx(const char* prefix, const char* format, ...)
{
	char logBuffer[LINE_MAX];
	va_list ap;
	va_start(ap, format);
	vsnprintf(logBuffer, sizeof(logBuffer), format, ap);    
	doLog(prefix, logBuffer);
	va_end(ap);
}

void logInfo(const char* prefix, const char* format, ...)
{
	char logBuffer[LINE_MAX];
	va_list ap;
	va_start(ap, format);
	vsnprintf(logBuffer, sizeof(logBuffer), format, ap);    
	doLog(prefix, logBuffer);
	va_end(ap);
}

void logDaily(const char* prefix, const char* format, ...)
{
	char logBuffer[LINE_MAX];
	va_list ap;
	va_start(ap, format);
	vsnprintf(logBuffer, sizeof(logBuffer), format, ap);    
	periodLog(prefix, "%Y%m%d", "[%X]", logBuffer);
	va_end(ap);
}

void logMonthly(const char* prefix, const char* format, ...)
{
	char logBuffer[LINE_MAX];
	va_list ap;
	va_start(ap, format);
	vsnprintf(logBuffer, sizeof(logBuffer), format, ap);    
	periodLog(prefix, "%Y%m", "[%Y-%m-%d %X]", logBuffer);
	va_end(ap);
}

