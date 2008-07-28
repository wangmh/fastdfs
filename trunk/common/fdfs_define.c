/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdfs_define.c
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#ifndef WIN32
#include <unistd.h>
#endif
#include "fdfs_define.h"

#ifdef WIN32
int snprintf(char *s, const int size, const char *format, ...)
{
	int nResult;
	va_list ap;
	va_start(ap, format);
	nResult = _vsnprintf(s, size - 1, format, ap);
	va_end(ap);
	
	if (nResult > 0 && nResult < size)
	{
		s[nResult] = '\0';
	}
	else
	{
		s[size - 1] = '\0';
	}
	return nResult;
}
#endif

void sleepEx(int seconds)
{
#ifdef WIN32
	Sleep(seconds * 1000);
#else
	int nRemainSecs;
	nRemainSecs = seconds;
	while (nRemainSecs > 0)
	{
		nRemainSecs = sleep(nRemainSecs);
	}
#endif
}
