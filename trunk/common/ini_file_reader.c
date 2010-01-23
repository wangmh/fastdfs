/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//ini_file_reader.c

#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "shared_func.h"
#include "logger.h"
#include "http_func.h"
#include "ini_file_reader.h"

#define _LINE_BUFFER_SIZE	512
#define _ALLOC_ITEMS_ONCE	8

static int iniDoLoadItemsFromFile(const char *szFilename, \
		IniItemContext *pContext, int *nAllocItems);
static int iniDoLoadItemsFromBuffer(char *content, \
		IniItemContext *pContext, int *nAllocItems);

int compareByItemName(const void *p1, const void *p2)
{
	return strcmp(((IniItemInfo *)p1)->name, ((IniItemInfo *)p2)->name);
}

int iniLoadItems(const char *szFilename, IniItemContext *pContext)
{
	int alloc_items;
	int result;
	char *pLast;
	char old_cwd[MAX_PATH_SIZE];

	memset(old_cwd, 0, sizeof(old_cwd));
	if (strncasecmp(szFilename, "http://", 7) != 0)
	{
		pLast = strrchr(szFilename, '/');
		if (pLast != NULL)
		{
			char path[256];
			int len;

			if (getcwd(old_cwd, sizeof(old_cwd)) == NULL)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"getcwd fail, errno: %d, " \
					"error info: %s", \
					__LINE__, errno, strerror(errno));
				*old_cwd = '\0';
			}

			len = pLast - szFilename;
			if (len >= sizeof(path))
			{
				len = sizeof(path) - 1;
			}

			memcpy(path, szFilename, len);
			*(path + len) = '\0';
			if (chdir(path) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"chdir to the path of conf file: " \
					"%s fail, errno: %d, error info: %s", \
					__LINE__, szFilename, \
					errno, strerror(errno));
				return errno != 0 ? errno : ENOENT;
			}
		}
	}

	pContext->count = 0;
	alloc_items = _ALLOC_ITEMS_ONCE;
	pContext->items = (IniItemInfo *)malloc(sizeof(IniItemInfo) * alloc_items);
	if (pContext->items == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %ld bytes fail", __LINE__, \
			sizeof(IniItemInfo) * alloc_items);
		return errno != 0 ? errno : ENOMEM;
	}

	memset(pContext->items, 0, sizeof(IniItemInfo) * alloc_items);
	result = iniDoLoadItemsFromFile(szFilename, pContext, &alloc_items);
	if (result != 0)
	{
		if (pContext->items != NULL)
		{
			free(pContext->items);
			pContext->items = NULL;
		}
		pContext->count = 0;
	}
	else
	{
		qsort(pContext->items, pContext->count, sizeof(IniItemInfo), \
			compareByItemName);
	}

	if (*old_cwd != '\0' && chdir(old_cwd) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"chdir to old path: %s fail, " \
			"errno: %d, error info: %s", \
			__LINE__, old_cwd, errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	return result;
}

static int iniDoLoadItemsFromFile(const char *szFilename, \
		IniItemContext *pContext, int *nAllocItems)
{
	char *content;
	int result;
	int http_status;
	int content_len;
	int64_t file_size;

	if (strncasecmp(szFilename, "http://", 7) == 0)
	{
		if ((result=get_url_content(szFilename, 60, &http_status, \
				&content, &content_len)) != 0)
		{
			return result;
		}

		if (http_status != 200)
		{
			free(content);
			logError("file: "__FILE__", line: %d, " \
				"HTTP status code: %d != 200, url=%s", \
				__LINE__, http_status, szFilename);
			return EINVAL;
		}
	}
	else
	{
		if ((result=getFileContent(szFilename, &content, \
				&file_size)) != 0)
		{
			return result;
		}
	}

	result = iniDoLoadItemsFromBuffer(content, pContext, nAllocItems);
	free(content);

	return result;
}

int iniLoadItemsFromBuffer(char *content, IniItemContext *pContext)
{
	int alloc_items;
	int result;

	pContext->count = 0;
	alloc_items = _ALLOC_ITEMS_ONCE;
	pContext->items = (IniItemInfo *)malloc(sizeof(IniItemInfo) * alloc_items);
	if (pContext->items == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %ld bytes fail", __LINE__, \
			sizeof(IniItemInfo) * alloc_items);
		return errno != 0 ? errno : ENOMEM;
	}

	memset(pContext->items, 0, sizeof(IniItemInfo) * alloc_items);
	result = iniDoLoadItemsFromBuffer(content, pContext, &alloc_items);
	if (result != 0)
	{
		if (pContext->items != NULL)
		{
			free(pContext->items);
			pContext->items = NULL;
		}
		pContext->count = 0;
	}
	else
	{
		qsort(pContext->items, pContext->count, sizeof(IniItemInfo), \
			compareByItemName);
	}

	return result;
}

static int iniDoLoadItemsFromBuffer(char *content, IniItemContext *pContext, \
		int *nAllocItems)
{
	IniItemInfo *pItem;
	char *pLine;
	char *pLastEnd;
	char *pEqualChar;
	char *pIncludeFilename;
	int nNameLen;
	int nValueLen;
	int result;

	result = 0;
	pLastEnd = content - 1;
	pItem = pContext->items + pContext->count;
	while (pLastEnd != NULL)
	{
		pLine = pLastEnd + 1;
		pLastEnd = strchr(pLine, '\n');
		if (pLastEnd != NULL)
		{
			*pLastEnd = '\0';
		}

		if (*pLine == '#' && \
			strncasecmp(pLine+1, "include", 7) == 0 && \
			(*(pLine+8) == ' ' || *(pLine+8) == '\t'))
		{
			pIncludeFilename = strdup(pLine + 9);
			if (pIncludeFilename == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"strdup %ld bytes fail", __LINE__, \
					strlen(pLine + 9) + 1);
				result = errno != 0 ? errno : ENOMEM;
				break;
			}

			trim(pIncludeFilename);
			if (strncasecmp(pIncludeFilename, "http://", 7) != 0 \
				&& !fileExists(pIncludeFilename))
			{
				logError("file: "__FILE__", line: %d, " \
					"include file \"%s\" not exists, " \
					"line: \"%s\"", __LINE__, \
					pIncludeFilename, pLine);
				free(pIncludeFilename);
				result = ENOENT;
				break;
			}

			result = iniDoLoadItemsFromFile(pIncludeFilename, \
					pContext, nAllocItems);
			if (result != 0)
			{
				free(pIncludeFilename);
				break;
			}

			pItem = pContext->items + pContext->count;  //must re-asign
			free(pIncludeFilename);
			continue;
		}

		trim(pLine);
		if (*pLine == '#' || *pLine == '\0')
		{
			continue;
		}
		
		pEqualChar = strchr(pLine, '=');
		if (pEqualChar == NULL)
		{
			continue;
		}
		
		nNameLen = pEqualChar - pLine;
		nValueLen = strlen(pLine) - (nNameLen + 1);
		if (nNameLen > INI_ITEM_NAME_LEN)
		{
			nNameLen = INI_ITEM_NAME_LEN;
		}
		
		if (nValueLen > INI_ITEM_VALUE_LEN)
		{
			nValueLen = INI_ITEM_VALUE_LEN;
		}
	
		if (pContext->count >= *nAllocItems)
		{
			(*nAllocItems) += _ALLOC_ITEMS_ONCE;
			pContext->items=(IniItemInfo *)realloc(pContext->items, 
				sizeof(IniItemInfo) * (*nAllocItems));
			if (pContext->items == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"realloc %ld bytes fail", __LINE__, \
					sizeof(IniItemInfo) * (*nAllocItems));
				result = errno != 0 ? errno : ENOMEM;
				break;
			}

			pItem = pContext->items + pContext->count;
			memset(pItem, 0, sizeof(IniItemInfo) * \
				((*nAllocItems) - pContext->count));
		}

		memcpy(pItem->name, pLine, nNameLen);
		memcpy(pItem->value, pEqualChar + 1, nValueLen);
		
		trim(pItem->name);
		trim(pItem->value);
		
		pContext->count++;
		pItem++;
	}

	return result;
}

void iniFreeItems(IniItemContext *pContext)
{
	if (pContext != NULL && pContext->items != NULL)
	{
		free(pContext->items);
		pContext->items = NULL;
		pContext->count = 0;
	}
}

char *iniGetStrValue(const char *szName, IniItemContext *pContext)
{
	IniItemInfo targetItem;
	void *pResult;
	
	if (pContext->count <= 0)
	{
		return NULL;
	}
	
	snprintf(targetItem.name, sizeof(targetItem.name), "%s", szName);
	pResult = bsearch(&targetItem, pContext->items, pContext->count, \
			sizeof(IniItemInfo), compareByItemName);
	if (pResult == NULL)
	{
		return NULL;
	}
	else
	{
		return ((IniItemInfo *)pResult)->value;
	}
}

int64_t iniGetInt64Value(const char *szName, IniItemContext *pContext, \
			const int64_t nDefaultValue)
{
	char *pValue;
	
	pValue = iniGetStrValue(szName, pContext);
	if (pValue == NULL)
	{
		return nDefaultValue;
	}
	else
	{
		return strtoll(pValue, NULL, 10);
	}
}

int iniGetIntValue(const char *szName, IniItemContext *pContext, \
		const int nDefaultValue)
{
	char *pValue;
	
	pValue = iniGetStrValue(szName, pContext);
	if (pValue == NULL)
	{
		return nDefaultValue;
	}
	else
	{
		return atoi(pValue);
	}
}

double iniGetDoubleValue(const char *szName, IniItemContext *pContext, \
			const double dbDefaultValue)
{
	char *pValue;
	
	pValue = iniGetStrValue(szName, pContext);
	if (pValue == NULL)
	{
		return dbDefaultValue;
	}
	else
	{
		return strtod(pValue, NULL);
	}
}

bool iniGetBoolValue(const char *szName, IniItemContext *pContext, \
		const bool bDefaultValue)
{
	char *pValue;
	
	pValue = iniGetStrValue(szName, pContext);
	if (pValue == NULL)
	{
		return bDefaultValue;
	}
	else
	{
		return  strcasecmp(pValue, "true") == 0 ||
			strcasecmp(pValue, "yes") == 0 ||
			strcasecmp(pValue, "on") == 0 ||
			strcmp(pValue, "1") == 0;
	}
}

int iniGetValues(const char *szName, IniItemContext *pContext, \
			char **szValues, const int max_values)
{
	IniItemInfo targetItem;
	IniItemInfo *pFound;
	IniItemInfo *pItem;
	IniItemInfo *pItemEnd;
	char **ppValues;
	
	if (pContext->count <= 0 || max_values <= 0)
	{
		return 0;
	}
	
	snprintf(targetItem.name, sizeof(targetItem.name), "%s", szName);
	pFound = (IniItemInfo *)bsearch(&targetItem, pContext->items, \
		pContext->count, sizeof(IniItemInfo), compareByItemName);
	if (pFound == NULL)
	{
		return 0;
	}

	ppValues = szValues;
	*ppValues++ = pFound->value;
	for (pItem=pFound-1; pItem>=pContext->items; pItem--)
	{
		if (strcmp(pItem->name, szName) != 0)
		{
			break;
		}

		if (ppValues - szValues < max_values)
		{
			*ppValues++ = pItem->value;
		}
	}

	pItemEnd = pContext->items + pContext->count;
	for (pItem=pFound+1; pItem<pItemEnd; pItem++)
	{
		if (strcmp(pItem->name, szName) != 0)
		{
			break;
		}

		if (ppValues - szValues < max_values)
		{
			*ppValues++ = pItem->value;
		}
	}

	return ppValues - szValues;
}

IniItemInfo *iniGetValuesEx(const char *szName, IniItemContext *pContext, \
		int *nTargetCount)
{
	IniItemInfo targetItem;
	IniItemInfo *pFound;
	IniItemInfo *pItem;
	IniItemInfo *pItemEnd;
	IniItemInfo *pItemStart;
	
	if (pContext->count <= 0)
	{
		*nTargetCount = 0;
		return NULL;
	}
	
	snprintf(targetItem.name, sizeof(targetItem.name), "%s", szName);
	pFound = (IniItemInfo *)bsearch(&targetItem, pContext->items, \
		pContext->count, sizeof(IniItemInfo), compareByItemName);
	if (pFound == NULL)
	{
		*nTargetCount = 0;
		return NULL;
	}

	*nTargetCount = 1;
	for (pItem=pFound-1; pItem>=pContext->items; pItem--)
	{
		if (strcmp(pItem->name, szName) != 0)
		{
			break;
		}

		(*nTargetCount)++;
	}
	pItemStart = pFound - (*nTargetCount) + 1;

	pItemEnd = pContext->items + pContext->count;
	for (pItem=pFound+1; pItem<pItemEnd; pItem++)
	{
		if (strcmp(pItem->name, szName) != 0)
		{
			break;
		}

		(*nTargetCount)++;
	}

	return pItemStart;
}

void iniPrintItems(IniItemContext *pContext)
{
	IniItemInfo *pItem;
	IniItemInfo *pItemEnd;
	int i;

	i = 0;
	pItemEnd = pContext->items + pContext->count;
	for (pItem=pContext->items; pItem<pItemEnd; pItem++)
	{
		printf("%d. %s=%s\n", ++i, pItem->name, pItem->value);	
	}
}

