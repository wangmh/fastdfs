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
#include "ini_file_reader.h"

#define _LINE_BUFFER_SIZE	512
#define _ALLOC_ITEMS_ONCE	8

static int iniDoLoadItems(const char *szFilename, IniItemInfo **ppItems, \
		int *nItemCount, int *nAllocItems);

int compareByItemName(const void *p1, const void *p2)
{
	return strcmp(((IniItemInfo *)p1)->name, ((IniItemInfo *)p2)->name);
}

int iniLoadItems(const char *szFilename, IniItemInfo **ppItems, int *nItemCount)
{
	int alloc_items;
	int result;
	char *pLast;
	char old_cwd[MAX_PATH_SIZE];

	memset(old_cwd, 0, sizeof(old_cwd));
	pLast = strrchr(szFilename, '/');
	if (pLast != NULL)
	{
		char path[256];
		int len;

		if (getcwd(old_cwd, sizeof(old_cwd)) == NULL)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"getcwd fail, errno: %d, error info: %s", \
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
				"chdir to the path of conf file: %s fail, " \
				"errno: %d, error info: %s", \
				__LINE__, szFilename, errno, strerror(errno));
			return errno != 0 ? errno : ENOENT;
		}
	}

	*nItemCount = 0;
	alloc_items = _ALLOC_ITEMS_ONCE;
	*ppItems = (IniItemInfo *)malloc(sizeof(IniItemInfo) * alloc_items);
	if (*ppItems == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail", __LINE__, \
			sizeof(IniItemInfo) * alloc_items);
		return errno != 0 ? errno : ENOMEM;
	}

	memset(*ppItems, 0, sizeof(IniItemInfo) * alloc_items);
	result = iniDoLoadItems(szFilename, ppItems, nItemCount, &alloc_items);
	if (result != 0)
	{
		if (*ppItems != NULL)
		{
			free(*ppItems);
			*ppItems = NULL;
		}
		*nItemCount = 0;
	}
	else
	{
		qsort(*ppItems, *nItemCount, sizeof(IniItemInfo), \
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

static int iniDoLoadItems(const char *szFilename, IniItemInfo **ppItems, \
		int *nItemCount, int *nAllocItems)
{
	FILE *fp;
	IniItemInfo *pItem;
	char szLineBuff[_LINE_BUFFER_SIZE + 1];
	char *pEqualChar;
	char *pIncludeFilename;
	int nNameLen;
	int nValueLen;
	int result;

	if ((fp = fopen(szFilename, "r")) == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"open conf file \"%s\" fail, " \
			"errno: %d, error info: %s",  \
			__LINE__, szFilename, errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}
	
	memset(szLineBuff, 0, sizeof(szLineBuff));

	result = 0;
	pItem = *ppItems + (*nItemCount);
	while (1)
	{
		if (fgets(szLineBuff, _LINE_BUFFER_SIZE, fp) == NULL)
		{
			break;
		}

		if (*szLineBuff == '#' && \
			strncasecmp(szLineBuff+1, "include", 7) == 0 && \
			(*(szLineBuff+8) == ' ' || *(szLineBuff+8) == '\t'))
		{
			pIncludeFilename = strdup(szLineBuff + 9);
			if (pIncludeFilename == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"strdup %d bytes fail", __LINE__, \
					strlen(szLineBuff + 9) + 1);
				result = errno != 0 ? errno : ENOMEM;
				break;
			}

			trim(pIncludeFilename);
			if (fileExists(pIncludeFilename))
			{
				result = iniDoLoadItems(pIncludeFilename, \
					ppItems, nItemCount, nAllocItems);
				if (result != 0)
				{
					free(pIncludeFilename);
					break;
				}
			}
			else
			{
				logError("file: "__FILE__", line: %d, " \
					"include file \"%s\" not exists, " \
					"line: \"%s\"", __LINE__, \
					pIncludeFilename, szLineBuff);
				free(pIncludeFilename);
				result = ENOENT;
				break;
			}

			pItem = (*ppItems) + (*nItemCount);  //must re-asign
			free(pIncludeFilename);
			continue;
		}

		trim(szLineBuff);
		if (szLineBuff[0] == '#' || szLineBuff[0] == '\0')
		{
			continue;
		}
		
		pEqualChar = strchr(szLineBuff, '=');
		if (pEqualChar == NULL)
		{
			continue;
		}
		
		nNameLen = pEqualChar - szLineBuff;
		nValueLen = strlen(szLineBuff) - (nNameLen + 1);
		if (nNameLen > INI_ITEM_NAME_LEN)
		{
			nNameLen = INI_ITEM_NAME_LEN;
		}
		
		if (nValueLen > INI_ITEM_VALUE_LEN)
		{
			nValueLen = INI_ITEM_VALUE_LEN;
		}
	
		if (*nItemCount >= *nAllocItems)
		{
			(*nAllocItems) += _ALLOC_ITEMS_ONCE;
			*ppItems = (IniItemInfo *)realloc(*ppItems, 
				sizeof(IniItemInfo) * (*nAllocItems));
			if (*ppItems == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"realloc %d bytes fail", __LINE__, \
					sizeof(IniItemInfo) * (*nAllocItems));
				result = errno != 0 ? errno : ENOMEM;
				break;
			}

			pItem = (*ppItems) + (*nItemCount);
			memset(pItem, 0, sizeof(IniItemInfo) * \
				((*nAllocItems) - (*nItemCount)));
		}

		memcpy(pItem->name, szLineBuff, nNameLen);
		memcpy(pItem->value, pEqualChar + 1, nValueLen);
		
		trim(pItem->name);
		trim(pItem->value);
		
		(*nItemCount)++;
		pItem++;
	}
	
	fclose(fp);
	
	return result;
}

void iniFreeItems(IniItemInfo *items)
{
	if (items != NULL)
	{
		free(items);
	}
}

char *iniGetStrValue(const char *szName, IniItemInfo *items, \
			const int nItemCount)
{
	IniItemInfo targetItem;
	void *pResult;
	
	if (nItemCount <= 0)
	{
		return NULL;
	}
	
	snprintf(targetItem.name, sizeof(targetItem.name), "%s", szName);
	pResult = bsearch(&targetItem, items, nItemCount, \
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

int64_t iniGetInt64Value(const char *szName, IniItemInfo *items, \
			const int nItemCount, const int64_t nDefaultValue)
{
	char *pValue;
	
	pValue = iniGetStrValue(szName, items, nItemCount);
	if (pValue == NULL)
	{
		return nDefaultValue;
	}
	else
	{
		return strtoll(pValue, NULL, 10);
	}
}

int iniGetIntValue(const char *szName, IniItemInfo *items, \
		const int nItemCount, const int nDefaultValue)
{
	char *pValue;
	
	pValue = iniGetStrValue(szName, items, nItemCount);
	if (pValue == NULL)
	{
		return nDefaultValue;
	}
	else
	{
		return atoi(pValue);
	}
}

bool iniGetBoolValue(const char *szName, IniItemInfo *items, \
		const int nItemCount)
{
	char *pValue;
	
	pValue = iniGetStrValue(szName, items, nItemCount);
	if (pValue == NULL)
	{
		return false;
	}
	else
	{
		return  strcasecmp(pValue, "true") == 0 ||
			strcasecmp(pValue, "yes") == 0 ||
			strcasecmp(pValue, "on") == 0 ||
			strcmp(pValue, "1") == 0;
	}
}

int iniGetValues(const char *szName, IniItemInfo *items, const int nItemCount, \
			char **szValues, const int max_values)
{
	IniItemInfo targetItem;
	IniItemInfo *pFound;
	IniItemInfo *pItem;
	IniItemInfo *pItemEnd;
	char **ppValues;
	
	if (nItemCount <= 0 || max_values <= 0)
	{
		return 0;
	}
	
	snprintf(targetItem.name, sizeof(targetItem.name), "%s", szName);
	pFound = (IniItemInfo *)bsearch(&targetItem, items, nItemCount, \
				sizeof(IniItemInfo), compareByItemName);
	if (pFound == NULL)
	{
		return 0;
	}

	ppValues = szValues;
	*ppValues++ = pFound->value;
	for (pItem=pFound-1; pItem>=items; pItem--)
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

	pItemEnd = items + nItemCount;
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

IniItemInfo *iniGetValuesEx(const char *szName, IniItemInfo *items, 
		const int nItemCount, int *nTargetCount)
{
	IniItemInfo targetItem;
	IniItemInfo *pFound;
	IniItemInfo *pItem;
	IniItemInfo *pItemEnd;
	IniItemInfo *pItemStart;
	
	if (nItemCount <= 0)
	{
		*nTargetCount = 0;
		return NULL;
	}
	
	snprintf(targetItem.name, sizeof(targetItem.name), "%s", szName);
	pFound = (IniItemInfo *)bsearch(&targetItem, items, nItemCount, \
				sizeof(IniItemInfo), compareByItemName);
	if (pFound == NULL)
	{
		*nTargetCount = 0;
		return NULL;
	}

	*nTargetCount = 1;
	for (pItem=pFound-1; pItem>=items; pItem--)
	{
		if (strcmp(pItem->name, szName) != 0)
		{
			break;
		}

		(*nTargetCount)++;
	}
	pItemStart = pFound - (*nTargetCount) + 1;

	pItemEnd = items + nItemCount;
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

void iniPrintItems(IniItemInfo *items, const int nItemCount)
{
	IniItemInfo *pItem;
	IniItemInfo *pItemEnd;
	int i;

	i = 0;
	pItemEnd = items + nItemCount;
	for (pItem=items; pItem<pItemEnd; pItem++)
	{
		printf("%d. %s=%s\n", ++i, pItem->name, pItem->value);	
	}
}

