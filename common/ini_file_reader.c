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
#include "ini_file_reader.h"

#define _LINE_BUFFER_SIZE	512
#define _ALLOC_ITEMS_ONCE	  8

int compareByItemName(const void *p1, const void *p2)
{
	return strcmp(((IniItemInfo *)p1)->name, ((IniItemInfo *)p2)->name);
}

int iniLoadItems(const char *szFilename, IniItemInfo **ppItems, int *nItemCount)
{
	FILE *fp;
	IniItemInfo *items;
	IniItemInfo *pItem;
	int alloc_items;
	char szLineBuff[_LINE_BUFFER_SIZE + 1];
	char *pEqualChar;
	int nNameLen;
	int nValueLen;

	alloc_items = _ALLOC_ITEMS_ONCE; 
	items = (IniItemInfo *)malloc(sizeof(IniItemInfo) * alloc_items);
	if (items == NULL)
	{
		*nItemCount = -1;
		*ppItems = NULL;
		return errno != 0 ? errno : ENOMEM;
	}
	
	if ((fp = fopen(szFilename, "r")) == NULL)
	{
		free(items);
		*nItemCount = -1;
		*ppItems = NULL;
		return errno != 0 ? errno : ENOENT;
	}
	
	memset(items, 0, sizeof(IniItemInfo) * alloc_items);
	memset(szLineBuff, 0, sizeof(szLineBuff));

	pItem = items;
	*nItemCount = 0;
	while (1)
	{
		if (fgets(szLineBuff, _LINE_BUFFER_SIZE, fp) == NULL)
		{
			break;
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
	
		if (*nItemCount >= alloc_items)
		{
			alloc_items += _ALLOC_ITEMS_ONCE;
			items = (IniItemInfo *)realloc(items, 
				sizeof(IniItemInfo) * alloc_items);
			if (items == NULL)
			{
				fclose(fp);
				*nItemCount = -1;
				*ppItems = NULL;
				return errno != 0 ? errno : ENOMEM;
			}

			pItem = items + (*nItemCount);
			memset(pItem, 0, sizeof(IniItemInfo) * \
				(alloc_items - (*nItemCount)));
		}

		memcpy(pItem->name, szLineBuff, nNameLen);
		memcpy(pItem->value, pEqualChar + 1, nValueLen);
		
		trim(pItem->name);
		trim(pItem->value);
		
		(*nItemCount)++;
		pItem++;
	}
	
	fclose(fp);
	
	qsort(items, *nItemCount, sizeof(IniItemInfo), compareByItemName);

	*ppItems = items;
	return 0;
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

