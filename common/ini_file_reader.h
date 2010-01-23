/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//ini_file_reader.h
#ifndef INI_FILE_READER_H
#define INI_FILE_READER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "common_define.h"

#define INI_ITEM_NAME_LEN		64
#define INI_ITEM_VALUE_LEN		128

typedef struct
{
	char name[INI_ITEM_NAME_LEN + 1];
	char value[INI_ITEM_VALUE_LEN + 1];
} IniItemInfo;

typedef struct
{
	IniItemInfo *items;
	int count;
} IniItemContext;

#ifdef __cplusplus
extern "C" {
#endif

int iniLoadItems(const char *szFilename, IniItemContext *pContext);
int iniLoadItemsFromBuffer(char *content, IniItemContext *pContext);

void iniFreeItems(IniItemContext *pContext);

char *iniGetStrValue(const char *szName, IniItemContext *pContext);
int iniGetValues(const char *szName, IniItemContext *pContext, \
			char **szValues, const int max_values);

int iniGetIntValue(const char *szName, IniItemContext *pContext, \
			const int nDefaultValue);
IniItemInfo *iniGetValuesEx(const char *szName, IniItemContext *pContext, \
		int *nTargetCount);

int64_t iniGetInt64Value(const char *szName, IniItemContext *pContext, \
			const int64_t nDefaultValue);
bool iniGetBoolValue(const char *szName, IniItemContext *pContext, \
		const bool bDefaultValue);
double iniGetDoubleValue(const char *szName, IniItemContext *pContext, \
			const double dbDefaultValue);

void iniPrintItems(IniItemContext *pContext);

#ifdef __cplusplus
}
#endif

#endif

