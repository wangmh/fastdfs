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
#include "hash.h"

#define INI_ITEM_NAME_LEN		64
#define INI_ITEM_VALUE_LEN		128

typedef struct
{
	char name[INI_ITEM_NAME_LEN + 1];
	char value[INI_ITEM_VALUE_LEN + 1];
} IniItem;

typedef struct
{
	IniItem *items;
	int count;  //item count
	int alloc_count;
} IniSection;

typedef struct
{
	IniSection global;
	HashArray sections;
	IniSection *current_section; //for load from ini file
} IniContext;

#ifdef __cplusplus
extern "C" {
#endif

int iniLoadFromFile(const char *szFilename, IniContext *pContext);
int iniLoadFromBuffer(char *content, IniContext *pContext);

void iniFreeContext(IniContext *pContext);

char *iniGetStrValue(const char *szSectionName, const char *szItemName, \
		IniContext *pContext);
int iniGetValues(const char *szSectionName, const char *szItemName, \
		IniContext *pContext, char **szValues, const int max_values);

int iniGetIntValue(const char *szSectionName, const char *szItemName, \
		IniContext *pContext, const int nDefaultValue);
IniItem *iniGetValuesEx(const char *szSectionName, const char *szItemName, \
		IniContext *pContext, int *nTargetCount);

int64_t iniGetInt64Value(const char *szSectionName, const char *szItemName, \
		IniContext *pContext, const int64_t nDefaultValue);
bool iniGetBoolValue(const char *szSectionName, const char *szItemName, \
		IniContext *pContext, const bool bDefaultValue);
double iniGetDoubleValue(const char *szSectionName, const char *szItemName, \
		IniContext *pContext, const double dbDefaultValue);

void iniPrintItems(IniContext *pContext);

#ifdef __cplusplus
}
#endif

#endif

