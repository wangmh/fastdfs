/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#ifndef _FDFS_HTTP_SHARED_H
#define _FDFS_HTTP_SHARED_H

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include "ini_file_reader.h"
#include "hash.h"

typedef struct 
{
	bool disabled;
	bool anti_steal_token;
	int server_port;

	/* key is file ext name, value is content type */
	HashArray content_type_hash;

	BufferInfo anti_steal_secret_key;
	char token_check_fail_content_type[64];
	BufferInfo token_check_fail_buff;
} FDFSHTTPParams;

#ifdef __cplusplus
extern "C" {
#endif

/**
load HTTP params from conf file
params:
	items: the ini file items, return by iniLoadItems
	nItemCount: item count, return by iniLoadItems
	conf_filename: config filename
	pHTTPParams: return the params
return: 0 for success, != 0 fail
**/
int fdfs_http_params_load(IniItemInfo *items, const int nItemCount, \
		const char *conf_filename, FDFSHTTPParams *pHTTPParams);

void fdfs_http_params_destroy(FDFSHTTPParams *pParams);

#ifdef __cplusplus
}
#endif

#endif

