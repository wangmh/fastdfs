
/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <fcntl.h>
#include "logger.h"
#include "shared_func.h"
#include "mime_file_parser.h"
#include "fdfs_http_shared.h"

int fdfs_http_params_load(IniItemInfo *items, const int nItemCount, \
		const char *conf_filename, FDFSHTTPParams *pParams)
{
	int result;
	char *mime_types_filename;
	char szMimeFilename[256];
	char *anti_steal_secret_key;
	char *token_check_fail_filename;
	char *pExtName;
	HashData *pHashData;
	int  ext_len;
	off_t file_size;

	memset(pParams, 0, sizeof(FDFSHTTPParams));

	pParams->disabled = iniGetBoolValue("http.disabled", \
					items, nItemCount, false);
	if (pParams->disabled)
	{
		return 0;
	}

	pParams->server_port = iniGetIntValue("http.server_port", \
					items, nItemCount, 0);
	if (pParams->server_port <= 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"invalid param \"http.server_port\": %d", \
			__LINE__, pParams->server_port);
		return EINVAL;
	}

	mime_types_filename = iniGetStrValue("http.mime_types_filename", \
                                        items, nItemCount);
	if (mime_types_filename == NULL || *mime_types_filename == '\0')
	{
		logError("file: "__FILE__", line: %d, " \
			"param \"http.mime_types_filename\" not exist " \
			"or is empty", __LINE__);
		return EINVAL;
	}

	if (strncasecmp(mime_types_filename, "http://", 7) != 0 && \
		*mime_types_filename != '/' && \
		strncasecmp(conf_filename, "http://", 7) != 0)
	{
		char *pPathEnd;

		pPathEnd = strrchr(conf_filename, '/');
		if (pPathEnd == NULL)
		{
			snprintf(szMimeFilename, sizeof(szMimeFilename), \
					"%s", mime_types_filename);
		}
		else
		{
			int nPathLen;
			int nFilenameLen;

			nPathLen = (pPathEnd - conf_filename) + 1;
			nFilenameLen = strlen(mime_types_filename);
			if (nPathLen + nFilenameLen >= sizeof(szMimeFilename))
			{
				logError("file: "__FILE__", line: %d, " \
					"filename is too long, length %d >= %d",
					__LINE__, nPathLen + nFilenameLen, \
					(int)sizeof(szMimeFilename));
				return ENOSPC;
			}

			memcpy(szMimeFilename, conf_filename, nPathLen);
			memcpy(szMimeFilename + nPathLen, mime_types_filename, \
				nFilenameLen);
			*(szMimeFilename + nPathLen + nFilenameLen) = '\0';
		}
	}
	else
	{
		snprintf(szMimeFilename, sizeof(szMimeFilename), \
				"%s", mime_types_filename);
	}

	result = load_mime_types_from_file(&pParams->content_type_hash, \
				szMimeFilename);
	if (result != 0)
	{
		return result;
	}

	pParams->anti_steal_token = iniGetBoolValue( \
				"http.anti_steal.check_token", \
				items, nItemCount, false);
	if (!pParams->anti_steal_token)
	{
		return 0;
	}

	anti_steal_secret_key = iniGetStrValue( \
			"http.anti_steal.secret_key", \
			items, nItemCount);
	if (anti_steal_secret_key == NULL || *anti_steal_secret_key == '\0')
	{
		logError("file: "__FILE__", line: %d, " \
			"param \"http.anti_steal.secret_key\" not exist " \
			"or is empty", __LINE__);
		return EINVAL;
	}

	buffer_strcpy(&pParams->anti_steal_secret_key, anti_steal_secret_key);

	token_check_fail_filename = iniGetStrValue( \
			"http.anti_steal.token_check_fail", \
			items, nItemCount);
	if (token_check_fail_filename == NULL || \
		*token_check_fail_filename == '\0')
	{
		return 0;
	}

	if (!fileExists(token_check_fail_filename))
	{
		logError("file: "__FILE__", line: %d, " \
			"token_check_fail file: %s not exists", __LINE__, \
			token_check_fail_filename);
		return ENOENT;
	}

	pExtName = strrchr(token_check_fail_filename, '.');
	if (pExtName == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"token_check_fail file: %s does not have " \
			"extension name", __LINE__, \
			token_check_fail_filename);
		return ENOENT;
	}

	pExtName++;
	ext_len = strlen(pExtName);
	if (ext_len == 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"token_check_fail file: %s 's " \
			"extension name is empty", __LINE__, \
			token_check_fail_filename);
		return EINVAL;
	}

	pHashData = hash_find_ex(&pParams->content_type_hash, \
				pExtName, ext_len + 1);
	if (pHashData == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"token_check_fail file: %s 's " \
			"extension name is invalid", __LINE__, \
			token_check_fail_filename);
		return EINVAL;
	}

	if (pHashData->value_len >= sizeof( \
			pParams->token_check_fail_content_type))
	{
		logError("file: "__FILE__", line: %d, " \
			"token_check_fail file: %s, " \
			"extension name 's content type is invalid", \
			__LINE__, token_check_fail_filename);
		return EINVAL;
	}
	memcpy(pParams->token_check_fail_content_type, pHashData->value, \
			pHashData->value_len);

	if ((result=getFileContent(token_check_fail_filename, \
		&pParams->token_check_fail_buff.buff, &file_size)) != 0)
	{
		return result;
	}

	pParams->token_check_fail_buff.alloc_size = file_size;
	pParams->token_check_fail_buff.length = file_size;

	return 0;
}

void fdfs_http_params_destroy(FDFSHTTPParams *pParams)
{
	hash_destroy(&pParams->content_type_hash);
}

