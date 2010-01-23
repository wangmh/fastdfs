
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
#include "md5.h"
#include "shared_func.h"
#include "mime_file_parser.h"
#include "fdfs_http_shared.h"

int fdfs_http_get_content_type_by_extname(FDFSHTTPParams *pParams, \
	const char *filename, char *content_type, const int content_type_size)
{
	char *pExtName;
	HashData *pHashData;
	int  ext_len;

	*content_type = '\0';
	do
	{
	pExtName = strrchr(filename, '.');
	if (pExtName == NULL)
	{
		/*
		logError("file: "__FILE__", line: %d, " \
			"token_check_fail file: %s does not have " \
			"extension name", __LINE__, \
			filename);
		return ENOENT;
		*/
		break;
	}

	pExtName++;
	ext_len = strlen(pExtName);
	if (ext_len == 0)
	{
		/*
		logError("file: "__FILE__", line: %d, " \
			"token_check_fail file: %s 's " \
			"extension name is empty", __LINE__, \
			filename);
		return EINVAL;
		*/
		break;
	}

	pHashData = hash_find_ex(&pParams->content_type_hash, \
				pExtName, ext_len + 1);
	if (pHashData == NULL)
	{
		/*
		logError("file: "__FILE__", line: %d, " \
			"token_check_fail file: %s 's " \
			"extension name is invalid", __LINE__, \
			filename);
		return EINVAL;
		*/
		break;
	}

	if (pHashData->value_len >= content_type_size)
	{
		logError("file: "__FILE__", line: %d, " \
			"file: %s, extension name 's content type " \
			"is too long", __LINE__, filename);
		return EINVAL;
	}
	memcpy(content_type, pHashData->value, pHashData->value_len);
	} while (0);

	if (*content_type == '\0')
	{
		strcpy(content_type, pParams->default_content_type);
	}

	return 0;
}


int fdfs_http_params_load(IniItemContext *pItemContext, \
		const char *conf_filename, FDFSHTTPParams *pParams)
{
	int result;
	char *mime_types_filename;
	char szMimeFilename[256];
	char *anti_steal_secret_key;
	char *token_check_fail_filename;
	char *default_content_type;
	int def_content_type_len;
	int64_t file_size;

	memset(pParams, 0, sizeof(FDFSHTTPParams));

	pParams->disabled = iniGetBoolValue("http.disabled", \
					pItemContext, false);
	if (pParams->disabled)
	{
		return 0;
	}

	pParams->server_port = iniGetIntValue("http.server_port", \
					pItemContext, 0);
	if (pParams->server_port <= 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"invalid param \"http.server_port\": %d", \
			__LINE__, pParams->server_port);
		return EINVAL;
	}

	mime_types_filename = iniGetStrValue("http.mime_types_filename", \
                                        pItemContext);
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

	default_content_type = iniGetStrValue( \
			"http.default_content_type", \
			pItemContext);
	if (default_content_type == NULL || *default_content_type == '\0')
	{
		logError("file: "__FILE__", line: %d, " \
			"param \"http.default_content_type\" not exist " \
			"or is empty", __LINE__);
		return EINVAL;
	}

	def_content_type_len = strlen(default_content_type);
	if (def_content_type_len >= sizeof(pParams->default_content_type))
	{
		logError("file: "__FILE__", line: %d, " \
			"default content type: %s is too long", \
			__LINE__, default_content_type);
		return EINVAL;
	}
	memcpy(pParams->default_content_type, default_content_type, \
			def_content_type_len);

	pParams->anti_steal_token = iniGetBoolValue( \
				"http.anti_steal.check_token", \
				pItemContext, false);
	if (!pParams->anti_steal_token)
	{
		return 0;
	}

	pParams->token_ttl = iniGetIntValue( \
				"http.anti_steal.token_ttl", \
				pItemContext, 600);
	if (pParams->token_ttl <= 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"param \"http.anti_steal.token_ttl\" is invalid", \
			__LINE__);
		return EINVAL;
	}

	anti_steal_secret_key = iniGetStrValue( \
			"http.anti_steal.secret_key", \
			pItemContext);
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
			pItemContext);
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

	if ((result=fdfs_http_get_content_type_by_extname(pParams, \
			token_check_fail_filename, \
			pParams->token_check_fail_content_type, \
			sizeof(pParams->token_check_fail_content_type))) != 0)
	{
		return result;
	}

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

int fdfs_http_gen_token(const BufferInfo *secret_key, const char *file_id, \
		const int timestamp, char *token)
{
	char buff[256 + 64];
	unsigned char digit[16];
	int id_len;
	int total_len;

	id_len = strlen(file_id);
	if (id_len + secret_key->length + 12 > sizeof(buff))
	{
		return ENOSPC;
	}

	memcpy(buff, file_id, id_len);
	total_len = id_len;
	memcpy(buff+total_len, secret_key->buff, secret_key->length);
	total_len += secret_key->length;
	total_len += sprintf(buff+total_len, "%d", timestamp);

	MD5Buffer(buff, total_len, digit);
	bin2hex((char *)digit, 16, token);
	return 0;
}

int fdfs_http_check_token(const BufferInfo *secret_key, const char *file_id, \
		const int timestamp, const char *token, const int ttl)
{
	char true_token[33];
	int result;
	int token_len;

	token_len = strlen(token);
	if (token_len != 32)
	{
		return EINVAL;
	}

	if (time(NULL) - timestamp > ttl)
	{
		return ETIMEDOUT;
	}

	if ((result=fdfs_http_gen_token(secret_key, file_id, \
			timestamp, true_token)) != 0)
	{
		return result;
	}

	return memcmp(token, true_token, 32);
}

char *fdfs_http_get_parameter(const char *param_name, KeyValuePair *params, \
		const int param_count)
{
	KeyValuePair *pCurrent;
	KeyValuePair *pEnd;

	pEnd = params + param_count;
	for (pCurrent=params; pCurrent<pEnd; pCurrent++)
	{
		if (strcmp(pCurrent->key, param_name) == 0)
		{
			return pCurrent->value;
		}
	}

	return NULL;
}

