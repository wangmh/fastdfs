/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdht_func.c

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "logger.h"
#include "sockopt.h"
#include "shared_func.h"
#include "ini_file_reader.h"
#include "fdht_proto.h"
#include "fdht_func.h"

int fdht_split_ids(const char *szIds, int **ppIds, int *id_count)
{
	char *pBuff;
	char *pNumStart;
	char *p;
	int alloc_count;
	int result;
	int count;
	int i;
	char ch;
	char *pNumStart1;
	char *pNumStart2;
	int nNumLen1;
	int nNumLen2;
	int nStart;
	int nEnd;

	alloc_count = getOccurCount(szIds, ',') + 1;
	*id_count = 0;
	*ppIds = (int *)malloc(sizeof(int) * alloc_count);
	if (*ppIds == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s.", \
			__LINE__, sizeof(int) * alloc_count, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	pBuff = strdup(szIds);
	if (pBuff == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"strdup \"%s\" fail, errno: %d, error info: %s.", \
			__LINE__, szIds, \
			errno, strerror(errno));
		free(*ppIds);
		*ppIds = NULL;
		return errno != 0 ? errno : ENOMEM;
	}

	result = 0;
	p = pBuff;
	while (*p != '\0')
	{
		while (*p == ' ' || *p == '\t') //trim prior spaces
		{
			p++;
		}

		if (*p == '\0')
		{
			break;
		}

		if (*p >= '0' && *p <= '9')
		{
			pNumStart = p;
			p++;
			while (*p >= '0' && *p <= '9')
			{
				p++;
			}

			if (p - pNumStart == 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"invalid group ids \"%s\", " \
					"which contains empty group id!", \
					__LINE__, szIds);
				result = EINVAL;
				break;
			}

			ch = *p;
			if (!(ch == ','  || ch == '\0'))
			{
				logError("file: "__FILE__", line: %d, " \
					"invalid group ids \"%s\", which contains " \
					"invalid char: %c(0x%02X)! remain string: %s", \
					__LINE__, szIds, *p, *p, p);
				result = EINVAL;
				break;
			}

			*p = '\0';
			(*ppIds)[*id_count] = atoi(pNumStart);
			(*id_count)++;
	
			if (ch == '\0')
			{
				break;
			}

			p++;  //skip ,
			continue;
		}

		if (*p != '[')
		{
			logError("file: "__FILE__", line: %d, " \
				"invalid group ids \"%s\", which contains " \
				"invalid char: %c(0x%02X)! remain string: %s", \
				__LINE__, szIds, *p, *p, p);
			result = EINVAL;
			break;
		}

		p++;  //skip [
		while (*p == ' ' || *p == '\t') //trim prior spaces
		{
			p++;
		}

		pNumStart1 = p;
		while (*p >='0' && *p <= '9')
		{
			p++;
		}

		nNumLen1 = p - pNumStart1;
		if (nNumLen1 == 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"invalid group ids: %s, " \
				"empty entry before char %c(0x%02X), " \
				"remain string: %s", \
				__LINE__, szIds, *p, *p, p);
			result = EINVAL;
			break;
		}

		while (*p == ' ' || *p == '\t') //trim spaces
		{
			p++;
		}

		if (*p != '-')
		{
			logError("file: "__FILE__", line: %d, " \
				"expect \"-\", but char %c(0x%02X) ocurs " \
				"in group ids: %s, remain string: %s",\
				__LINE__, *p, *p, szIds, p);
			result = EINVAL;
			break;
		}

		*(pNumStart1 + nNumLen1) = '\0';
		nStart = atoi(pNumStart1);

		p++;  //skip -
		while (*p == ' ' || *p == '\t') //trim spaces
		{
			p++;
		}

		pNumStart2 = p;
		while (*p >='0' && *p <= '9')
		{
			p++;
		}

		nNumLen2 = p - pNumStart2;
		if (nNumLen2 == 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"invalid group ids: %s, " \
				"empty entry before char %c(0x%02X)", \
				__LINE__, szIds, *p, *p);
			result = EINVAL;
			break;
		}

		/* trim tail spaces */
		while (*p == ' ' || *p == '\t')
		{
			p++;
		}

		if (*p != ']')
		{
			logError("file: "__FILE__", line: %d, " \
				"expect \"]\", but char %c(0x%02X) ocurs " \
				"in group ids: %s",\
				__LINE__, *p, *p, szIds);
			result = EINVAL;
			break;
		}

		*(pNumStart2 + nNumLen2) = '\0';
		nEnd = atoi(pNumStart2);

		count = nEnd - nStart;
		if (count < 0)
		{
			count = 0;
		}
		if (alloc_count < *id_count + (count + 1))
		{
			alloc_count += count + 1;
			*ppIds = (int *)realloc(*ppIds, \
				sizeof(int) * alloc_count);
			if (*ppIds == NULL)
			{
				result = errno != 0 ? errno : ENOMEM;
				logError("file: "__FILE__", line: %d, "\
					"malloc %d bytes fail, " \
					"errno: %d, error info: %s.", \
					__LINE__, \
					sizeof(int) * alloc_count,\
					result, strerror(result));

				break;
			}
		}

		for (i=nStart; i<=nEnd; i++)
		{
			(*ppIds)[*id_count] = i;
			(*id_count)++;
		}

		p++; //skip ]
		/* trim spaces */
		while (*p == ' ' || *p == '\t')
		{
			p++;
		}
		if (*p == ',')
		{
			p++; //skip ,
		}
	}

	free(pBuff);

	if (result == 0 && *id_count == 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"invalid group ids count: 0!", __LINE__);
		result = EINVAL;
	}

	if (result != 0)
	{
		*id_count = 0;
		free(*ppIds);
		*ppIds = NULL;
	}

	printf("*id_count=%d\n", *id_count);
	for (i=0; i<*id_count; i++)
	{
		printf("%d\n", (*ppIds)[i]);
	}

	return result;
}

static int fdht_cmp_by_ip_and_port(const void *p1, const void *p2)
{
	int res;

	res = strcmp(((FDHTServerInfo*)p1)->ip_addr, \
			((FDHTServerInfo*)p2)->ip_addr);
	if (res != 0)
	{
		return res;
	}

	return ((FDHTServerInfo*)p1)->port - \
			((FDHTServerInfo*)p2)->port;
}

int fdht_load_groups(IniItemInfo *items, const int nItemCount, \
		GroupArray *pGroupArray)
{
	IniItemInfo *pItemInfo;
	IniItemInfo *pItemEnd;
	int group_id;
	char item_name[32];
	ServerArray *pServerArray;
	FDHTServerInfo *pServerInfo;
	FDHTServerInfo *pServerEnd;
	char *ip_port[2];

	pGroupArray->count = iniGetIntValue("group_count", \
			items, nItemCount, 0);
	if (pGroupArray->count <= 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"invalid group count: %d <= 0!", \
			__LINE__, pGroupArray->count);
		return EINVAL;
	}

	pGroupArray->groups = (ServerArray *)malloc(sizeof(ServerArray) * \
					pGroupArray->count);
	if (pGroupArray->groups == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s", \
			__LINE__, sizeof(ServerArray) * pGroupArray->count, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	pServerArray = pGroupArray->groups;
	for (group_id=0; group_id<pGroupArray->count; group_id++)
	{
		sprintf(item_name, "group%d", group_id);
		pItemInfo = iniGetValuesEx(item_name, items, \
					nItemCount, &(pServerArray->count));
		if (pItemInfo == NULL || pServerArray->count <= 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"group %d not exist!", __LINE__, group_id);
			return ENOENT;
		}

		pServerArray->servers = (FDHTServerInfo *)malloc( \
			sizeof(FDHTServerInfo) * pServerArray->count);
		if (pServerArray->servers == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", __LINE__, \
				sizeof(FDHTServerInfo) * pServerArray->count, \
				errno, strerror(errno));
			return errno != 0 ? errno : ENOMEM;
		}

		memset(pServerArray->servers, 0, sizeof(FDHTServerInfo) * \
			pServerArray->count);

		pServerInfo = pServerArray->servers;
		pItemEnd = pItemInfo + pServerArray->count;
		for (; pItemInfo<pItemEnd; pItemInfo++)
		{
			if (splitEx(pItemInfo->value, ':', ip_port, 2) != 2)
			{
				logError("file: "__FILE__", line: %d, " \
					"\"%s\" 's value \"%s\" is invalid, "\
					"correct format is hostname:port", \
					__LINE__, item_name, pItemInfo->value);
				return EINVAL;
			}

			if (getIpaddrByName(ip_port[0], pServerInfo->ip_addr, \
				sizeof(pServerInfo->ip_addr)) == INADDR_NONE)
			{
				logError("file: "__FILE__", line: %d, " \
					"\"%s\" 's value \"%s\" is invalid, "\
					"invalid hostname: %s", \
					__LINE__, item_name, \
					pItemInfo->value, ip_port[0]);
				return EINVAL;
			}

			if (strcmp(pServerInfo->ip_addr, "127.0.0.1") == 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"group%d: invalid hostname \"%s\", " \
					"ip address can not be 127.0.0.1!", \
					__LINE__, group_id, pItemInfo->value);
				return EINVAL;
			}

			pServerInfo->port = atoi(ip_port[1]);
			if (pServerInfo->port <= 0 || pServerInfo->port > 65535)
			{
				logError("file: "__FILE__", line: %d, " \
					"\"%s\" 's value \"%s\" is invalid, "\
					"invalid port: %d", \
					__LINE__, item_name, \
					pItemInfo->value, pServerInfo->port);
				return EINVAL;
			}
			pServerInfo->sock = -1;

			/*
			logDebug("group%d. %s:%d", group_id, \
				pServerInfo->ip_addr, pServerInfo->port);
			*/

			pServerInfo++;
		}

		qsort(pServerArray->servers, pServerArray->count, \
			sizeof(FDHTServerInfo), fdht_cmp_by_ip_and_port);
		pServerEnd = pServerArray->servers + pServerArray->count;
		for (pServerInfo=pServerArray->servers + 1; \
			pServerInfo<pServerEnd; pServerInfo++)
		{
			if (fdht_cmp_by_ip_and_port(pServerInfo-1, \
				pServerInfo) == 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"group: \"%s\",  duplicate server: " \
					"%s:%d", __LINE__, item_name, \
					pServerInfo->ip_addr,pServerInfo->port);
				return EINVAL;
			}
		}

		/*
		for (pServerInfo=pServerArray->servers; \
			pServerInfo<pServerEnd; pServerInfo++)
		{
			logDebug("group%d. %s:%d", group_id, \
				pServerInfo->ip_addr, pServerInfo->port);
		}
		*/

		pServerArray++;
	}

	return 0;
}

void fdht_free_group_array(GroupArray *pGroupArray)
{
	ServerArray *pServerArray;
	ServerArray *pArrayEnd;
	FDHTServerInfo *pServerInfo;
	FDHTServerInfo *pServerEnd;

	if (pGroupArray->groups != NULL)
	{
		pArrayEnd = pGroupArray->groups + pGroupArray->count;
		for (pServerArray=pGroupArray->groups; pServerArray<pArrayEnd;
			 pServerArray++)
		{
			if (pServerArray->servers == NULL)
			{
				continue;
			}

			pServerEnd = pServerArray->servers+pServerArray->count;
			for (pServerInfo=pServerArray->servers; \
				pServerInfo<pServerEnd; pServerInfo++)
			{
				if (pServerInfo->sock > 0)
				{
					close(pServerInfo->sock);
					pServerInfo->sock = -1;
				}
			}

			free(pServerArray->servers);
			pServerArray->servers = NULL;
		}

		free(pGroupArray->groups);
		pGroupArray->groups = NULL;
	}
}

int fdht_connect_all_servers(GroupArray *pGroupArray, const bool bNoDelay, \
			int *success_count, int *fail_count)
{
	ServerArray *pServerArray;
	ServerArray *pArrayEnd;
	FDHTServerInfo *pServerInfo;
	FDHTServerInfo *pServerEnd;
	int conn_result;
	int result;

	*success_count = 0;
	*fail_count = 0;
	if (pGroupArray->groups == NULL)
	{
		return ENOENT;
	}

	result = 0;
	pArrayEnd = pGroupArray->groups + pGroupArray->count;
	for (pServerArray=pGroupArray->groups; pServerArray<pArrayEnd;
		 pServerArray++)
	{
		if (pServerArray->servers == NULL)
		{
			continue;
		}

		pServerEnd = pServerArray->servers+pServerArray->count;
		for (pServerInfo=pServerArray->servers; \
			pServerInfo<pServerEnd; pServerInfo++)
		{
			if ((conn_result=fdht_connect_server(pServerInfo)) != 0)
			{
				result = conn_result;
				(*fail_count)++;
			}
			else
			{
				(*success_count)++;
				if (bNoDelay)
				{
					tcpsetnodelay(pServerInfo->sock);
				}
			}
		}
	}

	if (result != 0)
	{
		return result;
	}
	else
	{
		return  *success_count > 0 ? 0: ENOENT;
	}
}

void fdht_disconnect_all_servers(GroupArray *pGroupArray)
{
	ServerArray *pServerArray;
	ServerArray *pArrayEnd;
	FDHTServerInfo *pServerInfo;
	FDHTServerInfo *pServerEnd;

	if (pGroupArray->groups != NULL)
	{
		pArrayEnd = pGroupArray->groups + pGroupArray->count;
		for (pServerArray=pGroupArray->groups; pServerArray<pArrayEnd;
			 pServerArray++)
		{
			if (pServerArray->servers == NULL)
			{
				continue;
			}

			pServerEnd = pServerArray->servers+pServerArray->count;
			for (pServerInfo=pServerArray->servers; \
				pServerInfo<pServerEnd; pServerInfo++)
			{
				if (pServerInfo->sock > 0)
				{
					close(pServerInfo->sock);
					pServerInfo->sock = -1;
				}
			}
		}
	}
}

