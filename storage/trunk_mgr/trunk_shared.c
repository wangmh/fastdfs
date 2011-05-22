/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//trunk_shared.c

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
#include "shared_func.h"
#include "trunk_shared.h"

char **g_fdfs_store_paths = NULL;
int g_fdfs_path_count = 0;
struct base64_context g_fdfs_base64_context;

void trunk_shared_init()
{
	base64_init_ex(&g_fdfs_base64_context, 0, '-', '_', '.');
}

char *trunk_info_dump(const FDFSTrunkFullInfo *pTrunkInfo, char *buff, \
				const int buff_size)
{
	snprintf(buff, buff_size, \
		"store_path_index=%d, " \
		"sub_path_high=%d, " \
		"sub_path_low=%d, " \
		"id=%d, offset=%d, size=%d, status=%d", \
		pTrunkInfo->path.store_path_index, \
		pTrunkInfo->path.sub_path_high, \
		pTrunkInfo->path.sub_path_low,  \
		pTrunkInfo->file.id, pTrunkInfo->file.offset, pTrunkInfo->file.size, \
		pTrunkInfo->status);

	return buff;
}

char *trunk_header_dump(const FDFSTrunkHeader *pTrunkHeader, char *buff, \
				const int buff_size)
{
	snprintf(buff, buff_size, \
		"file_type=%d, " \
		"alloc_size=%d, " \
		"file_size=%d, " \
		"crc32=%d, " \
		"mtime=%d, " \
		"ext_name=%s", \
		pTrunkHeader->file_type, pTrunkHeader->alloc_size, \
		pTrunkHeader->file_size, pTrunkHeader->crc32, \
		pTrunkHeader->mtime, pTrunkHeader->formatted_ext_name);

	return buff;
}

char *trunk_get_full_filename(const FDFSTrunkFullInfo *pTrunkInfo, \
		char *full_filename, const int buff_size)
{
	char filename[64];
	char *pStorePath;

	pStorePath = g_fdfs_store_paths[pTrunkInfo->path.store_path_index];
	TRUNK_GET_FILENAME(pTrunkInfo->file.id, filename);

	snprintf(full_filename, buff_size, \
			"%s/data/"FDFS_STORAGE_DATA_DIR_FORMAT"/" \
			FDFS_STORAGE_DATA_DIR_FORMAT"/%s", \
			pStorePath, pTrunkInfo->path.sub_path_high, \
			pTrunkInfo->path.sub_path_low, filename);

	return full_filename;
}

void trunk_pack_header(const FDFSTrunkHeader *pTrunkHeader, char *buff)
{
	*(buff + FDFS_TRUNK_FILE_FILE_TYPE_OFFSET) = pTrunkHeader->file_type;
	int2buff(pTrunkHeader->alloc_size, \
		buff + FDFS_TRUNK_FILE_ALLOC_SIZE_OFFSET);
	int2buff(pTrunkHeader->file_size, \
		buff + FDFS_TRUNK_FILE_FILE_SIZE_OFFSET);
	int2buff(pTrunkHeader->crc32, \
		buff + FDFS_TRUNK_FILE_FILE_CRC32_OFFSET);
	int2buff(pTrunkHeader->mtime, \
		buff + FDFS_TRUNK_FILE_FILE_MTIME_OFFSET);
	memcpy(buff + FDFS_TRUNK_FILE_FILE_EXT_NAME_OFFSET, \
		pTrunkHeader->formatted_ext_name, \
		FDFS_FILE_EXT_NAME_MAX_LEN + 1);
}

void trunk_unpack_header(const char *buff, FDFSTrunkHeader *pTrunkHeader)
{
	pTrunkHeader->file_type = *(buff + FDFS_TRUNK_FILE_FILE_TYPE_OFFSET);
	pTrunkHeader->alloc_size = buff2int(
			buff + FDFS_TRUNK_FILE_ALLOC_SIZE_OFFSET);
	pTrunkHeader->file_size = buff2int(
			buff + FDFS_TRUNK_FILE_FILE_SIZE_OFFSET);
	pTrunkHeader->crc32 = buff2int(
			buff + FDFS_TRUNK_FILE_FILE_CRC32_OFFSET);
	pTrunkHeader->mtime = buff2int(
			buff + FDFS_TRUNK_FILE_FILE_MTIME_OFFSET);
	memcpy(pTrunkHeader->formatted_ext_name, buff + \
		FDFS_TRUNK_FILE_FILE_EXT_NAME_OFFSET, \
		FDFS_FILE_EXT_NAME_MAX_LEN + 1);
	*(pTrunkHeader->formatted_ext_name + FDFS_FILE_EXT_NAME_MAX_LEN \
		 				+ 1) = '\0';
}

void trunk_file_info_encode(const FDFSTrunkFileInfo *pTrunkFile, char *str)
{
	char buff[sizeof(int) * 3];
	int len;

	int2buff(pTrunkFile->id, buff);
	int2buff(pTrunkFile->offset, buff + sizeof(int));
	int2buff(pTrunkFile->size, buff + sizeof(int) * 2);
	base64_encode_ex(&g_fdfs_base64_context, buff, sizeof(buff), \
			str, &len, false);
}

void trunk_file_info_decode(const char *str, FDFSTrunkFileInfo *pTrunkFile)
{
	char buff[sizeof(int) * 3];
	int len;

	base64_decode_auto(&g_fdfs_base64_context, str, FDFS_TRUNK_FILE_INFO_LEN, \
		buff, &len);

	pTrunkFile->id = buff2int(buff);
	pTrunkFile->offset = buff2int(buff + sizeof(int));
	pTrunkFile->size = buff2int(buff + sizeof(int) * 2);
}

int trunk_file_stat_func(const int store_path_index, const char *true_filename,\
	const int filename_len, stat_func statfunc, \
	struct stat *pStat, FDFSTrunkFullInfo *pTrunkInfo, \
	FDFSTrunkHeader *pTrunkHeader)
{
#define TRUNK_FILENAME_LENGTH (FDFS_TRUE_FILE_PATH_LEN + \
		FDFS_FILENAME_BASE64_LENGTH + FDFS_TRUNK_FILE_INFO_LEN + \
		1 + FDFS_FILE_EXT_NAME_MAX_LEN)

	char full_filename[MAX_PATH_SIZE];
	char buff[128];
	char temp[265];
	char pack_buff[FDFS_TRUNK_FILE_HEADER_SIZE];
	char szHexBuff[2 * FDFS_TRUNK_FILE_HEADER_SIZE + 1];
	int64_t file_size;
	int buff_len;
	int fd;
	int read_bytes;
	int result;
	FDFSTrunkHeader trueTrunkHeader;

	pTrunkInfo->file.id = 0;
	if (filename_len != TRUNK_FILENAME_LENGTH) //not trunk file
	{
		snprintf(full_filename, sizeof(full_filename), "%s/data/%s", \
			g_fdfs_store_paths[store_path_index], true_filename);

		if (statfunc(full_filename, pStat) == 0)
		{
			return 0;
		}
		else
		{
			return errno != 0 ? errno : ENOENT;
		}
	}

	memset(buff, 0, sizeof(buff));
	base64_decode_auto(&g_fdfs_base64_context, (char *)true_filename + \
		FDFS_TRUE_FILE_PATH_LEN, FDFS_FILENAME_BASE64_LENGTH, \
		buff, &buff_len);

	file_size = buff2long(buff + sizeof(int) * 2);
	if ((file_size & FDFS_TRUNK_FILE_SIZE) == 0)  //slave file
	{
		snprintf(full_filename, sizeof(full_filename), "%s/data/%s", \
			g_fdfs_store_paths[store_path_index], true_filename);

		if (statfunc(full_filename, pStat) == 0)
		{
			return 0;
		}
		else
		{
			return errno != 0 ? errno : ENOENT;
		}
	}

	trunk_file_info_decode(true_filename + FDFS_TRUE_FILE_PATH_LEN + \
		 FDFS_FILENAME_BASE64_LENGTH, &pTrunkInfo->file);

	pTrunkHeader->file_size = file_size & (~(FDFS_TRUNK_FILE_SIZE));
	pTrunkHeader->mtime = buff2int(buff + sizeof(int));
	pTrunkHeader->crc32 = buff2int(buff + sizeof(int) * 4);
	memcpy(pTrunkHeader->formatted_ext_name, true_filename + \
		(filename_len - (FDFS_FILE_EXT_NAME_MAX_LEN + 1)), \
		FDFS_FILE_EXT_NAME_MAX_LEN + 2); //include tailing '\0'
	pTrunkHeader->alloc_size = pTrunkInfo->file.size;
	pTrunkHeader->file_type = FDFS_TRUNK_FILE_TYPE_REGULAR;

	pTrunkInfo->path.store_path_index = store_path_index;
	pTrunkInfo->path.sub_path_high = strtol(true_filename, NULL, 16);
	pTrunkInfo->path.sub_path_low = strtol(true_filename + 3, NULL, 16);

	trunk_get_full_filename(pTrunkInfo, full_filename, \
				sizeof(full_filename));
	fd = open(full_filename, O_RDONLY);
	if (fd < 0)
	{
		return errno != 0 ? errno : EIO;
	}

	if (lseek(fd, pTrunkInfo->file.offset, SEEK_SET) < 0)
	{
		result = errno != 0 ? errno : EIO;
		close(fd);
		return result;
	}

	read_bytes = read(fd, buff, FDFS_TRUNK_FILE_HEADER_SIZE);
	result = errno;
	close(fd);
	if (read_bytes != FDFS_TRUNK_FILE_HEADER_SIZE)
	{
		return result != 0 ? errno : EINVAL;
	}

	trunk_pack_header(pTrunkHeader, pack_buff);

	fprintf(stderr, "file: "__FILE__", line: %d, true buff=%s\n", __LINE__, \
		bin2hex(buff+1, FDFS_TRUNK_FILE_HEADER_SIZE - 1, szHexBuff));
	trunk_unpack_header(buff, &trueTrunkHeader);
	fprintf(stderr, "file: "__FILE__", line: %d, true fields=%s\n", __LINE__, \
		trunk_header_dump(&trueTrunkHeader, full_filename, sizeof(full_filename)));

	fprintf(stderr, "file: "__FILE__", line: %d, my buff=%s\n", __LINE__, \
		bin2hex(pack_buff+1, FDFS_TRUNK_FILE_HEADER_SIZE - 1, szHexBuff));
	fprintf(stderr, "file: "__FILE__", line: %d, my trunk=%s, my fields=%s\n", __LINE__, \
		trunk_info_dump(pTrunkInfo, temp, sizeof(temp)), \
		trunk_header_dump(pTrunkHeader, full_filename, sizeof(full_filename)));

	if (memcmp(pack_buff+1, buff+1, FDFS_TRUNK_FILE_HEADER_SIZE - 1) != 0)
	{
		return ENOENT;
	}

	memset(pStat, 0, sizeof(struct stat));
	pStat->st_size = pTrunkHeader->file_size;
	pStat->st_mtime = pTrunkHeader->mtime;
	pStat->st_mode = S_IFREG;

	return 0;
}

