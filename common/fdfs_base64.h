/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdfs_base64.h

#ifndef _FDFS_BASE64_H
#define _FDFS_BASE64_H

#include "common_define.h"

#ifdef __cplusplus
extern "C" {
#endif

#define base64_init(nLineLength) \
        base64_init_ex(nLineLength, '+', '/', '=')
#define base64_encode(src, nSrcLen, dest, dest_len) \
        base64_encode_ex(src, nSrcLen, dest, dest_len, true)

void base64_init_ex(const int nLineLength, const unsigned char chPlus, \
                    const unsigned char chSplash, const unsigned char chPad);

int base64_get_encode_length(const int nSrcLen);

char *base64_encode_ex(char *src, const int nSrcLen, char *dest, \
                       int *dest_len, const bool bPad);
char *base64_decode(char *src, const int nSrcLen, char *dest, int *dest_len);

void base64_set_line_separator(const char *pLineSeparator);
void base64_set_line_length(const int length);

#ifdef __cplusplus
}
#endif

#endif


