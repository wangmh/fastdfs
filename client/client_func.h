/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//client_func.h

#ifndef _CLIENT_FUNC_H_
#define _CLIENT_FUNC_H_

#ifdef __cplusplus
extern "C" {
#endif

/**
* client initial function
* params:
*	filename: storage config filename
* return: 0 success, !=0 fail, return the error code
**/
int fdfs_client_init(const char *filename);

/**
* client destroy function
* params:
* return: 
**/
void fdfs_client_destroy();

#ifdef __cplusplus
}
#endif

#endif
