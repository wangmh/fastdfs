/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//socketopt.h

#ifndef _SOCKETOPT_H_
#define _SOCKETOPT_H_

#include "common_define.h"

#define FDFS_WRITE_BUFF_SIZE  512 * 1024

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*getnamefunc)(int socket, struct sockaddr *address, \
		socklen_t *address_len);

typedef int (*tcpsenddatafunc)(int sock, void* data, const int size, \
		const int timeout);

typedef int (*tcprecvdata_exfunc)(int sock, void *data, const int size, \
		const int timeout, int *count);

#define getSockIpaddr(sock, buff, bufferSize) \
	getIpaddr(getsockname, sock, buff, bufferSize)

#define getPeerIpaddr(sock, buff, bufferSize) \
	getIpaddr(getpeername, sock, buff, bufferSize)

int tcpgets(int sock, char *s, const int size, const int timeout);
int tcprecvdata_ex(int sock, void *data, const int size, \
		const int timeout, int *count);
int tcprecvdata_nb_ex(int sock, void *data, const int size, \
		const int timeout, int *count);
int tcpsenddata(int sock, void* data, const int size, const int timeout);
int tcpsenddata_nb(int sock, void* data, const int size, const int timeout);
int connectserverbyip(int sock, char* ip, short port);
int nbaccept(int sock, const int timeout, int *err_no);
int tcpsetserveropt(int fd, const int timeout);
int tcpsetnonblockopt(int fd);
int tcpsetnodelay(int fd);

in_addr_t getIpaddr(getnamefunc getname, int sock, \
		char *buff, const int bufferSize);
char *getHostnameByIp(const char *szIpAddr, char *buff, const int bufferSize);
in_addr_t getIpaddrByName(const char *name, char *buff, const int bufferSize);

int socketServer(const char *bind_ipaddr, const int port, int *err_no);

#define tcprecvdata(sock, data, size, timeout) \
	tcprecvdata_ex(sock, data, size, timeout, NULL)

#define tcpsendfile(sock, filename, file_bytes, timeout) \
	tcpsendfile_ex(sock, filename, 0, file_bytes, timeout)

#define tcprecvdata_nb(sock, data, size, timeout) \
	tcprecvdata_nb_ex(sock, data, size, timeout, NULL)

int tcpsendfile_ex(int sock, const char *filename, const int64_t file_offset, \
		const int64_t file_bytes, const int timeout);

int tcprecvfile(int sock, const char *filename, const int64_t file_bytes, \
		const int fsync_after_written_bytes, const int timeout);
int tcprecvfile_ex(int sock, const char *filename, const int64_t file_bytes, \
		const int fsync_after_written_bytes, \
		unsigned int *hash_codes, const int timeout);

int tcpdiscard(int sock, const int bytes, const int timeout);

int gethostaddrs(char ip_addrs[][IP_ADDRESS_SIZE], \
	const int max_count, int *count);

#ifdef __cplusplus
}
#endif

#endif
