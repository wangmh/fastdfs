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

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*getnamefunc)(int socket, struct sockaddr *address, socklen_t *address_len);

#define getSockIpaddr(sock, buff, bufferSize) getIpaddr(getsockname, sock, buff, bufferSize)
#define getPeerIpaddr(sock, buff, bufferSize) getIpaddr(getpeername, sock, buff, bufferSize)

int tcpgets(int sock, char *s, int size, int timeout);
int tcprecvdata_ex(int sock, void *data, int size, int timeout, int *count);
int tcpsenddata(int sock, void* data, int size, int timeout);
int connectserverbyip(int sock, char* ip, short port);
int nbaccept(int sock, int timeout, int *err_no);
in_addr_t getIpaddr(getnamefunc getname, int sock, char *buff, const int bufferSize);
in_addr_t getIpaddrByName(const char *name, char *buff, const int bufferSize);
int socketServer(const char *bind_ipaddr, const int port, int *err_no);

#define tcprecvdata(sock, data, size, timeout) \
	tcprecvdata_ex(sock, data, size, timeout, NULL)

int tcpsendfile(int sock, const char *filename, const int file_bytes);
int tcprecvfile(int sock, const char *filename, const int file_bytes);
int tcpdiscard(int sock, const int bytes);

#ifdef __cplusplus
}
#endif

#endif
