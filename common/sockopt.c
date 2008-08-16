/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//socketopt.c
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>

#ifdef USE_SENDFILE

#ifdef OS_LINUX
#include <netinet/tcp.h>
#include <sys/sendfile.h>
#else
#ifdef OS_FREEBSD
#include <sys/uio.h>
#endif
#endif

#endif

#include "sockopt.h"
#include "logger.h"
#include "fdfs_global.h"

#define FDFS_BUFF_SIZE  128 * 1024

int tcpgets(int sock,char* s,int size,int timeout)
{
	int result;
	char t;
	int i=1;
	if(s==NULL || size<=0)
	{
#ifdef __DEBUG__
		fprintf(stderr,"%s,%d:tcpgets argument is illegal.\n",
				__FILE__,__LINE__);
#endif
		return(-1);
	}
	while(i<size)
	{
		result=tcprecvdata(sock,&t,1,timeout);
		if(result<0)
		{
#ifdef __DEBUG__
			fprintf(stderr,"%s,%d:tcpgets call tcprecvdata failed.\n",
					__FILE__,__LINE__);
#endif
			return(-1);
		}
		if(result==0)
		{
			*s=0;
			return(0);
		}
		if(t=='\r')
			continue;
		if(t=='\n')
		{
			*s=t;
			s++;
			*s=0;
			return(1);
		}
		*s=t;
		s++,i++;
	}
	*s=0;
	return(1);
}

int tcprecvdata(int sock,void* data,int size,int timeout)
{
	int byteleft;
	int result;
	unsigned char* p;
	fd_set read_set;
	fd_set exception_set;
	struct timeval t;

	if(data==NULL)
	{
#ifdef __DEBUG__
		fprintf(stderr,"%s,%d:tcprecvdata argument data is NULL.\n",
			__FILE__,__LINE__);
#endif
		return(-1);
	}
	p=(unsigned char*)data;
	byteleft=size;
	FD_ZERO(&read_set);
	FD_ZERO(&exception_set);
	while(byteleft>0)
	{
		FD_CLR(sock,&read_set);
		FD_CLR(sock,&exception_set);
		FD_SET(sock,&read_set);
		FD_SET(sock,&exception_set);
		if(timeout <= 0)
		{
			result=select(sock+1,&read_set,NULL,&exception_set,NULL);
		}
		else
		{
			t.tv_usec = 0;
			t.tv_sec = timeout;
			result=select(sock+1,&read_set,NULL,&exception_set,&t);
		}
		
		if(result<0)
		{
#ifdef __DEBUG__
			fprintf(stderr,"%s,%d:tcprecvdata call select failed:%s.\n",
				__FILE__,__LINE__,strerror(errno));
#endif
			return(-1);
		}
		else if(result==0)
		{
#ifdef __DEBUG__
			fprintf(stderr,"%s,%d:tcprecvdata call select timeout.\n",
				__FILE__,__LINE__);
#endif
			return(0);
		}
		
		if(FD_ISSET(sock, &read_set))
		{
			result=read(sock,p,byteleft);
			if(result<0)
			{
#ifdef __DEBUG__
				fprintf(stderr,"%s,%d:tcprecvdata call read failed:%s.\n",
					__FILE__,__LINE__,strerror(errno));
#endif
				return(-1);
			}
			if(result == 0)
			{
#ifdef __DEBUG__
				fprintf(stderr, "%s,%d:tcprecvdata call read return 0, remote close connection? errno:%d, error info:%s.\n",
					__FILE__, __LINE__, errno, strerror(errno));
#endif
				return(-1);
			}
			byteleft-=result;
			p+=result;
			continue;
		}
		/*exception not support*/
		return(-1);
	}
	return(1);
}

int tcpsenddata(int sock,void* data,int size,int timeout)
{
	int byteleft;
	int result;
	unsigned char* p;
	fd_set write_set;
	fd_set exception_set;
	struct timeval t;

	if(data==NULL)
	{
#ifdef __DEBUG__
		fprintf(stderr,"%s,%d:tcpsenddata argument data is NULL.\n",
			__FILE__,__LINE__);
#endif
		return(-1);
	}

	p = (unsigned char*)data;
	byteleft = size;
	FD_ZERO(&write_set);
	FD_ZERO(&exception_set);
	while(byteleft > 0)
	{
		FD_CLR(sock,&write_set);
		FD_CLR(sock,&exception_set);
		FD_SET(sock,&write_set);
		FD_SET(sock,&exception_set);
		if(timeout <= 0)
		{
			result=select(sock+1,NULL,&write_set,&exception_set,NULL);
		}
		else
		{
			t.tv_usec = 0;
			t.tv_sec = timeout;
			result=select(sock+1,NULL,&write_set,&exception_set,&t);
		}
		if(result<0)
		{
#ifdef __DEBUG__
			fprintf(stderr,"%s,%d:tcpsenddata call select failed:%s.\n",
				__FILE__,__LINE__,strerror(errno));
#endif
			return(-1);
		}
		else if(result==0)
		{
#ifdef __DEBUG__
			fprintf(stderr,"%s,%d:tcpsenddata call select timeout.\n",
				__FILE__,__LINE__);
#endif
			return(0);
		}
		if(FD_ISSET(sock,&write_set))
		{
			result=write(sock,p,byteleft);
			if(result<0)
			{
#ifdef __DEBUG__
				fprintf(stderr,"%s,%d:tcpsenddata call write failed:%s.\n",
					__FILE__,__LINE__,strerror(errno));
#endif			
				return(-1);
			}
			byteleft-=result;
			p+=result;
			continue;
		}
		/*exception not support*/
		return(-1);
	}

	return(1);
}

int connectserverbyip(int sock, char* ip, short port)
{
	int result;
	struct sockaddr_in addr;
	addr.sin_family = PF_INET;
	addr.sin_port = htons(port);
	result = inet_aton(ip, &addr.sin_addr);
	if(result == 0 )
	{
#ifdef __DEBUG__
		fprintf(stderr,"file: %s, line: %d:connectserverbyip call " \
			"inet_aton failed: errno: %d, error info: %s.\n",
			__FILE__, __LINE__, errno, strerror(errno));
#endif
		return(-1);
	}

	result = connect(sock, (const struct sockaddr*)&addr, sizeof(addr));
	if(result < 0)
	{
#ifdef __DEBUG__
		fprintf(stderr,"file: %s, line: %d, connectserverbyip " \
			"%s:%d, call connect is failed, " \
			"errno: %d, error info: %s.\n",
			__FILE__, __LINE__, ip, port, errno, strerror(errno));
#endif
		return(-1);
	}

	return(1);
}

in_addr_t getIpaddr(getnamefunc getname, int sock, \
		char *buff, const int bufferSize)
{
	struct sockaddr_in addr;
	socklen_t addrlen;

	memset(&addr, 0, sizeof(addr));
	addrlen = sizeof(addr);
	
	if (getname(sock, (struct sockaddr *)&addr, &addrlen) != 0)
	{
		buff[0] = '\0';
		return INADDR_NONE;
	}
	
	if (addrlen > 0)
	{
		snprintf(buff, bufferSize, "%s", inet_ntoa(addr.sin_addr));
	}
	else
	{
		buff[0] = '\0';
	}
	
	return addr.sin_addr.s_addr;
}

in_addr_t getIpaddrByName(const char *name, char *buff, const int bufferSize)
{
    	struct in_addr ip_addr;
	struct hostent *ent;
	in_addr_t **addr_list;

	if (inet_pton(AF_INET, name, &ip_addr) == 1)
	{
		snprintf(buff, bufferSize, "%s", name);
		return ip_addr.s_addr;
	}

	ent = gethostbyname(name);
	if (ent == NULL)
	{
		return INADDR_NONE;
	}
        addr_list = (in_addr_t **)ent->h_addr_list;
	if (addr_list[0] == NULL)
	{
		return INADDR_NONE;
	}

	memset(&ip_addr, 0, sizeof(ip_addr));
	ip_addr.s_addr = *(addr_list[0]);
	snprintf(buff, bufferSize, "%s", inet_ntoa(ip_addr));
	return ip_addr.s_addr;
}

int nbaccept(int sock, int timeout, int *err_no)
{
	struct sockaddr_in inaddr;
	unsigned int sockaddr_len;
	fd_set read_set;
	fd_set exception_set;
	struct timeval t;
	int result;
	
	if (timeout > 0)
	{
		t.tv_usec = 0;
		t.tv_sec = timeout;
		
		FD_ZERO(&read_set);
		FD_ZERO(&exception_set);
		FD_SET(sock, &read_set);
		FD_SET(sock, &exception_set);
		
		result = select(sock+1, &read_set, NULL, &exception_set, &t);
		if(result == 0)  //timeout
		{
			*err_no = ETIMEDOUT;
			return -1;
		}
		else if (result < 0) //error
		{
			*err_no = errno;
			return -1;
		}
		
		if(!FD_ISSET(sock, &read_set))
		{
			*err_no = EAGAIN;
			return -1;
		}
	}
	
	sockaddr_len = sizeof(inaddr);
	result = accept(sock, (struct sockaddr*)&inaddr, &sockaddr_len);
	if (result < 0)
	{
		*err_no = errno;
	}
	else
	{
		*err_no = 0;
	}

	return result;
}

int socketServer(const char *bind_ipaddr, const int port)
{
	struct sockaddr_in bindaddr;
	int sock;
	int result;
	
	sock = socket(AF_INET, SOCK_STREAM, 0);
	if(sock < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"socket create failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		return -1;
	}

	result = 1;
	if(setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &result, sizeof(int)) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"setsockopt failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		close(sock);
		return -2;
	}
	
	bindaddr.sin_family = AF_INET;
	bindaddr.sin_port = htons(port);
	if (bind_ipaddr == NULL || bind_ipaddr[0] == '\0')
	{
		bindaddr.sin_addr.s_addr = INADDR_ANY;
	}
	else
	{
		if (inet_aton(bind_ipaddr, &bindaddr.sin_addr) == 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"invalid ip addr %s, " \
				"errno: %d, error info: %s.", \
				__LINE__, bind_ipaddr, \
				errno, strerror(errno));
			close(sock);
			return -3;
		}
	}

	result = bind(sock, (struct sockaddr*)&bindaddr, sizeof(bindaddr));
	if(result<0)
	{
		logError("file: "__FILE__", line: %d, " \
			"bind port %d failed, " \
			"errno: %d, error info: %s.", \
			__LINE__, port, errno, strerror(errno));
		close(sock);
		return -4;
	}
	
	result = listen(sock, 5);
	if(result < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"listen port %d failed, " \
			"errno: %d, error info: %s.", \
			__LINE__, port, errno, strerror(errno));
		close(sock);
		return -5;
	}
	
	return sock;
}

int tcprecvfile(int sock, const char *filename, const int file_bytes)
{
	int fd;
	char buff[FDFS_BUFF_SIZE];
	int remain_bytes;
	int recv_bytes;
	int result;

	fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0)
	{
		return errno != 0 ? errno : EACCES;
	}

	remain_bytes = file_bytes;
	while (remain_bytes > 0)
	{
		if (remain_bytes > sizeof(buff))
		{
			recv_bytes = sizeof(buff);
		}
		else
		{
			recv_bytes = remain_bytes;
		}

		if (tcprecvdata(sock, buff, recv_bytes, g_network_timeout) != 1)
		{
			result = errno;
			close(fd);
			unlink(filename);
			return result != 0 ? result : EIO;
		}

		if (write(fd, buff, recv_bytes) != recv_bytes)
		{
			result = errno;
			close(fd);
			unlink(filename);
			return result != 0 ? result : EIO;
		}

		remain_bytes -= recv_bytes;
	}

	close(fd);
	return 0;
}

int tcpdiscard(int sock, const int bytes)
{
	char buff[FDFS_BUFF_SIZE];
	int remain_bytes;
	int recv_bytes;
	int result;

	remain_bytes = bytes;
	while (remain_bytes > 0)
	{
		if (remain_bytes > sizeof(buff))
		{
			recv_bytes = sizeof(buff);
		}
		else
		{
			recv_bytes = remain_bytes;
		}

		if (tcprecvdata(sock, buff, recv_bytes, g_network_timeout) != 1)
		{
			result = errno;
			return result != 0 ? result : EIO;
		}

		remain_bytes -= recv_bytes;
	}

	return 0;
}

int tcpsendfile(int sock, const char *filename, const int file_bytes)
{
	int fd;
	int send_bytes;
	int result;
#ifdef USE_SENDFILE
	off_t offset;
#endif

	fd = open(filename, O_RDONLY);
	if (fd < 0)
	{
		return errno != 0 ? errno : EACCES;
	}

#ifdef USE_SENDFILE

#ifdef OS_LINUX
	/*
	result = 1;
	if (setsockopt(sock, SOL_TCP, TCP_CORK, &result, sizeof(int)) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"setsockopt failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		close(fd);
		return errno != 0 ? errno : EIO;
	}
	*/

	offset = 0;
	send_bytes = sendfile(sock, fd, &offset, file_bytes);
	close(fd);
	if (send_bytes != file_bytes)
	{
		return errno != 0 ? errno : EIO;
	}
	else
	{
		return 0;
	}
#else
#ifdef OS_FREEBSD
	offset = 0;
	result = sendfile(fd, sock, offset, file_bytes, NULL, NULL, 0);
	close(fd);
	if (result != 0)
	{
		return errno != 0 ? errno : EIO;
	}
	else
	{
		return 0;
	}
#endif
#endif

#endif

	//printf("file_bytes=%d\n", file_bytes);

	{
	char buff[FDFS_BUFF_SIZE];
	int remain_bytes;
	remain_bytes = file_bytes;
	while (remain_bytes > 0)
	{
		if (remain_bytes > sizeof(buff))
		{
			send_bytes = sizeof(buff);
		}
		else
		{
			send_bytes = remain_bytes;
		}

		if (read(fd, buff, send_bytes) != send_bytes)
		{
			result = errno;
			close(fd);
			return result != 0 ? result : EIO;
		}

		//printf("send bytes=%d, total send1: %d, remain_bytes1=%d\n", send_bytes, file_bytes - remain_bytes, remain_bytes);

		if (tcpsenddata(sock, buff, send_bytes, g_network_timeout) != 1)
		{
			result = errno;
			close(fd);
			return result != 0 ? result : EIO;
		}

		remain_bytes -= send_bytes;
		//printf("total send2: %d, remain_bytes2=%d\n\n", file_bytes - remain_bytes, remain_bytes);
	}
	}

	close(fd);
	return 0;
}

