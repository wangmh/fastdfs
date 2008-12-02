/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//socketopt.c
#include "common_define.h"
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
#include <netinet/tcp.h>
#include <netdb.h>
#include <fcntl.h>

#ifdef USE_SENDFILE

#ifdef OS_LINUX
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

int tcpgets(int sock, char* s, const int size, const int timeout)
{
	int result;
	char t;
	int i=1;

	if (s == NULL || size <= 0)
	{
#ifdef __DEBUG__
		fprintf(stderr,"%s,%d:tcpgets argument is illegal.\n",
				__FILE__,__LINE__);
#endif
		return EINVAL;
	}

	while (i < size)
	{
		result = tcprecvdata(sock, &t, 1, timeout);
		if (result != 0)
		{
#ifdef __DEBUG__
			fprintf(stderr,"%s,%d:tcpgets call tcprecvdata failed.\n",
					__FILE__,__LINE__);
#endif
			*s = 0;
			return result;
		}

		if (t == '\r')
		{
			continue;
		}

		if (t == '\n')
		{
			*s = t;
			s++;
			*s = 0;
			return 0;
		}

		*s = t;
		s++,i++;
	}

	*s = 0;
	return 0;
}

int tcprecvdata_ex(int sock, void *data, const int size, \
		const int timeout, int *count)
{
	int left_bytes;
	int read_bytes;
	int res;
	int ret_code;
	unsigned char* p;
	fd_set read_set;
	struct timeval t;

	if (data == NULL)
	{
#ifdef __DEBUG__
		fprintf(stderr,"%s,%d:tcprecvdata argument data is NULL.\n",
			__FILE__,__LINE__);
#endif
		return EINVAL;
	}

	FD_ZERO(&read_set);
	FD_SET(sock, &read_set);

	read_bytes = 0;
	ret_code = 0;
	p = (unsigned char*)data;
	left_bytes = size;
	while (left_bytes > 0)
	{
		if (timeout <= 0)
		{
			res = select(sock+1, &read_set, NULL, NULL, NULL);
		}
		else
		{
			t.tv_usec = 0;
			t.tv_sec = timeout;
			res = select(sock+1, &read_set, NULL, NULL, &t);
		}
		
		/*	
		struct timespec ts;
		sigset_t sigmask;

		sigemptyset(&sigmask);
		if (timeout <= 0)
		{
			res = pselect(sock+1, &read_set, NULL, NULL, NULL, &sigmask);
		}
		else
		{
			ts.tv_nsec = 0;
			ts.tv_sec = timeout;
			res = pselect(sock+1, &read_set, NULL, NULL, &ts, &sigmask);
		}
		*/
	
		if (res < 0)
		{
#ifdef __DEBUG__
			fprintf(stderr,"%s,%d:tcprecvdata call select failed:%s.\n",
				__FILE__,__LINE__,strerror(errno));
#endif
			ret_code = errno != 0 ? errno : EINTR;
			break;
		}
		else if (res == 0)
		{
#ifdef __DEBUG__
			fprintf(stderr,"%s,%d:tcprecvdata call select timeout.\n",
				__FILE__,__LINE__);
#endif
			ret_code = ETIMEDOUT;
			break;
		}
		
		//if (FD_ISSET(sock, &read_set))
		{
			read_bytes = read(sock, p, left_bytes);
			if (read_bytes < 0)
			{
#ifdef __DEBUG__
				fprintf(stderr,"%s,%d:tcprecvdata call read failed:%s.\n",
					__FILE__,__LINE__,strerror(errno));
#endif
				ret_code = errno != 0 ? errno : EINTR;
				break;
			}
			if (read_bytes == 0)
			{
#ifdef __DEBUG__
				fprintf(stderr, "%s,%d:tcprecvdata call read return 0, remote close connection? errno:%d, error info:%s.\n",
					__FILE__, __LINE__, errno, strerror(errno));
#endif
				ret_code = ENOTCONN;
				break;
			}

			left_bytes -= read_bytes;
			p += read_bytes;
		}
	}

	if (count != NULL)
	{
		*count = size - left_bytes;
	}
	return ret_code;
}

int tcpsenddata(int sock, void* data, const int size, const int timeout)
{
	int left_bytes;
	int write_bytes;
	int result;
	unsigned char* p;
	fd_set write_set;
	struct timeval t;

	if (data == NULL)
	{
#ifdef __DEBUG__
		fprintf(stderr,"%s,%d:tcpsenddata argument data is NULL.\n",
			__FILE__,__LINE__);
#endif
		return EINVAL;
	}

	FD_ZERO(&write_set);
	FD_SET(sock, &write_set);

	p = (unsigned char*)data;
	left_bytes = size;
	while (left_bytes > 0)
	{
		if (timeout <= 0)
		{
			result = select(sock+1, NULL, &write_set, NULL, NULL);
		}
		else
		{
			t.tv_usec = 0;
			t.tv_sec = timeout;
			result = select(sock+1, NULL, &write_set, NULL, &t);
		}
		if (result < 0)
		{
#ifdef __DEBUG__
			fprintf(stderr,"%s,%d:tcpsenddata call select failed:%s.\n",
				__FILE__,__LINE__,strerror(errno));
#endif
			return errno != 0 ? errno : EINTR;
		}
		else if (result == 0)
		{
#ifdef __DEBUG__
			fprintf(stderr,"%s,%d:tcpsenddata call select timeout.\n",
				__FILE__,__LINE__);
#endif
			return ETIMEDOUT;
		}

		//if (FD_ISSET(sock, &write_set))
		{
			write_bytes = write(sock, p, left_bytes);
			if (write_bytes < 0)
			{
#ifdef __DEBUG__
				fprintf(stderr,"%s,%d:tcpsenddata call write failed:%s.\n",
					__FILE__,__LINE__,strerror(errno));
#endif			
				return errno != 0 ? errno : EINTR;
			}

			left_bytes -= write_bytes;
			p += write_bytes;
		}
	}

	return 0;
}

int connectserverbyip(int sock, char* ip, short port)
{
	int result;
	struct sockaddr_in addr;

	addr.sin_family = PF_INET;
	addr.sin_port = htons(port);
	result = inet_aton(ip, &addr.sin_addr);
	if (result == 0 )
	{
#ifdef __DEBUG__
		fprintf(stderr,"file: %s, line: %d:connectserverbyip call " \
			"inet_aton failed: errno: %d, error info: %s.\n",
			__FILE__, __LINE__, errno, strerror(errno));
#endif
		return EINVAL;
	}

	result = connect(sock, (const struct sockaddr*)&addr, sizeof(addr));
	if (result < 0)
	{
#ifdef __DEBUG__
		fprintf(stderr,"file: %s, line: %d, connectserverbyip " \
			"%s:%d, call connect is failed, " \
			"errno: %d, error info: %s.\n",
			__FILE__, __LINE__, ip, port, errno, strerror(errno));
#endif
		return errno != 0 ? errno : EINTR;
	}

	return 0;
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
		if (inet_ntop(AF_INET, &addr.sin_addr, buff, bufferSize) == NULL)
		{
			*buff = '\0';
		}
	}
	else
	{
		*buff = '\0';
	}
	
	return addr.sin_addr.s_addr;
}

char *getHostnameByIp(const char *szIpAddr, char *buff, const int bufferSize)
{
	struct in_addr ip_addr;
	struct hostent *ent;

	if (inet_pton(AF_INET, szIpAddr, &ip_addr) != 1)
	{
		*buff = '\0';
		return buff;
	}

	ent = gethostbyaddr((char *)&ip_addr, sizeof(ip_addr), AF_INET);
	if (ent == NULL || ent->h_name == NULL)
	{
		*buff = '\0';
	}
	else
	{
		snprintf(buff, bufferSize, "%s", ent->h_name);
	}

	return buff;
}

in_addr_t getIpaddrByName(const char *name, char *buff, const int bufferSize)
{
    	struct in_addr ip_addr;
	struct hostent *ent;
	in_addr_t **addr_list;

	if (inet_pton(AF_INET, name, &ip_addr) == 1)
	{
		if (buff != NULL)
		{
			snprintf(buff, bufferSize, "%s", name);
		}
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
	if (buff != NULL)
	{
		if (inet_ntop(AF_INET, &ip_addr, buff, bufferSize) == NULL)
		{
			*buff = '\0';
		}
	}

	return ip_addr.s_addr;
}

int nbaccept(int sock, const int timeout, int *err_no)
{
	struct sockaddr_in inaddr;
	unsigned int sockaddr_len;
	fd_set read_set;
	struct timeval t;
	int result;
	
	if (timeout > 0)
	{
		t.tv_usec = 0;
		t.tv_sec = timeout;
		
		FD_ZERO(&read_set);
		FD_SET(sock, &read_set);
		
		result = select(sock+1, &read_set, NULL, NULL, &t);
		if (result == 0)  //timeout
		{
			*err_no = ETIMEDOUT;
			return -1;
		}
		else if (result < 0) //error
		{
			*err_no = errno != 0 ? errno : EINTR;
			return -1;
		}
	
		/*	
		if (!FD_ISSET(sock, &read_set))
		{
			*err_no = EAGAIN;
			return -1;
		}
		*/
	}
	
	sockaddr_len = sizeof(inaddr);
	result = accept(sock, (struct sockaddr*)&inaddr, &sockaddr_len);
	if (result < 0)
	{
		*err_no = errno != 0 ? errno : EINTR;
	}
	else
	{
		*err_no = 0;
	}

	return result;
}

int socketServer(const char *bind_ipaddr, const int port, int *err_no)
{
	struct sockaddr_in bindaddr;
	int sock;
	int result;
	
	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0)
	{
		*err_no = errno != 0 ? errno : EMFILE;
		logError("file: "__FILE__", line: %d, " \
			"socket create failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		return -1;
	}

	result = 1;
	if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &result, sizeof(int)) < 0)
	{
		*err_no = errno != 0 ? errno : ENOMEM;
		logError("file: "__FILE__", line: %d, " \
			"setsockopt failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		close(sock);
		return -2;
	}
	
	bindaddr.sin_family = AF_INET;
	bindaddr.sin_port = htons(port);
	if (bind_ipaddr == NULL || *bind_ipaddr == '\0')
	{
		bindaddr.sin_addr.s_addr = INADDR_ANY;
	}
	else
	{
		if (inet_aton(bind_ipaddr, &bindaddr.sin_addr) == 0)
		{
			*err_no = EINVAL;
			logError("file: "__FILE__", line: %d, " \
				"invalid ip addr %s", \
				__LINE__, bind_ipaddr);
			close(sock);
			return -3;
		}
	}

	result = bind(sock, (struct sockaddr*)&bindaddr, sizeof(bindaddr));
	if (result < 0)
	{
		*err_no = errno != 0 ? errno : ENOMEM;
		logError("file: "__FILE__", line: %d, " \
			"bind port %d failed, " \
			"errno: %d, error info: %s.", \
			__LINE__, port, errno, strerror(errno));
		close(sock);
		return -4;
	}
	
	result = listen(sock, 128);
	if (result < 0)
	{
		*err_no = errno != 0 ? errno : EINVAL;
		logError("file: "__FILE__", line: %d, " \
			"listen port %d failed, " \
			"errno: %d, error info: %s.", \
			__LINE__, port, errno, strerror(errno));
		close(sock);
		return -5;
	}

	*err_no = 0;
	return sock;
}

int tcprecvfile(int sock, const char *filename, const int64_t file_bytes, \
		const int fsync_after_written_bytes)
{
	int fd;
	char buff[FDFS_WRITE_BUFF_SIZE];
	int64_t remain_bytes;
	int recv_bytes;
	int written_bytes;
	int result;

	fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0)
	{
		return errno != 0 ? errno : EACCES;
	}

	written_bytes = 0;
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

		if ((result=tcprecvdata(sock, buff, recv_bytes, \
				g_network_timeout)) != 0)
		{
			close(fd);
			unlink(filename);
			return result;
		}

		if (write(fd, buff, recv_bytes) != recv_bytes)
		{
			result = errno != 0 ? errno: EIO;
			close(fd);
			unlink(filename);
			return result;
		}

		if (fsync_after_written_bytes > 0)
		{
			written_bytes += recv_bytes;
			if (written_bytes >= fsync_after_written_bytes)
			{
				written_bytes = 0;
				if (fsync(fd) != 0)
				{
					result = errno != 0 ? errno: EIO;
					close(fd);
					unlink(filename);
					return result;
				}
			}
		}

		remain_bytes -= recv_bytes;
	}

	close(fd);
	return 0;
}

int tcpdiscard(int sock, const int bytes)
{
	char buff[FDFS_WRITE_BUFF_SIZE];
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

		if ((result=tcprecvdata(sock, buff, recv_bytes, \
				g_network_timeout)) != 0)
		{
			return result;
		}

		remain_bytes -= recv_bytes;
	}

	return 0;
}

int tcpsendfile(int sock, const char *filename, const int64_t file_bytes)
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

	//printf("sendfile.............., file_bytes=%d\n", file_bytes);
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
	char buff[FDFS_WRITE_BUFF_SIZE];
	int64_t remain_bytes;
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

		if ((result=tcpsenddata(sock, buff, send_bytes, \
				g_network_timeout)) != 0)
		{
			close(fd);
			return result;
		}

		remain_bytes -= send_bytes;
		//printf("total send2: %d, remain_bytes2=%d\n\n", file_bytes - remain_bytes, remain_bytes);
	}
	}

	close(fd);
	return 0;
}

int tcpsetnonblockopt(int fd, const int timeout)
{
	int result;
	int flags;

	struct linger linger;
	struct timeval waittime;

	linger.l_onoff = 1;
	linger.l_linger = timeout * 100;
	result = setsockopt(fd, SOL_SOCKET, SO_LINGER, \
                      &linger, (socklen_t)sizeof(struct linger));
	if (result < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"setsockopt failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	waittime.tv_sec = timeout;
	waittime.tv_usec = 0;

	result = setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO,
                      &waittime, (socklen_t)sizeof(struct timeval));
	if (result < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"setsockopt failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	result = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,
                      &waittime, (socklen_t)sizeof(struct timeval));
	if (result < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"setsockopt failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	/*
	{
	int bytes;
	int size;

	bytes = 0;
	size = sizeof(int);
	result = getsockopt(fd, SOL_SOCKET, SO_SNDBUF,
			&bytes, (socklen_t *)&size);
	if (result < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"getsockopt failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	printf("send buff size: %d\n", bytes);

	result = getsockopt(fd, SOL_SOCKET, SO_RCVBUF,
			&bytes, (socklen_t *)&size);
	if (result < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"getsockopt failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	printf("recv buff size: %d\n", bytes);
	}
	*/

	flags = 1;
	result = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, \
			(char *)&flags, sizeof(flags));
	if (result < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"setsockopt failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : EINVAL;
	}

	flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"fcntl failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : EACCES;
	}

	if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
	{
		logError("file: "__FILE__", line: %d, " \
			"fcntl failed, errno: %d, error info: %s.", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : EACCES;
	}

	return 0;
}

int tcpsetnodelay(int fd)
{
	int flags;

	flags = 1;
	if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, \
			(char *)&flags, sizeof(flags)) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"setsockopt failed, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : EINVAL;
	}

	return 0;
}

int gethostaddrs(char ip_addrs[][IP_ADDRESS_SIZE], \
	const int max_count, int *count)
{
	struct hostent *ent;
	char hostname[128];
	char ip_addr[IP_ADDRESS_SIZE];
	int k;

	*count = 0;
	if (gethostname(hostname, sizeof(hostname)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call gethostname fail, " \
			"error no: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : EFAULT;
	}

	memset(ip_addr, 0, sizeof(ip_addr));
        ent = gethostbyname(hostname);
	if (ent == NULL)
	{
		*count = 0;
		return h_errno != 0 ? h_errno : EFAULT;
	}

	k = 0;
	while (ent->h_addr_list[k] != NULL)
	{
		if (*count >= max_count)
		{
			break;
		}

		if (inet_ntop(ent->h_addrtype, ent->h_addr_list[k], \
			ip_addrs[*count], IP_ADDRESS_SIZE) != NULL)
		{
			(*count)++;
		}

		k++;
	}

	return 0;
}

