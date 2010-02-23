#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/ioctl.h>
#include "shared_func.h"
#include "logger.h"
#include "ini_file_reader.h"

#define CONF_FILE "./batch_cmd.conf"
#define MAX_ARG_COUNT 32
#define MALLOC_BUFF_ONCE     (4 * 1024)

/*
typedef struct {
	int f_read;
	int f_write;
} PipeFD;
*/

static int *f_ins;
static int *f_outs;
static int *f_errs;

static char stdin_buff[1024];
static BufferInfo *stdout_buff;
static BufferInfo *stderr_buff;

static bool *read_in;
static bool *read_out;
static bool *read_err;

static IniItem *hosts;
static IniItem *pHostEnd;
static int host_count;

static int do_init();
static int batch_exec(char * args[]);
static int proccess_io();

int main(int argc,char* argv[])
{
	int result;
	int i;
	char *args[MAX_ARG_COUNT];
	char *group_name;
	IniContext iniContext;
	IniItem *pHost;

	if (argc < 3 || argc >= MAX_ARG_COUNT)
	{
		printf("Usage: %s <group_name> <comand line>\n", argv[0]);
		return 1;
	}

	/*
	int tty_fd;
	printf("ttyname(0)=%s\n", ttyname(0));
	printf("ttyname(1)=%s\n", ttyname(1));
	printf("ttyname(2)=%s\n", ttyname(2));

	tty_fd = open("/dev/tty", O_WRONLY);
	if (tty_fd < 0)
	{
		perror("open");
		return errno;
	}

	char buff[32];
	int buff_len;

	buff_len = sprintf(buff, "%s", "yq54321\n");
	printf("fd=%d, write bytes: %d\n", tty_fd, write(tty_fd, buff, buff_len));
	*/

	log_init();
	if ((result=iniLoadFromFile(CONF_FILE, &iniContext)) != 0)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"load from conf file \"%s\" fail, " \
			"error code: %d\n", \
			__LINE__, CONF_FILE, result);
		return result;
	}

	group_name = argv[1];

	//iniPrintItems(&iniContext);

	hosts = iniGetValuesEx(group_name, "host", &iniContext, &host_count);
	if (hosts == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"host of section: \"%s\" not exist\n", \
			__LINE__, group_name);
		return ENOENT;
	}

	printf("host count: %d\n", host_count);
	pHostEnd = hosts + host_count;
	for (pHost=hosts; pHost<pHostEnd; pHost++)
	{
		printf("%s\n", pHost->value);
	}

	args[0] = "ssh";

	for (i=2; i<argc; i++)
	{
		args[i] = argv[i];
	}
	args[argc] = NULL;

	if ((result=do_init()) != 0)
	{
		return result;
	}

	if ((result=batch_exec(args)) != 0)
	{
		return result;
	}

	if ((result=proccess_io()) != 0)
	{
		return result;
	}

	iniFreeContext(&iniContext);
	return 0;
}

static int do_init()
{
	f_ins = (int *)malloc(sizeof(int) * host_count);
	if (f_ins == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s\n", __LINE__, \
			(int)sizeof(int) * host_count, \
			errno, strerror(errno));
		return errno !=0 ? errno : ENOMEM;
	}

	f_outs = (int *)malloc(sizeof(int) * host_count);
	if (f_outs == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s\n", __LINE__, \
			(int)sizeof(int) * host_count, \
			errno, strerror(errno));
		return errno !=0 ? errno : ENOMEM;
	}

	f_errs = (int *)malloc(sizeof(int) * host_count);
	if (f_errs == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s\n", __LINE__, \
			(int)sizeof(int) * host_count, \
			errno, strerror(errno));
		return errno !=0 ? errno : ENOMEM;
	}

	stdout_buff = (BufferInfo *)malloc(sizeof(BufferInfo) * host_count);
	if (stdout_buff == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s\n", __LINE__, \
			(int)sizeof(BufferInfo) * host_count, \
			errno, strerror(errno));
		return errno !=0 ? errno : ENOMEM;
	}
	memset(stdout_buff, 0, sizeof(BufferInfo) * host_count);

	stderr_buff = (BufferInfo *)malloc(sizeof(BufferInfo) * host_count);
	if (stderr_buff == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s\n", __LINE__, \
			(int)sizeof(BufferInfo) * host_count, \
			errno, strerror(errno));
		return errno !=0 ? errno : ENOMEM;
	}
	memset(stderr_buff, 0, sizeof(BufferInfo) * host_count);

	read_in = (bool *)malloc(sizeof(bool) * host_count);
	if (read_in == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s\n", __LINE__, \
			(int)sizeof(bool) * host_count, \
			errno, strerror(errno));
		return errno !=0 ? errno : ENOMEM;
	}
	memset(read_in, 1, sizeof(bool) * host_count);

	read_out = (bool *)malloc(sizeof(bool) * host_count);
	if (read_out == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s\n", __LINE__, \
			(int)sizeof(bool) * host_count, \
			errno, strerror(errno));
		return errno !=0 ? errno : ENOMEM;
	}
	memset(read_out, 1, sizeof(bool) * host_count);

	read_err = (bool *)malloc(sizeof(bool) * host_count);
	if (read_err == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s\n", __LINE__, \
			(int)sizeof(bool) * host_count, \
			errno, strerror(errno));
		return errno !=0 ? errno : ENOMEM;
	}
	memset(read_err, 1, sizeof(bool) * host_count);

	return 0;
}

static int batch_exec(char * args[])
{
	IniItem *pHost;
	int pid;
	int in_fds[2];
	int err_fds[2];
	int out_fds[2];
	int i;
	int result;

	for (pHost=hosts, i=0; pHost<pHostEnd; pHost++, i++)
	{
		printf("%s\n", pHost->value);
		if(pipe(in_fds) < 0)
		{
			fprintf(stderr, "file: "__FILE__", line: %d, " \
				"call pipe fail, " \
				"errno: %d, error info: %s\n", __LINE__, \
				errno, strerror(errno));
			return errno != 0 ? errno : EMFILE;
		}

		if(pipe(err_fds) < 0)
		{
			fprintf(stderr, "file: "__FILE__", line: %d, " \
				"call pipe fail, " \
				"errno: %d, error info: %s\n", __LINE__, \
				errno, strerror(errno));
			return errno != 0 ? errno : EMFILE;
		}


		if(pipe(out_fds) < 0)
		{
			fprintf(stderr, "file: "__FILE__", line: %d, " \
				"call pipe fail, " \
				"errno: %d, error info: %s\n", __LINE__, \
				errno, strerror(errno));
			return errno != 0 ? errno : EMFILE;
		}

		if((pid=fork()) < 0)
		{
			fprintf(stderr, "file: "__FILE__", line: %d, " \
				"call fork fail, " \
				"errno: %d, error info: %s\n", __LINE__, \
				errno, strerror(errno));
			return errno != 0 ? errno : ENOMEM;
		}

		if(pid == 0) //child process
		{
			dup2(in_fds[0], STDIN_FILENO);
			dup2(out_fds[1], STDOUT_FILENO);
			dup2(err_fds[1], STDERR_FILENO);

			close(in_fds[0]);
			close(in_fds[1]);

			close(out_fds[0]);
			close(out_fds[1]);

			close(err_fds[0]);
			close(err_fds[1]);

			args[1] = pHost->value;
			if(execvp(args[0], args) < 0)
			{
				fprintf(stderr, "file: "__FILE__", line: %d, " \
					"call execvp fail, " \
					"errno: %d, error info: %s\n", \
					__LINE__, errno, strerror(errno));
  			}
			exit(0);
 		}

		f_ins[i] = in_fds[1];
		f_outs[i] = out_fds[0];
		f_errs[i] = err_fds[0];

		if ((result=set_nonblock(f_outs[i])) != 0)
		{
			return result;
		}

		if ((result=set_nonblock(f_errs[i])) != 0)
		{
			return result;
		}

		close(in_fds[0]);
		close(out_fds[1]);
		close(err_fds[1]);
	}

	return 0;
}

#define CHECK_BUFF_SIZE(bf) \
	if (bf.alloc_size - bf.length <= 1) \
	{ \
		bf.alloc_size += MALLOC_BUFF_ONCE; \
		bf.buff = (char *)realloc(bf.buff, bf.alloc_size); \
		if (bf.buff == NULL) \
		{ \
			fprintf(stderr, "file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s\n", __LINE__, \
				bf.alloc_size, errno, strerror(errno)); \
			return errno != 0 ? errno : ENOMEM; \
		} \
	}


static int proccess_io()
{
	IniItem *pHost;
	int bytes;
	int result;
	int i;
	int k;
	int in_count;
	int out_count;
	int err_count;

	if ((result=set_nonblock(STDIN_FILENO)) != 0)
	{
		return result;
	}

	in_count = host_count;
	out_count = host_count;
	err_count = host_count;

	while (out_count > 0 || err_count > 0)
	{
	sleep(2);

	for (pHost=hosts, i=0; pHost<pHostEnd; pHost++, i++)
	{
		while (read_err[i])
		{
			CHECK_BUFF_SIZE((stderr_buff[i]))

			bytes = read(f_errs[i], stderr_buff[i].buff + \
				stderr_buff[i].length, \
				stderr_buff[i].alloc_size - \
				stderr_buff[i].length - 1);
			if (bytes < 0)
			{
				if (!(errno == EAGAIN || errno == EWOULDBLOCK))
				{
					fprintf(stderr, "read error, bytes=%d, errno=%d\n", bytes, errno);
					read_err[i] = 0;
					err_count--;
				}

				break;
			}
			else if (bytes == 0)
			{
				read_err[i] = 0;
				err_count--;
				break;
			}

			if (i == 0)
			{
				*(stderr_buff[i].buff + stderr_buff[i].length \
					+ bytes) = '\0';
				printf("err=%s", stderr_buff[i].buff + \
					stderr_buff[i].length);
			}

			stderr_buff[i].length += bytes;
		}

		while (read_out[i])
		{
			CHECK_BUFF_SIZE((stdout_buff[i]))

			bytes = read(f_outs[i], stdout_buff[i].buff + \
				stdout_buff[i].length, \
				stdout_buff[i].alloc_size - \
				stdout_buff[i].length - 1);
			if (bytes < 0)
			{
				if (!(errno == EAGAIN || errno == EWOULDBLOCK))
				{
					fprintf(stderr, "read error, bytes=%d, errno=%d\n", bytes, errno);
					read_out[i] = 0;
					out_count--;
				}

				break;
			}
			else if (bytes == 0)
			{
				read_out[i] = 0;
				out_count--;
				break;
			}

			if (i == 0)
			{
				*(stdout_buff[i].buff + stdout_buff[i].length \
					+ bytes) = '\0';
				printf("out=%s", stdout_buff[i].buff + \
						stdout_buff[i].length);
			}

			stdout_buff[i].length += bytes;
		}

	if (in_count++ < 2)
	{
	ioctl(0, TIOCSTI, "y");
	ioctl(0, TIOCSTI, "q");
	ioctl(0, TIOCSTI, "5");
	ioctl(0, TIOCSTI, "4");
	ioctl(0, TIOCSTI, "3");
	ioctl(0, TIOCSTI, "2");
	ioctl(0, TIOCSTI, "1");
	ioctl(0, TIOCSTI, "\n");
	}
			/*
		while (in_count > 0)
		{
			bytes = read(STDIN_FILENO, stdin_buff, sizeof(stdin_buff));
			if (bytes < 0)
			{
				if (!(errno == EAGAIN || errno == EWOULDBLOCK))
				{
					fprintf(stderr, "read bytes=%d, errno=%d\n", bytes, errno);
				}

				break;
			}
			else if (bytes == 0)
			{
				break;
			}

			*(stdin_buff + bytes) = '\0';
			printf("stdin_buff=%s\n", stdin_buff);

			for (k=0; k<host_count; k++)
			{
				if (!read_in[k])
				{
					continue;
				}

				if (write(f_ins[k], stdin_buff, bytes) != bytes)
				{
					fprintf(stderr, "write bytes=%d, errno=%d\n", bytes, errno);
					read_in[k] = 0;
					in_count--;
				}
			}
		}
			*/
	}
	}

	return 0;
}

