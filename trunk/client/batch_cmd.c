#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include "shared_func.h"
#include "logger.h"
#include "ini_file_reader.h"

#define CONF_FILE "./batch_cmd.conf"

int main(int argc,char* argv[])
{
	int f_ins[2];  //f_ins[0]:for read, f_ins[1]:for write.
	int f_outs[2];  //f_outs[0]:for read, f_ins[1]:for write.
	int f_errs[2];  //f_errs[0]:for read, f_ins[1]:for write.
	int pid;
	char stdin_buff[1024];
	char stdout_buff[1024];
	char stderr_buff[1024];
	char read_in;
	char read_out;
	char read_err;
	int result;
	int bytes;
	int i;
	char *args[64];
	char *group_name;
	IniContext iniContext;

	if (argc < 3 || argc > 64)
	{
		printf("Usage: %s <comand line>\n", argv[0]);
		return 1;
	}

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

	iniPrintItems(&iniContext);
	exit(0);

	/*
	IniItem *iniGetValuesEx(const char *szSectionName, const char *szItemName, \
		IniContext *pContext, int *nTargetCount);
	*/

	iniFreeContext(&iniContext);

	for (i=2; i<argc; i++)
	{
		args[i-2] = argv[i];
	}
	args[argc-2] = NULL;


 if(pipe(f_ins)==-1)
 {
  perror("Fail to creat pipe.\n");
  return 1;
 }

 if(pipe(f_outs)==-1)
 {
  perror("Fail to creat pipe.\n");
  return 1;
 }

 if(pipe(f_errs)==-1)
 {
  perror("Fail to creat pipe.\n");
  return 1;
 }

 if((pid=fork())==-1)
 {
  perror("Fail to creat child process.\n");
  return 1;
 }

 if(pid==0)
 {
  dup2(f_ins[0], STDIN_FILENO);
  dup2(f_outs[1], STDOUT_FILENO);
  dup2(f_errs[1], STDERR_FILENO);

  close(f_ins[0]);
  close(f_ins[1]);

  close(f_outs[0]);
  close(f_outs[1]);

  close(f_errs[0]);
  close(f_errs[1]);

  if(execvp(argv[2], args)==-1)
  {
   printf("Child process can't exec command %s.\n",argv[1]);
   return 1;
  }
  _exit(0);
 }

	close(f_ins[0]);
	close(f_outs[1]);
	close(f_errs[1]);

	if (set_nonblock(STDIN_FILENO) < 0)
	{
		return 1;
	}

	if (set_nonblock(f_outs[0]) < 0)
	{
		return 1;
	}

	if (set_nonblock(f_errs[0]) < 0)
	{
		return 1;
	}

	read_in = 1;
	read_out = 1;
	read_err = 1;

	while (read_out || read_err)
	{
		sleep(1);

		while (read_err)
		{
			bytes = read(f_errs[0], stderr_buff, sizeof(stderr_buff) - 1);
			if (bytes < 0)
			{
				if (!(errno == EAGAIN || errno == EWOULDBLOCK))
				{
					fprintf(stderr, "read error, bytes=%d, errno=%d\n", bytes, errno);
					read_err = 0;
				}

				break;
			}
			else if (bytes == 0)
			{
				read_err = 0;
			}

			*(stderr_buff + bytes) = '\0';
			printf("%s", stderr_buff);
		}

		while (read_out)
		{
			bytes = read(f_outs[0], stdout_buff, sizeof(stdout_buff) - 1);
			if (bytes < 0)
			{
				if (!(errno == EAGAIN || errno == EWOULDBLOCK))
				{
					fprintf(stderr, "read error, bytes=%d, errno=%d\n", bytes, errno);
					read_out = 0;
				}

				break;
			}
			else if (bytes == 0)
			{
				read_out = 0;
			}

			*(stdout_buff + bytes) = '\0';
			printf("%s", stdout_buff);
		}

		while (read_in)
		{
			bytes = read(STDIN_FILENO, stdin_buff, sizeof(stdin_buff));
			if (bytes < 0)
			{
				if (!(errno == EAGAIN || errno == EWOULDBLOCK))
				{
					fprintf(stderr, "read bytes=%d, errno=%d\n", bytes, errno);
					read_in = 0;
				}

				break;
			}
			else if (bytes == 0)
			{
				read_in = 0;
				break;
			}

			if (write(f_ins[1], stdin_buff, bytes) != bytes)
			{
				fprintf(stderr, "write bytes=%d, errno=%d\n", bytes, errno);
				read_in = 0;
				break;
			}
		}
	}

	close(f_ins[1]);
	close(f_outs[0]);
	close(f_errs[0]);

	return 0;
}

