#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <event.h>
#include <evhttp.h>
#include "logger.h"
#include "fdfs_global.h"
#include "shared_func.h"
#include "http_func.h"
#include "storage_func.h"
#include "storage_global.h"
#include "storage_httpd.h"

static struct evbuffer *ev_buf = NULL;
static char *file_trunk_buff = NULL;
static int http_start_status = 0;

static void generic_handler(struct evhttp_request *req, void *arg)
{
#define HTTPD_MAX_PARAMS   32
	char *url;
	char *file_id;
	int url_len;
	int uri_len;
	KeyValuePair params[HTTPD_MAX_PARAMS];
	int param_count;
	char *p;
	char *group_name;
	char *filename;
	char *pStorePath;
	char true_filename[128];
	char full_filename[MAX_PATH_SIZE + 64 + sizeof(FDFS_STORAGE_META_FILE_EXT)];
	char content_type[64];
	char szContentLength[16];
	struct stat file_stat;
	int filename_len;
	int fd;
	int read_bytes;
	off_t remain_bytes;

	url = (char *)evhttp_request_uri(req);
	url_len = strlen(url);
	if (url_len < 16)
	{
		evhttp_send_error(req, HTTP_BADREQUEST, "Bad request");
		return;
	}

	if (strncasecmp(url, "http://", 7) == 0)
	{
		p = strchr(url+7, '/');
		if (p == NULL)
		{
			evhttp_send_error(req, HTTP_BADREQUEST, "Bad request");
			return;
		}

		uri_len = url_len - (p - url);
		url = p;
	}
	else
	{
		uri_len = url_len;
	}

	if (uri_len + 1 >= 256)
	{
		evhttp_send_error(req, HTTP_BADREQUEST, "Bad request");
		return;
	}

	param_count = http_parse_query(url, params, HTTPD_MAX_PARAMS);
	file_id = url;
	if (strlen(file_id) < 22)
	{
		evhttp_send_error(req, HTTP_BADREQUEST, "Bad request");
		return;
	}

	if (*file_id == '/')
	{
		file_id++;
	}

	if (g_http_params.anti_steal_token)
	{
		char *token;
		char *ts;
		int timestamp;

		token = fdfs_http_get_parameter("token", params, param_count);
		ts = fdfs_http_get_parameter("ts", params, param_count);
		if (token == NULL || ts == NULL)
		{
			evhttp_send_error(req, HTTP_BADREQUEST, "Bad request");
			return;
		}

		timestamp = atoi(ts);
		if (fdfs_http_check_token(&g_http_params.anti_steal_secret_key,\
			file_id, timestamp, token, \
			g_http_params.token_ttl) != 0)
		{
			if (*(g_http_params.token_check_fail_content_type))
			{
				evbuffer_add(ev_buf, \
				g_http_params.token_check_fail_buff.buff, \
				g_http_params.token_check_fail_buff.length);

				evhttp_add_header(req->output_headers, \
				"Content-Type", \
				g_http_params.token_check_fail_content_type);

				evhttp_send_reply(req, HTTP_OK, "OK", ev_buf);
			}
			else
			{
				evhttp_send_error(req, HTTP_BADREQUEST, "Bad request");
			}

			return;
		}
	}

	group_name = file_id;
	filename = strchr(file_id, '/');
	if (filename == NULL)
	{
		evhttp_send_error(req, HTTP_BADREQUEST, "Bad request");
		return;
	}

	*filename = '\0';
	filename++;  //skip '/'

	if (strcmp(group_name, g_group_name) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"group_name: %s is not my group!", \
			__LINE__, group_name);
		evhttp_send_error(req, HTTP_NOTFOUND, "Not found");
		return;
	}

	filename_len = strlen(filename);
	if (storage_split_filename(filename, &filename_len, \
		true_filename, &pStorePath) != 0)
	{
		evhttp_send_error(req, HTTP_BADREQUEST, "Bad request");
		return;
	}

	if (fdfs_check_data_filename(true_filename, filename_len) != 0)
	{
		evhttp_send_error(req, HTTP_BADREQUEST, "Bad request");
		return;
	}

	snprintf(full_filename, sizeof(full_filename), "%s/data/%s", \
		pStorePath, true_filename);
	if (stat(full_filename, &file_stat) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"stat file: %s fail, errno: %d, error info: %s", \
			__LINE__, full_filename, errno, STRERROR(errno));
		evhttp_send_error(req, HTTP_NOTFOUND, "Not found");
		return;
	}

	if (fdfs_http_get_content_type_by_extname(&g_http_params, \
		true_filename, content_type, sizeof(content_type)) != 0)
        {
		evhttp_send_error(req, HTTP_SERVUNAVAIL, "Service unavail");
		return;
        }

	fd = open(full_filename, O_RDONLY);
	if (fd < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"open file %s fail, " \
			"errno: %d, error info: %s", __LINE__, \
			full_filename, errno, STRERROR(errno));
		evhttp_send_error(req, HTTP_SERVUNAVAIL, "Service unavail");
		return;
	}

	sprintf(szContentLength, INT64_PRINTF_FORMAT, file_stat.st_size);
	evhttp_add_header(req->output_headers, "Connection", "close");
	evhttp_add_header(req->output_headers, "Content-Type", content_type);
	evhttp_add_header(req->output_headers, "Content-Length", szContentLength);
	evhttp_send_reply_start(req, HTTP_OK, "OK");

	remain_bytes = file_stat.st_size;
	while (remain_bytes > 0)
	{
		read_bytes = remain_bytes <= g_http_trunk_size ? \
			     remain_bytes : g_http_trunk_size;
		if (read(fd, file_trunk_buff, read_bytes) != read_bytes)
		{
			close(fd);
			logError("file: "__FILE__", line: %d, " \
				"read from file %s fail, " \
				"errno: %d, error info: %s", __LINE__, \
				full_filename, errno, STRERROR(errno));
			evhttp_send_error(req, HTTP_SERVUNAVAIL, "Service unavail");
			return;
		}

		evbuffer_add(ev_buf, file_trunk_buff, read_bytes);
		evhttp_send_reply_chunk(req, ev_buf);

		remain_bytes -= read_bytes;
	}

	close(fd);
	evhttp_send_reply_end(req);
}
 
static void status_handler(struct evhttp_request *req, void *arg)
{
	evbuffer_add(ev_buf, "OK", 2);
	evhttp_send_reply(req, HTTP_OK, "OK", ev_buf);
}

static void *httpd_entrance(void *arg)
{
	char *bind_addr;
	struct evhttp *httpd;

	bind_addr = (char *)arg;
	if (*bind_addr == '\0')
	{
		bind_addr = "0.0.0.0";
	}
	event_init();
	httpd = evhttp_start(bind_addr, g_http_params.server_port);
	if (httpd == NULL)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"evhttp_start fail, errno: %d, error info: %s", \
			__LINE__, errno, STRERROR(errno));
		http_start_status = errno != 0 ? errno : EACCES;
		return NULL;
	}

	http_start_status = 0;
	evhttp_set_cb(httpd, "/status.html", status_handler, NULL);
	evhttp_set_gencb(httpd, generic_handler, NULL);

	event_dispatch();
	evhttp_free(httpd);
	return NULL;
}

int storage_httpd_start(const char *bind_addr)
{
	int result;
	pthread_t tid;

	file_trunk_buff = (char *)malloc(g_http_trunk_size);
	if (file_trunk_buff == NULL)
	{
		result = errno != 0 ? errno : ENOMEM;
		logCrit("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s", \
			__LINE__, g_http_trunk_size, result, STRERROR(result));
		return result;
	}

	ev_buf = evbuffer_new();
	if (ev_buf == NULL)
	{
		result = errno != 0 ? errno : ENOMEM;
		logCrit("file: "__FILE__", line: %d, " \
			"call evbuffer_new fail, errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	http_start_status = -1;
	if ((result=pthread_create(&tid, NULL, httpd_entrance, \
			(void *)bind_addr)) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"create thread failed, errno: %d, error info: %s", \
			__LINE__, result, STRERROR(result));
		return result;
	}

	while (http_start_status == -1)
	{
		sleep(1);
	}

	return http_start_status;
}

