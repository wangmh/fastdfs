#include <sys/types.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <event.h>
#include <evhttp.h>
#include "logger.h"
#include "tracker_global.h"
#include "http_func.h"
#include "tracker_httpd.h"

static struct evbuffer *ev_buf = NULL;

static void generic_handler(struct evhttp_request *req, void *arg)
{
#define HTTPD_MAX_PARAMS   32
	char *url;
	char *file_id;
	KeyValuePair params[HTTPD_MAX_PARAMS];
	int param_count;
	KeyValuePair *pCurrent;
	KeyValuePair *pEnd;
	char cbuff[1 * 1024];
	char *p;

	url = (char *)evhttp_request_uri(req);
	param_count = http_parse_query(url, params, HTTPD_MAX_PARAMS);

	/*
	memset(cbuff, ' ', sizeof(cbuff));

	p = cbuff;
	p += sprintf(p, "url=%s\n", url);
	pEnd = params + param_count;
	for (pCurrent=params; pCurrent<pEnd; pCurrent++)
	{
		p += sprintf(p, "%s=%s\n", pCurrent->key, pCurrent->value);
	}
	*/

	file_id = url;
	if (strlen(file_id) < 16)
	{
		evhttp_send_reply(req, HTTP_BADREQUEST, "Bad request", ev_buf);
		return;
	}

	if (strncasecmp(file_id, "http://", 7) == 0)
	{
		p = strchr(file_id+7, '/');
		if (p == NULL)
		{
			evhttp_send_reply(req, HTTP_BADREQUEST, \
					"Bad request", ev_buf);
			return;
		}

		file_id = p + 1;
	}
	else if (*file_id == '/')
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
			evhttp_send_reply(req, HTTP_BADREQUEST, \
					"Bad request", ev_buf);
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
				evhttp_send_reply(req, HTTP_BADREQUEST, \
						"Bad request", ev_buf);
			}

			return;
		}

		evbuffer_add_printf(ev_buf, "token=%s\n", token);
		evbuffer_add_printf(ev_buf, "ts=%s\n", ts);
	}
	
	evbuffer_add_printf(ev_buf, "file_id=%s\n", file_id);
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
		logError("file: "__FILE__", line: %d, " \
			"evhttp_start fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return NULL;
	}

	evhttp_set_gencb(httpd, generic_handler, NULL);

	event_dispatch();
	evhttp_free(httpd);
	return NULL;
}

int tracker_httpd_start(const char *bind_addr)
{
	int result;
	pthread_t tid;

	ev_buf = evbuffer_new();
	if (ev_buf == NULL)
	{
		result = errno != 0 ? errno : ENOMEM;
		logError("file: "__FILE__", line: %d, " \
			"call evbuffer_new fail, errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	if ((result=pthread_create(&tid, NULL, httpd_entrance, \
			(void *)bind_addr)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"create thread failed, errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	return 0;
}

