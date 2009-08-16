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
	KeyValuePair params[HTTPD_MAX_PARAMS];
	int param_count;
	KeyValuePair *pCurrent;
	KeyValuePair *pEnd;
	char cbuff[1 * 1024];
	char *p;

	url = (char *)evhttp_request_uri(req);
	param_count = http_parse_query(url, params, HTTPD_MAX_PARAMS);

	memset(cbuff, ' ', sizeof(cbuff));

	p = cbuff;
	p += sprintf(p, "url=%s\n", url);
	pEnd = params + param_count;
	for (pCurrent=params; pCurrent<pEnd; pCurrent++)
	{
		p += sprintf(p, "%s=%s\n", pCurrent->key, pCurrent->value);
	}
	
	evbuffer_add_printf(ev_buf, "Hello World!!!\n");
	evbuffer_add(ev_buf, cbuff, sizeof(cbuff));
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

