#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <php.h>

#ifdef ZTS
#include "TSRM.h"
#endif

#include <SAPI.h>
#include <php_ini.h>
#include "ext/standard/info.h"
#include <zend_extensions.h>
#include <zend_exceptions.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <fdfs_client.h>
#include <logger.h>
#include "fdfs_global.h"
#include "shared_func.h"
#include "client_global.h"
#include "fastdfs_client.h"

typedef struct
{
        TrackerServerGroup *pTrackerGroup;
} FDFSConfigInfo;

typedef struct
{
        zend_object zo;
        FDFSConfigInfo *pConfigInfo;
        TrackerServerGroup *pTrackerGroup;
} php_fdfs_t;

static FDFSConfigInfo *config_list = NULL;
static int config_count = 0;

static int le_fdht;

static zend_class_entry *fdfs_ce = NULL;
static zend_class_entry *fdfs_exception_ce = NULL;
static zend_class_entry *spl_ce_RuntimeException = NULL;

#if (PHP_MAJOR_VERSION == 5 && PHP_MINOR_VERSION < 3)
const zend_fcall_info empty_fcall_info = { 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, 0 };
#undef ZEND_BEGIN_ARG_INFO_EX
#define ZEND_BEGIN_ARG_INFO_EX(name, pass_rest_by_reference, return_reference, required_num_args) \
    static zend_arg_info name[] = {                                                               \
        { NULL, 0, NULL, 0, 0, 0, pass_rest_by_reference, return_reference, required_num_args },
#endif

// Every user visible function must have an entry in fastdfs_client_functions[].
	function_entry fastdfs_client_functions[] = {
		ZEND_FE(fastdfs_tracker_get_connection, NULL)
		ZEND_FE(fastdfs_connect_server, NULL)
		ZEND_FE(fastdfs_disconnect_server, NULL)
		ZEND_FE(fastdfs_tracker_list_groups, NULL)
		ZEND_FE(fastdfs_tracker_query_storage_store, NULL)
		ZEND_FE(fastdfs_tracker_query_storage_update, NULL)
		ZEND_FE(fastdfs_tracker_query_storage_fetch, NULL)
		/*
		ZEND_FE(fastdfs_tracker_query_storage_list, NULL)
		*/

		{NULL, NULL, NULL}  /* Must be the last line */
	};

zend_module_entry fastdfs_client_module_entry = {
	STANDARD_MODULE_HEADER,
	"fastdfs_client",
	fastdfs_client_functions,
	PHP_MINIT(fastdfs_client),
	PHP_MSHUTDOWN(fastdfs_client),
	NULL,//PHP_RINIT(fastdfs_client),
	NULL,//PHP_RSHUTDOWN(fastdfs_client),
	PHP_MINFO(fastdfs_client),
	"1.00", 
	STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_FASTDFS_CLIENT
	ZEND_GET_MODULE(fastdfs_client)
#endif

static void php_fdfs_tracker_get_connection_impl(INTERNAL_FUNCTION_PARAMETERS, \
		TrackerServerGroup *pTrackerGroup)
{
	int argc;
	TrackerServerInfo *pTrackerServer;

	argc = ZEND_NUM_ARGS();
	if (argc != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdfs_tracker_get_connection parameters count: %d != 0", 
			__LINE__, argc);
		RETURN_BOOL(false);
	}

	pTrackerServer = tracker_get_connection_ex(pTrackerGroup);
	if (pTrackerServer == NULL)
	{
		RETURN_BOOL(false);
	}

	array_init(return_value);
	
	add_assoc_stringl_ex(return_value, "ip_addr", sizeof("ip_addr"), \
		pTrackerServer->ip_addr, strlen(pTrackerServer->ip_addr), 1);
	add_assoc_long_ex(return_value, "port", sizeof("port"), \
		pTrackerServer->port);
	add_assoc_long_ex(return_value, "sock", sizeof("sock"), \
		pTrackerServer->sock);
}

static void php_fdfs_connect_server_impl(INTERNAL_FUNCTION_PARAMETERS)
{
	int argc;
	char *ip_addr;
	int ip_len;
	long port;
	TrackerServerInfo server_info;

	argc = ZEND_NUM_ARGS();
	if (argc != 2)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdfs_connect_server parameters count: %d != 2", \
			__LINE__, argc);
		RETURN_BOOL(false);
	}

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sl", \
				&ip_addr, &ip_len, &port) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
				"zend_parse_parameters fail!", __LINE__);
		RETURN_BOOL(false);
	}

	snprintf(server_info.ip_addr, sizeof(server_info.ip_addr), \
		"%s", ip_addr);
	server_info.port = port;

	if (tracker_connect_server(&server_info) == 0)
	{
		array_init(return_value);
		add_assoc_stringl_ex(return_value, "ip_addr", \
			sizeof("ip_addr"), ip_addr, ip_len, 1);
		add_assoc_long_ex(return_value, "port", sizeof("port"), \
			port);
		add_assoc_long_ex(return_value, "sock", sizeof("sock"), \
			server_info.sock);
	}
	else
	{
		RETURN_BOOL(false);
	}
}

static void php_fdfs_disconnect_server_impl(INTERNAL_FUNCTION_PARAMETERS)
{
	int argc;
	zval *server_info;
	HashTable *server_hash;
	zval **data;
	zval ***ppp;
	int sock;

	argc = ZEND_NUM_ARGS();
	if (argc != 1)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdfs_disconnect_server parameters count: %d != 1", \
			__LINE__, argc);
		RETURN_BOOL(false);
	}

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "a", \
				&server_info) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
				"zend_parse_parameters fail!", __LINE__);
		RETURN_BOOL(false);
	}

	server_hash = Z_ARRVAL_P(server_info);
	data = NULL;
	ppp = &data;
	if (zend_hash_find(server_hash, "sock", sizeof("sock"), \
			(void **)ppp) == FAILURE)
	{
		RETURN_BOOL(false);
	}

	if ((*data)->type == IS_LONG)
	{
		sock = (*data)->value.lval;
		if (sock > 0)
		{
			close(sock);
		}
		RETURN_BOOL(true);
	}
	else
	{
		RETURN_BOOL(false);
	}
}

static int php_fdfs_get_tracker_from_hash(HashTable *server_hash, \
		TrackerServerInfo *pTrackerServer)
{
	zval **data;
	zval ***ppp;
	char *ip_addr;
	int ip_len;

	memset(pTrackerServer, 0, sizeof(TrackerServerInfo));
	data = NULL;
	ppp = &data;
	if (zend_hash_find(server_hash, "ip_addr", sizeof("ip_addr"), \
			(void **)ppp) == FAILURE)
	{
		return ENOENT;
	}
	if ((*data)->type != IS_STRING)
	{
		return EINVAL;
	}
	ip_addr = Z_STRVAL_PP(data);
	ip_len = Z_STRLEN_PP(data);
	if (ip_len >= IP_ADDRESS_SIZE)
	{
		ip_len = IP_ADDRESS_SIZE - 1;
	}
	memcpy(pTrackerServer->ip_addr, ip_addr, ip_len);

	if (zend_hash_find(server_hash, "port", sizeof("port"), \
			(void **)ppp) == FAILURE)
	{
		return ENOENT;
	}
	if ((*data)->type != IS_LONG)
	{
		return EINVAL;
	}
	pTrackerServer->port = (*data)->value.lval;

	if (zend_hash_find(server_hash, "sock", sizeof("sock"), \
			(void **)ppp) == FAILURE)
	{
		return ENOENT;
	}
	if ((*data)->type != IS_LONG)
	{
		return EINVAL;
	}

	pTrackerServer->sock = (*data)->value.lval;
	return 0;
}

static void php_fdfs_tracker_list_groups_impl(INTERNAL_FUNCTION_PARAMETERS, \
		TrackerServerGroup *pTrackerGroup)
{
	int argc;
	char *group_name;
	int group_nlen;
	zval *server_info;
	zval *group_info_array;
	zval *server_info_array;
	HashTable *server_hash;
	TrackerServerInfo tracker_server;
	TrackerServerInfo *pTrackerServer;
	FDFSGroupStat group_stats[FDFS_MAX_GROUPS];
	FDFSGroupStat *pGroupStat;
	FDFSGroupStat *pGroupEnd;
	int group_count;
	int result;
        int storage_count;
	FDFSStorageInfo storage_infos[FDFS_MAX_SERVERS_EACH_GROUP];
	FDFSStorageInfo *pStorage;
	FDFSStorageInfo *pStorageEnd;
	FDFSStorageStat *pStorageStat;

	argc = ZEND_NUM_ARGS();
	if (argc > 2)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdfs_tracker_list_groups parameters count: %d > 2", 
			__LINE__, argc);
		RETURN_BOOL(false);
	}

	group_name = NULL;
	group_nlen = 0;
	if (argc == 0)
	{
		pTrackerServer = tracker_get_connection_ex(pTrackerGroup);
		if (pTrackerServer == NULL)
		{
			RETURN_BOOL(false);
		}
	}
	else
	{
		if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "a|s", \
			&server_info, &group_name, &group_nlen) == FAILURE)
		{
			logError("file: "__FILE__", line: %d, " \
					"zend_parse_parameters fail!", __LINE__);
			RETURN_BOOL(false);
		}

		pTrackerServer = &tracker_server;
		server_hash = Z_ARRVAL_P(server_info);
		if ((result=php_fdfs_get_tracker_from_hash(server_hash, \
				pTrackerServer)) != 0)
		{
			RETURN_BOOL(false);
		}
	}

	if ((result=tracker_list_groups(pTrackerServer, group_stats, \
		FDFS_MAX_GROUPS, &group_count)) != 0)
	{
		RETURN_BOOL(false);
	}

	array_init(return_value);

	pGroupEnd = group_stats + group_count;
	for (pGroupStat=group_stats; pGroupStat<pGroupEnd; pGroupStat++)
	{
		if (group_name != NULL && strcmp(pGroupStat->group_name, \
			group_name) != 0)
		{
			continue;
		}

		ALLOC_INIT_ZVAL(group_info_array);
		array_init(group_info_array);

		add_assoc_zval_ex(return_value, pGroupStat->group_name, \
			strlen(pGroupStat->group_name) + 1, group_info_array);

		add_assoc_long_ex(group_info_array, "free_space", \
			sizeof("free_space"), pGroupStat->free_mb);
		add_assoc_long_ex(group_info_array, "server_count", \
			sizeof("server_count"), pGroupStat->count);
		add_assoc_long_ex(group_info_array, "active_count", \
			sizeof("active_count"), pGroupStat->active_count);
		add_assoc_long_ex(group_info_array, "storage_port", \
			sizeof("storage_port"), pGroupStat->storage_port);
		add_assoc_long_ex(group_info_array, "storage_http_port", \
			sizeof("storage_http_port"), \
			pGroupStat->storage_http_port);
		add_assoc_long_ex(group_info_array, "store_path_count", \
			sizeof("store_path_count"), \
			pGroupStat->store_path_count);
		add_assoc_long_ex(group_info_array, "subdir_count_per_path", \
			sizeof("subdir_count_per_path"), \
			pGroupStat->subdir_count_per_path);
		add_assoc_long_ex(group_info_array, "current_write_server", \
			sizeof("current_write_server"), \
			pGroupStat->current_write_server);

		       
		result = tracker_list_servers(pTrackerServer, \
				pGroupStat->group_name, \
				storage_infos, FDFS_MAX_SERVERS_EACH_GROUP, \
				&storage_count);
		if (result != 0)
		{       
			RETURN_BOOL(false);
		}

		pStorageEnd = storage_infos + storage_count;
		for (pStorage=storage_infos; pStorage<pStorageEnd; \
				pStorage++)
		{
			ALLOC_INIT_ZVAL(server_info_array);
			array_init(server_info_array);

			add_assoc_zval_ex(group_info_array, pStorage->ip_addr, \
				strlen(pStorage->ip_addr)+1, server_info_array);

			pStorageStat = &(pStorage->stat);

			add_assoc_long_ex(server_info_array, "status", \
				sizeof("status"), pStorage->status);
			add_assoc_long_ex(server_info_array, "total_space", \
				sizeof("total_space"), pStorage->total_mb);
			add_assoc_long_ex(server_info_array, "free_space", \
				sizeof("free_space"), pStorage->free_mb);

			add_assoc_long_ex(server_info_array, \
				"total_upload_count", \
				sizeof("total_upload_count"), \
				pStorageStat->total_upload_count);

			add_assoc_long_ex(server_info_array, \
				"success_upload_count", \
				sizeof("success_upload_count"), \
				pStorageStat->success_upload_count);

			add_assoc_long_ex(server_info_array, \
				"total_set_meta_count", \
				sizeof("total_set_meta_count"), \
				pStorageStat->total_set_meta_count);

			add_assoc_long_ex(server_info_array, \
				"success_set_meta_count", \
				sizeof("success_set_meta_count"), \
				pStorageStat->success_set_meta_count);

			add_assoc_long_ex(server_info_array, \
				"total_delete_count", \
				sizeof("total_delete_count"), \
				pStorageStat->total_delete_count);

			add_assoc_long_ex(server_info_array, \
				"success_delete_count", \
				sizeof("success_delete_count"), \
				pStorageStat->success_delete_count);

			add_assoc_long_ex(server_info_array, \
				"total_download_count", \
				sizeof("total_download_count"), \
				pStorageStat->total_download_count);

			add_assoc_long_ex(server_info_array, \
				"success_download_count", \
				sizeof("success_download_count"), \
				pStorageStat->success_download_count);

			add_assoc_long_ex(server_info_array, \
				"total_get_meta_count", \
				sizeof("total_get_meta_count"), \
				pStorageStat->total_get_meta_count);

			add_assoc_long_ex(server_info_array, \
				"success_get_meta_count", \
				sizeof("success_get_meta_count"), \
				pStorageStat->success_get_meta_count);

			add_assoc_long_ex(server_info_array, \
				"total_create_link_count", \
				sizeof("total_create_link_count"), \
				pStorageStat->total_create_link_count);

			add_assoc_long_ex(server_info_array, \
				"success_create_link_count", \
				sizeof("success_create_link_count"), \
				pStorageStat->success_create_link_count);

			add_assoc_long_ex(server_info_array, \
				"total_delete_link_count", \
				sizeof("total_delete_link_count"), \
				pStorageStat->total_delete_link_count);

			add_assoc_long_ex(server_info_array, \
				"success_delete_link_count", \
				sizeof("success_delete_link_count"), \
				pStorageStat->success_delete_link_count);

			add_assoc_long_ex(server_info_array, \
				"last_heart_beat_time", \
				sizeof("last_heart_beat_time"), \
				pStorageStat->last_heart_beat_time);

			add_assoc_long_ex(server_info_array, \
				"last_source_update", \
				sizeof("last_source_update"), \
				pStorageStat->last_source_update);

			add_assoc_long_ex(server_info_array, \
				"last_sync_update", \
				sizeof("last_sync_update"), \
				pStorageStat->last_sync_update);

			add_assoc_long_ex(server_info_array, \
				"last_synced_timestamp", \
				sizeof("last_synced_timestamp"), \
				pStorageStat->last_synced_timestamp);
		}
	}
}

static void php_fdfs_tracker_query_storage_store_impl( \
		INTERNAL_FUNCTION_PARAMETERS, \
		TrackerServerGroup *pTrackerGroup)
{
	int argc;
	char *group_name;
	int group_nlen;
	zval *server_info;
	HashTable *server_hash;
	TrackerServerInfo tracker_server;
	TrackerServerInfo storage_server;
	TrackerServerInfo *pTrackerServer;
	int store_path_index;
	int result;

    	argc = ZEND_NUM_ARGS();
	if (argc > 2)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdfs_tracker_query_storage_store parameters " \
			"count: %d > 2", __LINE__, argc);
		RETURN_BOOL(false);
	}

	group_name = NULL;
	group_nlen = 0;
	if (argc == 0)
	{
		pTrackerServer = tracker_get_connection_ex(pTrackerGroup);
		if (pTrackerServer == NULL)
		{
			RETURN_BOOL(false);
		}
	}
	else
	{
		if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "a|s", \
			&server_info, &group_name, &group_nlen) == FAILURE)
		{
			logError("file: "__FILE__", line: %d, " \
					"zend_parse_parameters fail!", __LINE__);
			RETURN_BOOL(false);
		}

		pTrackerServer = &tracker_server;
		server_hash = Z_ARRVAL_P(server_info);
		if ((result=php_fdfs_get_tracker_from_hash(server_hash, \
				pTrackerServer)) != 0)
		{
			RETURN_BOOL(false);
		}
	}

	if (group_name != NULL && group_nlen > 0)
	{
		result = tracker_query_storage_store_with_group(pTrackerServer,\
                	group_name, &storage_server, &store_path_index);
	}
	else
	{
		result = tracker_query_storage_store_without_group( \
			pTrackerServer, &storage_server, &store_path_index);
	}

	if (result != 0)
	{
		RETURN_BOOL(false);
	}

	array_init(return_value);
	add_assoc_stringl_ex(return_value, "ip_addr", \
			sizeof("ip_addr"), storage_server.ip_addr, \
			strlen(storage_server.ip_addr), 1);
	add_assoc_long_ex(return_value, "port", sizeof("port"), \
			storage_server.port);
	add_assoc_long_ex(return_value, "sock", sizeof("sock"), -1);
	add_assoc_long_ex(return_value, "store_path_index", \
			sizeof("store_path_index"), \
			store_path_index);
}

static void php_fdfs_tracker_do_query_storage_impl( \
		INTERNAL_FUNCTION_PARAMETERS, \
		TrackerServerGroup *pTrackerGroup, const byte cmd)
{
	int argc;
	char *group_name;
	char *remote_filename;
	int group_nlen;
	int filename_len;
	zval *server_info;
	HashTable *server_hash;
	TrackerServerInfo tracker_server;
	TrackerServerInfo storage_server;
	TrackerServerInfo *pTrackerServer;
	int result;

    	argc = ZEND_NUM_ARGS();
	if (argc < 2 || argc > 3)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker_do_query_storage parameters " \
			"count: %d < 2 or > 3", __LINE__, argc);
		RETURN_BOOL(false);
	}

	server_info = NULL;
	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss|a", \
		&group_name, &group_nlen, &remote_filename, &filename_len, \
		&server_info) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
				"zend_parse_parameters fail!", __LINE__);
		RETURN_BOOL(false);
	}

	if (server_info == NULL)
	{
		pTrackerServer = tracker_get_connection_ex(pTrackerGroup);
		if (pTrackerServer == NULL)
		{
			RETURN_BOOL(false);
		}
	}
	else
	{
		pTrackerServer = &tracker_server;
		server_hash = Z_ARRVAL_P(server_info);
		if ((result=php_fdfs_get_tracker_from_hash(server_hash, \
				pTrackerServer)) != 0)
		{
			RETURN_BOOL(false);
		}
	}

	result = tracker_do_query_storage(pTrackerServer, &storage_server, \
			cmd, group_name, remote_filename);

	if (result != 0)
	{
		RETURN_BOOL(false);
	}

	array_init(return_value);
	add_assoc_stringl_ex(return_value, "ip_addr", \
			sizeof("ip_addr"), storage_server.ip_addr, \
			strlen(storage_server.ip_addr), 1);
	add_assoc_long_ex(return_value, "port", sizeof("port"), \
			storage_server.port);
	add_assoc_long_ex(return_value, "sock", sizeof("sock"), -1);
}

/*
array fastdfs_tracker_get_connection()
return array for success, false for error
*/
ZEND_FUNCTION(fastdfs_tracker_get_connection)
{
	php_fdfs_tracker_get_connection_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_tracker_group);
}

/*
int fastdfs_connect_server(string ip_addr, int port)
return array for success, false for error
*/
ZEND_FUNCTION(fastdfs_connect_server)
{
	php_fdfs_connect_server_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

/*
boolean fastdfs_disconnect_server(array serverInfo)
return true for success, false for error
*/
ZEND_FUNCTION(fastdfs_disconnect_server)
{
	php_fdfs_disconnect_server_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

/*
array fastdfs_tracker_list_groups([array tracker_server, string group_name])
return array for success, false for error
*/
ZEND_FUNCTION(fastdfs_tracker_list_groups)
{
	php_fdfs_tracker_list_groups_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_tracker_group);
}

/*
array fastdfs_tracker_query_storage_store([array tracker_server, 
			string group_name])
return array for success, false for error
*/
ZEND_FUNCTION(fastdfs_tracker_query_storage_store)
{
	php_fdfs_tracker_query_storage_store_impl( \
		INTERNAL_FUNCTION_PARAM_PASSTHRU, &g_tracker_group);
}

/*
array fastdfs_tracker_query_storage_update(string group_name, 
		string remote_filename [, array tracker_server])
return array for success, false for error
*/
ZEND_FUNCTION(fastdfs_tracker_query_storage_update)
{
	php_fdfs_tracker_do_query_storage_impl( \
		INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		&g_tracker_group, TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE);
}

/*
array fastdfs_tracker_query_storage_fetch(string group_name, 
		string remote_filename [, array tracker_server])
return array for success, false for error
*/
ZEND_FUNCTION(fastdfs_tracker_query_storage_fetch)
{
	php_fdfs_tracker_do_query_storage_impl( \
		INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		&g_tracker_group, TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE);
}

static void php_fdfs_close(php_fdfs_t *i_obj TSRMLS_DC)
{
	if (i_obj->pTrackerGroup == NULL)
	{
		return;
	}

	if (i_obj->pTrackerGroup != i_obj->pConfigInfo->pTrackerGroup)
	{
		tracker_close_all_connections_ex(i_obj->pTrackerGroup);
	}
}

/* constructor/destructor */
static void php_fdfs_destroy(php_fdfs_t *i_obj TSRMLS_DC)
{
	php_fdfs_close(i_obj);
	if (i_obj->pTrackerGroup != NULL && i_obj->pTrackerGroup != \
		i_obj->pConfigInfo->pTrackerGroup)
	{
		fdfs_client_destroy_ex(i_obj->pTrackerGroup);
		efree(i_obj->pTrackerGroup);
		i_obj->pTrackerGroup = NULL;
	}

	efree(i_obj);
}

ZEND_RSRC_DTOR_FUNC(php_fdfs_dtor)
{
	if (rsrc->ptr != NULL)
	{
		php_fdfs_t *i_obj = (php_fdfs_t *)rsrc->ptr;
		php_fdfs_destroy(i_obj TSRMLS_CC);
		rsrc->ptr = NULL;
	}
}

/* FastDFS::__construct([int config_index = 0, bool bMultiThread = false])
   Creates a FastDFS object */
static PHP_METHOD(FastDFS, __construct)
{
	long config_index;
	bool bMultiThread;
	zval *object = getThis();
	php_fdfs_t *i_obj;

	config_index = 0;
	bMultiThread = false;
	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "|lb", \
			&config_index, &bMultiThread) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"zend_parse_parameters fail!", __LINE__);
		ZVAL_NULL(object);
		return;
	}

	if (config_index < 0 || config_index >= config_count)
	{
		logError("file: "__FILE__", line: %d, " \
			"invalid config_index: %d < 0 || >= %d", \
			__LINE__, config_index, config_count);
		ZVAL_NULL(object);
		return;
	}

	i_obj = (php_fdfs_t *) zend_object_store_get_object(object TSRMLS_CC);
	i_obj->pConfigInfo = config_list + config_index;
	if (bMultiThread)
	{
		i_obj->pTrackerGroup = (TrackerServerGroup *)emalloc( \
					sizeof(TrackerServerGroup));
		if (i_obj->pTrackerGroup == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail!", __LINE__, \
				sizeof(TrackerServerGroup));
			ZVAL_NULL(object);
			return;
		}

		if (fdfs_copy_tracker_group(i_obj->pTrackerGroup, \
			i_obj->pConfigInfo->pTrackerGroup) != 0)
		{
			ZVAL_NULL(object);
			return;
		}
	}
	else
	{
		i_obj->pTrackerGroup = i_obj->pConfigInfo->pTrackerGroup;
	}
}

/*
int FastDFS::set(string namespace, string object_id, string key, 
		string value [, int expires])
return 0 for success, != 0 for error
*/
PHP_METHOD(FastDFS, tracker_get_connection)
{
	zval *object = getThis();
	php_fdfs_t *i_obj;

	i_obj = (php_fdfs_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdfs_tracker_get_connection_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pTrackerGroup);
}

PHP_METHOD(FastDFS, connect_server)
{
	php_fdfs_connect_server_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(FastDFS, disconnect_server)
{
	php_fdfs_disconnect_server_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

PHP_METHOD(FastDFS, tracker_list_groups)
{
	zval *object = getThis();
	php_fdfs_t *i_obj;

	i_obj = (php_fdfs_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdfs_tracker_list_groups_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pTrackerGroup);
}

PHP_METHOD(FastDFS, tracker_query_storage_store)
{
	zval *object = getThis();
	php_fdfs_t *i_obj;

	i_obj = (php_fdfs_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdfs_tracker_query_storage_store_impl( \
			INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			i_obj->pTrackerGroup);
}

PHP_METHOD(FastDFS, tracker_query_storage_update)
{
	zval *object = getThis();
	php_fdfs_t *i_obj;

	i_obj = (php_fdfs_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdfs_tracker_do_query_storage_impl( \
		INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		i_obj->pTrackerGroup, TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE);
}

PHP_METHOD(FastDFS, tracker_query_storage_fetch)
{
	zval *object = getThis();
	php_fdfs_t *i_obj;

	i_obj = (php_fdfs_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdfs_tracker_do_query_storage_impl( \
		INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		i_obj->pTrackerGroup,TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE);
}

/*
void FastDFS::close()
*/
PHP_METHOD(FastDFS, close)
{
	zval *object = getThis();
	php_fdfs_t *i_obj;

	i_obj = (php_fdfs_t *) zend_object_store_get_object(object TSRMLS_CC);
	php_fdfs_close(i_obj);
}

ZEND_BEGIN_ARG_INFO_EX(arginfo___construct, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_tracker_get_connection, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_connect_server, 0, 0, 2)
ZEND_ARG_INFO(0, ip_addr)
ZEND_ARG_INFO(0, port)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_disconnect_server, 0, 0, 1)
ZEND_ARG_INFO(0, server_info)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_tracker_list_groups, 0, 0, 0)
ZEND_ARG_INFO(0, tracker_server)
ZEND_ARG_INFO(0, group_name)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_tracker_query_storage_store, 0, 0, 0)
ZEND_ARG_INFO(0, tracker_server)
ZEND_ARG_INFO(0, group_name)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_tracker_query_storage_update, 0, 0, 2)
ZEND_ARG_INFO(0, group_name)
ZEND_ARG_INFO(0, remote_filename)
ZEND_ARG_INFO(0, tracker_server)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_tracker_query_storage_fetch, 0, 0, 2)
ZEND_ARG_INFO(0, group_name)
ZEND_ARG_INFO(0, remote_filename)
ZEND_ARG_INFO(0, tracker_server)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_close, 0, 0, 0)
ZEND_END_ARG_INFO()

/* {{{ fdfs_class_methods */
#define FDFS_ME(name, args) PHP_ME(FastDFS, name, args, ZEND_ACC_PUBLIC)
static zend_function_entry fdfs_class_methods[] = {
    FDFS_ME(__construct,        arginfo___construct)
    FDFS_ME(tracker_get_connection,   arginfo_tracker_get_connection)
    FDFS_ME(connect_server,   arginfo_connect_server)
    FDFS_ME(disconnect_server,   arginfo_disconnect_server)
    FDFS_ME(tracker_list_groups,   arginfo_tracker_list_groups)
    FDFS_ME(tracker_query_storage_store,   arginfo_tracker_query_storage_store)
    FDFS_ME(tracker_query_storage_update,   arginfo_tracker_query_storage_update)
    FDFS_ME(tracker_query_storage_fetch,   arginfo_tracker_query_storage_fetch)
    FDFS_ME(close,              arginfo_close)
    { NULL, NULL, NULL }
};
#undef FDFS_ME
/* }}} */

static void php_fdfs_free_storage(php_fdfs_t *i_obj TSRMLS_DC)
{
	zend_object_std_dtor(&i_obj->zo TSRMLS_CC);
	php_fdfs_destroy(i_obj TSRMLS_CC);
}

zend_object_value php_fdfs_new(zend_class_entry *ce TSRMLS_DC)
{
	zend_object_value retval;
	php_fdfs_t *i_obj;
	zval *tmp;

	i_obj = ecalloc(1, sizeof(php_fdfs_t));

	zend_object_std_init( &i_obj->zo, ce TSRMLS_CC );
	zend_hash_copy(i_obj->zo.properties, &ce->default_properties, \
		(copy_ctor_func_t) zval_add_ref, (void *)&tmp, sizeof(zval *));

	retval.handle = zend_objects_store_put(i_obj, \
		(zend_objects_store_dtor_t)zend_objects_destroy_object, \
		(zend_objects_free_object_storage_t)php_fdfs_free_storage, \
		NULL TSRMLS_CC);
	retval.handlers = zend_get_std_object_handlers();

	return retval;
}

PHP_FASTDFS_API zend_class_entry *php_fdfs_get_ce(void)
{
	return fdfs_ce;
}

PHP_FASTDFS_API zend_class_entry *php_fdfs_get_exception(void)
{
	return fdfs_exception_ce;
}

PHP_FASTDFS_API zend_class_entry *php_fdfs_get_exception_base(int root TSRMLS_DC)
{
#if HAVE_SPL
	if (!root)
	{
		if (!spl_ce_RuntimeException)
		{
			zend_class_entry **pce;
			zend_class_entry ***ppce;

			ppce = &pce;
			if (zend_hash_find(CG(class_table), "runtimeexception",
			   sizeof("RuntimeException"), (void **) ppce) == SUCCESS)
			{
				spl_ce_RuntimeException = *pce;
				return *pce;
			}
		}
		else
		{
			return spl_ce_RuntimeException;
		}
	}
#endif
#if (PHP_MAJOR_VERSION == 5) && (PHP_MINOR_VERSION < 2)
	return zend_exception_get_default();
#else
	return zend_exception_get_default(TSRMLS_C);
#endif
}

static int load_config_files()
{
	#define ITEM_NAME_CONF_COUNT "fastdfs_client.tracker_group_count"
	#define ITEM_NAME_CONF_FILE  "fastdfs_client.tracker_group"
	zval conf_c;
	zval conf_filename;
	char szItemName[sizeof(ITEM_NAME_CONF_FILE) + 10];
	int nItemLen;
	FDFSConfigInfo *pConfigInfo;
	FDFSConfigInfo *pConfigEnd;
	int result;

	if (zend_get_configuration_directive(ITEM_NAME_CONF_COUNT, 
		sizeof(ITEM_NAME_CONF_COUNT), &conf_c) == SUCCESS)
	{
		config_count = atoi(conf_c.value.str.val);
		if (config_count <= 0)
		{
			fprintf(stderr, "file: "__FILE__", line: %d, " \
				"fastdfs_client.ini, config_count: %d <= 0!\n",\
				__LINE__, config_count);
			return EINVAL;
		}
	}
	else
	{
		 config_count = 1;
	}

	config_list = (FDFSConfigInfo *)malloc(sizeof(FDFSConfigInfo) * \
			config_count);
	if (config_list == NULL)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"malloc %d bytes fail!\n",\
			__LINE__, (int)sizeof(FDFSConfigInfo) * config_count);
		return errno != 0 ? errno : ENOMEM;
	}

	pConfigEnd = config_list + config_count;
	for (pConfigInfo=config_list; pConfigInfo<pConfigEnd; pConfigInfo++)
	{
		nItemLen = sprintf(szItemName, "%s%d", ITEM_NAME_CONF_FILE, \
				(int)(pConfigInfo - config_list));
		if (zend_get_configuration_directive(szItemName, \
			nItemLen + 1, &conf_filename) != SUCCESS)
		{
			if (pConfigInfo != config_list)
			{
				fprintf(stderr, "file: "__FILE__", line: %d, " \
					"fastdfs_client.ini: get param %s " \
					"fail!\n", __LINE__, szItemName);

				return ENOENT;
			}

			if (zend_get_configuration_directive( \
				ITEM_NAME_CONF_FILE, \
				sizeof(ITEM_NAME_CONF_FILE), \
				&conf_filename) != SUCCESS)
			{
				fprintf(stderr, "file: "__FILE__", line: %d, " \
					"fastdfs_client.ini: get param %s " \
					"fail!\n",__LINE__,ITEM_NAME_CONF_FILE);

				return ENOENT;
			}
		}

		if (pConfigInfo == config_list) //first config file
		{
			pConfigInfo->pTrackerGroup = &g_tracker_group;
			result = fdfs_client_init(conf_filename.value.str.val);
			if (result != 0)
			{
				return result;
			}
		}
		else
		{
			pConfigInfo->pTrackerGroup = (TrackerServerGroup *)malloc( \
							sizeof(TrackerServerGroup));
			if (pConfigInfo->pTrackerGroup == NULL)
			{
				fprintf(stderr, "file: "__FILE__", line: %d, " \
					"malloc %d bytes fail!\n", \
					__LINE__, (int)sizeof(TrackerServerGroup));
				return errno != 0 ? errno : ENOMEM;
			}

			if ((result=fdfs_load_tracker_group(\
				pConfigInfo->pTrackerGroup, 
				conf_filename.value.str.val)) != 0)
			{
				return result;
			}
		}
	}

	logInfo("base_path=%s, network_timeout=%d, " \
		"tracker_group_count=%d, first tracker group server_count=%d", \
		g_base_path, g_network_timeout, \
		config_count, g_tracker_group.server_count);

	return 0;
}

PHP_MINIT_FUNCTION(fastdfs_client)
{
	zend_class_entry ce;

	if (load_config_files() != 0)
	{
		return FAILURE;
	}

	le_fdht = zend_register_list_destructors_ex(NULL, php_fdfs_dtor, \
			"FastDFS", module_number);

	INIT_CLASS_ENTRY(ce, "FastDFS", fdfs_class_methods);
	fdfs_ce = zend_register_internal_class(&ce TSRMLS_CC);
	fdfs_ce->create_object = php_fdfs_new;

	INIT_CLASS_ENTRY(ce, "FastDFSException", NULL);
	fdfs_exception_ce = zend_register_internal_class_ex(&ce, \
		php_fdfs_get_exception_base(0 TSRMLS_CC), NULL TSRMLS_CC);

	return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(fastdfs_client)
{
	FDFSConfigInfo *pConfigInfo;
	FDFSConfigInfo *pConfigEnd;

	if (config_list != NULL)
	{
		pConfigEnd = config_list + config_count;
		for (pConfigInfo=config_list; pConfigInfo<pConfigEnd; \
			pConfigInfo++)
		{
			if (pConfigInfo->pTrackerGroup != NULL)
			{
				tracker_close_all_connections_ex( \
						pConfigInfo->pTrackerGroup);
			}
		}
	}

	fdfs_client_destroy();

	return SUCCESS;
}

PHP_RINIT_FUNCTION(fastdfs_client)
{
	return SUCCESS;
}

PHP_RSHUTDOWN_FUNCTION(fastdfs_client)
{
	fprintf(stderr, "request shut down. file: "__FILE__", line: %d\n", __LINE__);
	return SUCCESS;
}

PHP_MINFO_FUNCTION(fastdfs_client)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "fastdfs_client support", "enabled");
	php_info_print_table_end();

}

