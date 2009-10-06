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

		/*
		ZEND_FE(fastdfs_connect_server, NULL)
		ZEND_FE(fastdfs_disconnect_server, NULL)
		ZEND_FE(fastdfs_tracker_list_groups, NULL)
		ZEND_FE(fastdfs_tracker_list_servers, NULL)
		ZEND_FE(fastdfs_tracker_query_storage_store, NULL)
		ZEND_FE(fastdfs_tracker_query_storage_store_without_group, NULL)
		ZEND_FE(fastdfs_tracker_query_storage_store_with_group, NULL)
		ZEND_FE(fastdfs_tracker_query_storage_update, NULL)
		ZEND_FE(fastdfs_tracker_query_storage_fetch, NULL)
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

/*
int fastdfs_tracker_get_connection(string namespace, string object_id, string key, 
		string value [, int expires])
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdfs_tracker_get_connection)
{
	php_fdfs_tracker_get_connection_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_tracker_group);
}

/*
int fastdfs_tracker_list_servers(string namespace, string object_id, array key_list, 
		[, int expires])
return 0 for success, != 0 for error
*/
/*
ZEND_FUNCTION(fastdfs_tracker_list_servers)
{
	php_fdfs_batch_set_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		&g_tracker_group, g_keep_alive);
}
*/

/*
array/int/boolean fastdfs_tracker_query_storage_store(string namespace, string object_id, \
		array key_list, [, bool return_errno, int expires])
return 0 for success, != 0 for error
*/
/*
ZEND_FUNCTION(fastdfs_tracker_query_storage_store)
{
	php_fdfs_batch_get_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		&g_tracker_group, g_keep_alive);
}
*/

/*
int fastdfs_tracker_query_storage_store_without_group(string namespace, string object_id, array key_list)
return 0 for success, != 0 for error
*/
/*
ZEND_FUNCTION(fastdfs_tracker_query_storage_store_without_group)
{
	php_fdfs_batch_delete_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
		&g_tracker_group, g_keep_alive);
}
*/

/*
string/int/boolean fastdfs_connect_server(string namespace, string object_id, string key
		[bool return_errno, int expires])
return string value for success, int value (errno) for error
*/
/*
ZEND_FUNCTION(fastdfs_connect_server)
{
	php_fdfs_get_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_tracker_group, g_keep_alive);
}
*/

/*
string/int/boolean fastdfs_disconnect_server(string namespace, string object_id, string key, 
		int increment [, bool return_errno, int expires])
return string value for success, int value (errno) for error
*/
/*
ZEND_FUNCTION(fastdfs_disconnect_server)
{
	php_fdfs_inc_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_tracker_group, g_keep_alive);
}
*/

/*
int fastdfs_tracker_list_groups(string namespace, string object_id, string key)
return 0 for success, != 0 for error
*/
/*
ZEND_FUNCTION(fastdfs_tracker_list_groups)
{
	php_fdfs_delete_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_tracker_group, g_keep_alive);
}
*/

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

ZEND_BEGIN_ARG_INFO_EX(arginfo_close, 0, 0, 0)
ZEND_END_ARG_INFO()

/* {{{ fdfs_class_methods */
#define FDFS_ME(name, args) PHP_ME(FastDFS, name, args, ZEND_ACC_PUBLIC)
static zend_function_entry fdfs_class_methods[] = {
    FDFS_ME(__construct,        arginfo___construct)
    FDFS_ME(tracker_get_connection,   arginfo_tracker_get_connection)
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

/*
array/int/boolean fastdfs_tracker_query_storage_store_with_group(int server_index[, bool return_errno])
return 0 for success, != 0 for error
*/
/*
ZEND_FUNCTION(fastdfs_tracker_query_storage_store_with_group)
{
	php_fdfs_stat_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_tracker_group, g_keep_alive);
}
*/

/*
array/int/boolean fastdfs_tracker_query_storage_update([bool return_errno])
return 0 for success, != 0 for error
*/
/*
ZEND_FUNCTION(fastdfs_tracker_query_storage_update)
{
	php_fdfs_stat_all_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, \
			&g_tracker_group, g_keep_alive);
}
*/

