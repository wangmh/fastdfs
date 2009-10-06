#ifndef FASTDFS_CLIENT_H
#define FASTDFS_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef PHP_WIN32
#define PHP_FASTDFS_API __declspec(dllexport)
#else
#define PHP_FASTDFS_API
#endif

PHP_MINIT_FUNCTION(fastdfs_client);
PHP_RINIT_FUNCTION(fastdfs_client);
PHP_MSHUTDOWN_FUNCTION(fastdfs_client);
PHP_RSHUTDOWN_FUNCTION(fastdfs_client);
PHP_MINFO_FUNCTION(fastdfs_client);

ZEND_FUNCTION(fastdfs_tracker_get_connection);

PHP_FASTDFS_API zend_class_entry *php_fdfs_get_ce(void);
PHP_FASTDFS_API zend_class_entry *php_fdfs_get_exception(void);
PHP_FASTDFS_API zend_class_entry *php_fdfs_get_exception_base(int root TSRMLS_DC);

#ifdef __cplusplus
}
#endif

#endif	/* FASTDFS_CLIENT_H */
