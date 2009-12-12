#ifndef PHP_BHT_H
#define PHP_BHT_H

extern zend_module_entry bht_module_entry;
#define phpext_bht_ptr &bht_module_entry

#ifdef PHP_WIN32
#define PHP_BHT_API __declspec(dllexport)
#else
#define PHP_BHT_API
#endif

#ifdef ZTS
#include "TSRM.h"
#endif

PHP_MINIT_FUNCTION(bht);
PHP_MSHUTDOWN_FUNCTION(bht);
PHP_RINIT_FUNCTION(bht);
PHP_RSHUTDOWN_FUNCTION(bht);
PHP_MINFO_FUNCTION(bht);

PHP_FUNCTION(bht_open);
PHP_FUNCTION(bht_close);
PHP_FUNCTION(bht_popen);
PHP_FUNCTION(bht_get);
PHP_FUNCTION(bht_set);
PHP_FUNCTION(bht_del);
PHP_FUNCTION(bht_mget);
PHP_FUNCTION(bht_mset);
PHP_FUNCTION(bht_mdel);

// 默认连接的 BHT 服务主机名
#define DEFAULT_BHT_HOST "localhost"
// 默认连接的 BHT 服务端口
#define DEFAULT_BHT_PORT 9090
// PHP 资源名称
#define PHP_BHT_RESOURCE_NAME "BHT Client"

ZEND_BEGIN_MODULE_GLOBALS(bht)
	long num_persistent;	/* 长连接总数 */
	long num_ondemand;		/* 短连接总数 */
ZEND_END_MODULE_GLOBALS(bht)

/* In every utility function you add that needs to use variables 
   in php_bht_globals, call TSRMLS_FETCH(); after declaring other 
   variables used by that function, or better yet, pass in TSRMLS_CC
   after the last function argument and declare your utility function
   with TSRMLS_DC after the last declared argument.  Always refer to
   the globals in your function as BHT_G(variable).  You are 
   encouraged to rename these macros something shorter, see
   examples in any other php module directory.
*/

#ifdef ZTS
#define BHT_G(v) TSRMG(bht_globals_id, zend_bht_globals *, v)
#else
#define BHT_G(v) (bht_globals.v)
#endif

#endif	/* PHP_BHT_H */


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
