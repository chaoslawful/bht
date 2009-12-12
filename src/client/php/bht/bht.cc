#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_bht.h"

#include <string>
#include <map>
#include <set>
#include <stdexcept>
#include "Client.h"

using namespace ::std;
using namespace ::BHT;

/* True globals, no need for thread safety */
// 自定义 PHP 资源类型描述符
static int le_bht_resource;
static int le_bht_resource_persistent;

ZEND_DECLARE_MODULE_GLOBALS(bht)

/*  bht_functions[] */
/* Every user visible function must have an entry in bht_functions[]. {{{
 */
zend_function_entry bht_functions[] = {
	PHP_FE(bht_open, NULL)
	PHP_FE(bht_close, NULL)
	PHP_FE(bht_popen, NULL)
	PHP_FE(bht_get, NULL)
	PHP_FE(bht_set, NULL)
	PHP_FE(bht_del, NULL)
	PHP_FE(bht_mget, NULL)
	PHP_FE(bht_mset, NULL)
	PHP_FE(bht_mdel, NULL)
	{NULL, NULL, NULL}	/* Must be the last line in bht_functions[] */
};
/* }}} */

/*  bht_module_entry */
/* {{{ */
zend_module_entry bht_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
	STANDARD_MODULE_HEADER,
#endif
	"bht",
	bht_functions,
	PHP_MINIT(bht),
	PHP_MSHUTDOWN(bht),
	PHP_RINIT(bht),		/* Replace with NULL if there's nothing to do at request start */
	PHP_RSHUTDOWN(bht),	/* Replace with NULL if there's nothing to do at request end */
	PHP_MINFO(bht),
#if ZEND_MODULE_API_NO >= 20010901
	"0.1", /* Replace with version number for your extension */
#endif
	STANDARD_MODULE_PROPERTIES
};
/* }}} */

#ifdef COMPILE_DL_BHT
ZEND_GET_MODULE(bht)
#endif

/* BHT 扩展模块全局变量构造函数 */
/* {{{ */
static void php_bht_globals_ctor(zend_bht_globals *bht_globals TSRMLS_DC)
{
#if defined(DEBUG)
	php_error_docref(NULL TSRMLS_CC, E_NOTICE, "BHT module globals ctor called!");
#endif
	bht_globals->num_persistent = 0;
	bht_globals->num_ondemand = 0;
}
/* }}} */

/* BHT 扩展模块全局变量析构函数 */
/* {{{ */
static void php_bht_globals_dtor(zend_bht_globals *bht_globals TSRMLS_DC)
{
#if defined(DEBUG)
	php_error_docref(NULL TSRMLS_CC, E_NOTICE, "BHT module globals dtor called!");
#endif
}
/* }}} */

/* BHT PHP 短连接资源析构函数 */
/* {{{ */
static void php_bht_resource_dtor(zend_rsrc_list_entry *rsrc TSRMLS_DC)
{
#if defined(DEBUG)
	php_error_docref(NULL TSRMLS_CC, E_NOTICE, "BHT on-demand resource dtor called!");
#endif
	Client *p = (Client*)(rsrc->ptr);
	if(p) {
		delete p;
		// 减少短连接统计计数
		--BHT_G(num_ondemand);
	}
}
/* }}} */

/* BHT PHP 长连接资源析构函数 */
/* {{{ */
static void php_bht_resource_dtor_persistent(zend_rsrc_list_entry *rsrc TSRMLS_DC)
{
#if defined(DEBUG)
	php_error_docref(NULL TSRMLS_CC, E_NOTICE, "BHT persistent resource dtor called!");
#endif
	Client *p = (Client*)(rsrc->ptr);
	if(p) {
		delete p;
		// 减少长连接统计计数
		--BHT_G(num_persistent);
	}
}
/* }}} */

/*  PHP_MINIT_FUNCTION */
/* {{{ */
PHP_MINIT_FUNCTION(bht)
{
#if defined(DEBUG)
	php_error_docref(NULL TSRMLS_CC, E_NOTICE, "BHT module init function called!");
#endif

	// 注册 BHT Client PHP 资源类型
	le_bht_resource = zend_register_list_destructors_ex(php_bht_resource_dtor, NULL, PHP_BHT_RESOURCE_NAME, module_number);
	le_bht_resource_persistent =
		zend_register_list_destructors_ex(NULL, php_bht_resource_dtor_persistent, PHP_BHT_RESOURCE_NAME, module_number);

#ifdef ZTS
	// 在线程化PHP中注册全局变量标识并注册其构造和析构函数
	ts_allocate_id(&bht_globals_id,
			sizeof(zend_bht_globals),
			(ts_allocate_ctor)php_bht_globals_ctor,
			(ts_allocate_dtor)php_bht_globals_dtor);
#else
	// 在非线程化PHP中显式调用全局变量构造函数
	php_bht_globals_ctor(&bht_globals TSRMLS_CC);
#endif
	return SUCCESS;
}
/* }}} */

/*  PHP_MSHUTDOWN_FUNCTION */
/* {{{ */
PHP_MSHUTDOWN_FUNCTION(bht)
{
#if defined(DEBUG)
	php_error_docref(NULL TSRMLS_CC, E_NOTICE, "BHT module shutdown function called!");
#endif

#ifndef ZTS
	// 仅在非线程化PHP中显式调用全局变量析构函数
	php_bht_globals_dtor(&bht_globals TSRMLS_CC);
#endif
	return SUCCESS;
}
/* }}} */

/*  PHP_RINIT_FUNCTION */
/* {{{ */
PHP_RINIT_FUNCTION(bht)
{
#if defined(DEBUG)
	php_error_docref(NULL TSRMLS_CC, E_NOTICE, "BHT module request init function called!");
#endif
	return SUCCESS;
}
/* }}} */

/*  PHP_RSHUTDOWN_FUNCTION */
/* {{{ */
PHP_RSHUTDOWN_FUNCTION(bht)
{
#if defined(DEBUG)
	php_error_docref(NULL TSRMLS_CC, E_NOTICE, "BHT module request shutdown function called!");
#endif
	return SUCCESS;
}
/* }}} */

/*  PHP_MINFO_FUNCTION */
/* {{{ */
PHP_MINFO_FUNCTION(bht)
{
#if defined(DEBUG)
	php_error_docref(NULL TSRMLS_CC, E_NOTICE, "BHT module info function called!");
#endif

	char buf[32];

	php_info_print_table_start();
	php_info_print_table_header(2, "BHT support", "enabled");
	snprintf(buf, sizeof(buf), "%ld", BHT_G(num_persistent));
	php_info_print_table_row(2, "Active Persistent Links", buf);
	snprintf(buf, sizeof(buf), "%ld", BHT_G(num_ondemand));
	php_info_print_table_row(2, "Active On-demand Links", buf);
	php_info_print_table_end();

	/* Remove comments if you have entries in php.ini
	DISPLAY_INI_ENTRIES();
	*/
}
/* }}} */

/**
 * 建立BHT连接。本函数开启的BHT连接为短连接，当前PHP请求处理完毕后即断开。
 * <code>
 * 		list(string $err, resource $handle) = bht_open(string $domain[, string $host[, integer $port]]);
 * </code>
 * @param $domain 要访问的域字符串
 * @param $host [可选]BHT接口服务主机地址字符串
 * @param $port [可选]BHT接口服务端口
 * @retval 操作成功时$err为false，否则为错误信息字符串；操作成功时$handle为BHT连接句柄
 * @see popen, close
 * */
/* {{{ */
PHP_FUNCTION(bht_open)
{
	char *domain;
	int domain_len;
	char *host = DEFAULT_BHT_HOST;
	int host_len = sizeof(DEFAULT_BHT_HOST) - 1;
	long port = DEFAULT_BHT_PORT;

	// 解析函数参数
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s|sl",
				&domain, &domain_len, &host, &host_len, &port) == FAILURE) {
		// 参数解析错误，类型或个数不对，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "invalid paramter format!");

		RETURN_NULL();
	}

	if(!return_value_used) {
		// 调用本函数时未使用返回值，不用费劲进行实际操作，提示后返回null
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "function called without processing return value, no actual work will be done!");

		RETURN_NULL();
	}

	// 准备返回列表值
	array_init(return_value);

	try {
		// 用 auto_ptr 管理 BHT::Client 实例，以便后续操作抛出异常时不再关心资源释放问题
		auto_ptr<Client> cli(new Client(string(domain, domain_len), string(host, host_len),	port));

		// 打开对 BHT 服务的连接(可能抛出 runtime_error 异常)
		cli->open();

		// 操作完成，解除 auto_ptr 对 BHT::Client 实例的管理，以便该实例注册为 PHP 资源
		Client *p = cli.release();

		// 注册 BHT::Client 实例为短连接资源
		zval *res;
		MAKE_STD_ZVAL(res);
		ZEND_REGISTER_RESOURCE(res, p, le_bht_resource);

		// 构造成功返回值 list(false, handle)
		add_next_index_bool(return_value, 0);	// err 为 false
		add_next_index_zval(return_value, res);	// handle 为连接句柄

		// 增加短连接实例计数
		++BHT_G(num_ondemand);
	} catch(const runtime_error &e) {
		// 构造失败返回值 list(errmsg, null)
		add_next_index_string(return_value, (char*)(e.what()), 1/*dup*/);	// err 为错误信息字符串
		add_next_index_null(return_value);							// handle 为 null
	} catch(...) {
		// 未知类型异常
		add_next_index_string(return_value,
				"unknown exception caught", 1/*dup*/);	// err 为错误信息字符串
		add_next_index_null(return_value);				// handle 为 null
	}
}
/* }}} */

/**
 * 建立BHT连接。本函数开启的BHT连接为长连接，当前PHP进程结束时才断开。使用相同的$domain、$host和$port
 * 参数调用popen时获得的总是同一个BHT连接句柄。
 * <code>
 * 		list(string $err, resource $handle) = bht_popen(string $domain[, string $host[, integer $port]]);
 * </code>
 * @param $domain 要访问的域字符串
 * @param $host [可选]BHT接口服务主机地址字符串
 * @param $port [可选]BHT接口服务端口
 * @retval 操作成功时$err为false，否则为错误信息字符串；操作成功时$handle为BHT连接句柄
 * @see open, close
 * */
/* {{{ */
PHP_FUNCTION(bht_popen)
{
	char *domain;
	int domain_len;
	char *host = DEFAULT_BHT_HOST;
	int host_len = sizeof(DEFAULT_BHT_HOST) - 1;
	long port = DEFAULT_BHT_PORT;

	// 解析函数参数
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s|sl",
				&domain, &domain_len, &host, &host_len, &port) == FAILURE) {
		// 参数解析错误，类型或个数不对，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid paramter format!");
		RETURN_NULL();
	}

	if(!return_value_used) {
		// 调用本函数时未使用返回值，不用费劲进行实际操作，提示后返回null
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "function called without processing return value, no actual work will be done!");
		RETURN_NULL();
	}

	// 准备返回列表值
	array_init(return_value);

	list_entry *existing_client;
	char *hash_key;
	int hash_key_len;

	// 构造对应长连接资源的散列键
	hash_key_len = spprintf(&hash_key, 0, "bht_client:%s:%s:%ld", domain, host, port);

	if(zend_hash_find(&EG(persistent_list), hash_key, hash_key_len + 1, (void**)(&existing_client)) == SUCCESS) {
		// 找到了散列键对应的已有长连接资源
#if defined(DEBUG)
		php_printf("found existing connection: hash_key=%s\n", hash_key);
#endif

		// 将其注册为 PHP 资源
		zval *res;
		MAKE_STD_ZVAL(res);
		ZEND_REGISTER_RESOURCE(res, existing_client->ptr, le_bht_resource_persistent);

		// 构造成功返回值 list(false, handle)
		add_next_index_bool(return_value, 0);	// err 为 false
		add_next_index_zval(return_value, res);	// handle 为长连接句柄

		// 释放散列键占用资源并返回
		efree(hash_key);
		return;
	}

	// 未找到散列键对应的长连接资源，创建一个新的连接
	try {
		// 用 auto_ptr 管理 BHT::Client 实例，以便后续操作抛出异常时不再关心资源释放问题
		auto_ptr<Client> cli(new Client(string(domain, domain_len), string(host, host_len),	port));

		// 打开对 BHT 服务的连接(可能抛出 runtime_error 异常)
		cli->open();

		// 操作完成，解除 auto_ptr 对 BHT::Client 实例的管理，以便该实例注册为 PHP 资源
		Client *p = cli.release();

		// 注册 BHT::Client 实例为长连接资源
		zval *res;
		MAKE_STD_ZVAL(res);
		ZEND_REGISTER_RESOURCE(res, p, le_bht_resource_persistent);

		// 构造成功返回值 list(false, handle)
		add_next_index_bool(return_value, 0);	// err 为 false
		add_next_index_zval(return_value, res);	// handle 为连接句柄

		list_entry le;
		le.type = le_bht_resource_persistent;
		le.ptr = p;

		// 使用之前创建的散列键将新建 BHT::Client 实例加入持久资源列表以备后续重复使用
		zend_hash_update(&EG(persistent_list), hash_key, hash_key_len + 1, (void*)(&le), sizeof(list_entry), NULL);

		// 增加长连接实例计数
		++BHT_G(num_persistent);
	} catch(const runtime_error &e) {
		// 构造失败返回值 list(errmsg, null)
		add_next_index_string(return_value, (char*)(e.what()), 1/*dup*/);	// err 为错误信息字符串
		add_next_index_null(return_value);							// handle 为 null
	} catch(...) {
		// 未知类型异常
		add_next_index_string(return_value,
				"unknown exception caught", 1/*dup*/);	// err 为错误信息字符串
		add_next_index_null(return_value);				// handle 为 null
	}

	// 释放散列键占用资源
	efree(hash_key);
}
/* }}} */

/**
 * 显式断开BHT连接。
 * <code>
 * 		string $err = bht_close(resource $handle);
 * </code>
 * @param $handle BHT连接句柄
 * @retval 操作成功时$err为false，否则为错误信息字符串。
 * @see open, popen
 * */
/* {{{ */
PHP_FUNCTION(bht_close)
{
	zval *rsrc;

	// 解析函数参数
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "r", &rsrc) == FAILURE) {
		// 参数解析错误，类型或个数不对，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_WARNING, "invalid parameter format!");

		RETURN_NULL();
	}

	try {
		Client *p;
		int res_type;

		// 将资源标识符转换为对应的资源，这里要同时识别短连接句柄和长连接句柄
		p = (Client*)zend_fetch_resource(&rsrc TSRMLS_CC, -1, PHP_BHT_RESOURCE_NAME, &res_type, 2
				, le_bht_resource, le_bht_resource_persistent);
		if(!p) {
			// 未找到有效的连接句柄，提示错误信息并返回
			php_error_docref(NULL TSRMLS_CC, E_WARNING, "invalid resource handler!");
			RETURN_STRING("invalid resource handler", 1/*dup*/);
		}

		// 强制删除句柄对应的连接资源
		zend_hash_index_del(&EG(regular_list), Z_RESVAL_P(rsrc));

		// 针对长连接还要额外删除长连接池中的对应项
		if(res_type == le_bht_resource_persistent) {
			char *hash_key;
			int hash_key_len;

			// 构造对应长连接资源的散列键
			hash_key_len = spprintf(&hash_key, 0, "bht_client:%s:%s:%ld"
					, (p->domain()).c_str()
					, (p->host()).c_str()
					, (long)(p->port()));

			zend_hash_del(&EG(persistent_list), hash_key, hash_key_len + 1);

			// 释放散列键占用的资源
			efree(hash_key);
		}

		RETURN_FALSE;
	} catch(const runtime_error &e) {
		RETURN_STRING((char*)(e.what()), 1/*dup*/);
	} catch(...) {
		// 未知类型异常
		RETURN_STRING("unknown exception caught", 1/*dup*/);
	}
}
/* }}} */

/**
 * 获得指定键对应的值。
 * <code>
 * 		list(string $err, string $val) = bht_get(resource $handle, string $pkey[, string $skey]);
 * </code>
 * @param $handle BHT连接句柄
 * @param $pkey 待获取的主键字符串
 * @param $skey [可选]待获取的子键字符串。默认为空字符串。
 * @retval 操作成功时$err为false，否则为错误信息字符串；操作成功时，若给定键存在则$val为对应的值字符串，
 * 否则$val为null。
 * @see mget
 * */
/* {{{ */
PHP_FUNCTION(bht_get)
{
	zval *rsrc = NULL;
	char *pkey = NULL;
	int pkey_len = 0;
	char *skey = NULL;
	int skey_len = 0;

	// 解析函数参数
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs|s",
				&rsrc, &pkey, &pkey_len, &skey, &skey_len) == FAILURE) {
		// 参数解析错误，类型或个数不对，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid paramter format!");
		RETURN_NULL();
	}

	if(!return_value_used) {
		// 调用本函数时未使用返回值，不用费劲进行实际操作，提示后返回null
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "function called without processing return value, no actual work will be done!");
		RETURN_NULL();
	}

	// 根据 PHP 资源标识符获取 BHT::Client 实例指针，若资源标识符无效则会直接返回null，可以保证取得的实例指针非空
	Client *cli;
	ZEND_FETCH_RESOURCE2(cli, Client*, &rsrc, -1, PHP_BHT_RESOURCE_NAME, le_bht_resource, le_bht_resource_persistent);

	// 准备返回列表值
	array_init(return_value);

	try {
		
		// 构造 BHT::Client 所需的键结构
		Client::key_type k = Client::make_key(string(pkey, pkey_len), string(skey, skey_len));
		Client::val_type v;

		if(cli->get(k, &v)) {
			// 给定键存在，返回值 list(false, value)
			const string &str = Client::get_value(v);
			add_next_index_bool(return_value, 0);
			add_next_index_stringl(return_value, (char*)(str.data()), str.size(), 1/*dup*/);
		} else {
			// 给定键不存在，返回值 list(false, null)
			add_next_index_bool(return_value, 0);
			add_next_index_null(return_value);
		}

	} catch(const runtime_error &e) {
		// 操作失败，返回值 list(errmsg, null)
		add_next_index_string(return_value, (char*)(e.what()), 1/*dup*/);	// err 为错误信息字符串
		add_next_index_null(return_value);							// val 为 null
	} catch(...) {
		// 未知类型异常
		add_next_index_string(return_value,
				"unknown exception caught", 1/*dup*/);	// err 为错误信息字符串
		add_next_index_null(return_value);				// val 为 null
	}
}
/* }}} */

/**
 * 设置指定键对应的值。
 * <code>
 * 		string $err = bht_set(resource $handle, string $pkey, string $val);
 * 		string $err = bht_set(resource $handle, string $pkey, string $skey, string $val);
 * </code>
 * @param $handle BHT连接句柄
 * @param $pkey 待设置的主键字符串
 * @param $skey [可选]待获取的子键字符串。默认为空字符串。
 * @param $val 待设置的值字符串
 * @retval 操作成功时$err为false，否则为错误信息字符串。
 * @see mset
 * */
/* {{{ */
PHP_FUNCTION(bht_set)
{
	zval *rsrc = NULL;
	char *pkey = NULL;
	int pkey_len = 0;
	char *skey = NULL;
	int skey_len = 0;
	char *val = NULL;
	int val_len = 0;

	// 解析函数参数，先尝试 4 参数形式，失败时再尝试 3 参数形式
	if(ZEND_NUM_ARGS() == 4) {
		if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rsss",
					&rsrc, &pkey, &pkey_len, &skey, &skey_len, &val, &val_len) == FAILURE) {
			// 参数解析错误，类型或个数不对，提示错误信息并返回null
			php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid paramter format!");
			RETURN_NULL();
		}
	} else {
		if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rss",
					&rsrc, &pkey, &pkey_len, &val, &val_len) == FAILURE) {
			// 参数解析错误，类型或个数不对，提示错误信息并返回null
			php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid paramter format!");
			RETURN_NULL();
		}
	}

	// 根据 PHP 资源标识符获取 BHT::Client 实例指针，若资源标识符无效则会直接返回null，可以保证取得的实例指针非空
	Client *cli;
	ZEND_FETCH_RESOURCE2(cli, Client*, &rsrc, -1, PHP_BHT_RESOURCE_NAME, le_bht_resource, le_bht_resource_persistent);

	try {
		// 构造 BHT::Client 所需格式的键值结构
		Client::key_type k = Client::make_key(string(pkey, pkey_len), string(skey, skey_len));
		Client::val_type v = Client::make_val(string(val, val_len));

		cli->set(k, v);

		// 操作成功，返回 false
		RETURN_FALSE;
	} catch(const runtime_error &e) {
		// 操作失败，返回 errmsg
		RETURN_STRING((char*)(e.what()), 1/*dup*/);
	} catch(...) {
		// 未知类型异常
		RETURN_STRING("unknown exception caught", 1/*dup*/);
	}
}
/* }}} */

/**
 * 删除指定键。
 * <code>
 * 		string $err = bht_del(resource $handle, string $pkey[, string $skey]);
 * </code>
 * @param $handle BHT连接句柄
 * @param $pkey 待删除的主键字符串
 * @param $skey [可选]待删除的子键字符串。默认为空字符串。
 * @retval 操作成功时$err为false，否则为错误信息字符串。
 * @see mdel
 * */
/* {{{ */
PHP_FUNCTION(bht_del)
{
	zval *rsrc;
	char *pkey;
	int pkey_len;
	char *skey = NULL;
	int skey_len = 0;

	// 解析函数参数
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "rs|s",
				&rsrc, &pkey, &pkey_len, &skey, &skey_len) == FAILURE) {
		// 参数解析错误，类型或个数不对，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid paramter format!");
		RETURN_NULL();
	}

	// 根据 PHP 资源标识符获取 BHT::Client 实例指针，若资源标识符无效则会直接返回null，可以保证取得的实例指针非空
	Client *cli;
	ZEND_FETCH_RESOURCE2(cli, Client*, &rsrc, -1, PHP_BHT_RESOURCE_NAME, le_bht_resource, le_bht_resource_persistent);

	try {
		// 构造 BHT::Client 所需格式的键结构
		Client::key_type k = Client::make_key(string(pkey, pkey_len), string(skey, skey_len));

		cli->del(k);

		// 操作成功，返回 false
		RETURN_FALSE;
	} catch(const runtime_error &e) {
		// 操作失败，返回 errmsg
		RETURN_STRING((char*)(e.what()), 1/*dup*/);
	} catch(...) {
		// 未知类型异常
		RETURN_STRING("unknown exception caught", 1/*dup*/);
	}
}
/* }}} */

/**
 * 将 PHP 数组形式的键列表转换为 STL vector
 * @param ks 含有PHP数组形式键列表的zval指针
 * @param cks 保存转换结果的目标vector
 * @retval 转换成功时返回true，否则返回false
 * */
/* {{{ */
static bool convert_key_hashtable_to_vector(zval *ks, vector<Client::key_type> *cks)
{
	HashPosition pos;

	// 遍历给定的键数组
	for(zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(ks), &pos);
			zend_hash_has_more_elements_ex(Z_ARRVAL_P(ks), &pos) == SUCCESS;
			zend_hash_move_forward_ex(Z_ARRVAL_P(ks), &pos)) {

		zval **ppzval;
		
		if(zend_hash_get_current_data_ex(Z_ARRVAL_P(ks), (void**)&ppzval, &pos) == FAILURE) {
			// 当前迭代器对应的元素不存在？一般不应该出现该错误。跳过当前元素以绕过该问题。
			continue;
		}

		// 根据当前数组元素的类型进行对应形式的键格式转换
		switch(Z_TYPE_PP(ppzval)) {
			case IS_STRING:	// string pkey
				{
					Client::key_type ck = Client::make_key(string(Z_STRVAL_PP(ppzval), Z_STRLEN_PP(ppzval)));
#if defined(DEBUG)
					php_printf("key: %s:%s\n", Client::get_primary_key(ck).c_str(), Client::get_secondary_key(ck).c_str());
#endif
					cks->push_back(ck);
				}
				break;
			case IS_ARRAY:	// array(string pkey, string skey)
				{
					zval **pp_pkey;
					zval **pp_skey;

					if(zend_hash_index_find(Z_ARRVAL_PP(ppzval), 0, (void**)&pp_pkey) == FAILURE
							|| Z_TYPE_PP(pp_pkey) != IS_STRING) {
						return false;
					}
					if(zend_hash_index_find(Z_ARRVAL_PP(ppzval), 1, (void**)&pp_skey) == FAILURE
							|| Z_TYPE_PP(pp_skey) != IS_STRING) {
						return false;
					}

					Client::key_type ck = Client::make_key(
								string(Z_STRVAL_PP(pp_pkey), Z_STRLEN_PP(pp_pkey)),
								string(Z_STRVAL_PP(pp_skey), Z_STRLEN_PP(pp_skey))
							);
#if defined(DEBUG)
					php_printf("key: %s:%s\n", Client::get_primary_key(ck).c_str(), Client::get_secondary_key(ck).c_str());
#endif
					cks->push_back(ck);
				}
				break;
			default:
				return false;
		}
	}

	return true;
}
/* }}} */

/**
 * 将 PHP 数组形式的值列表转换为 STL vector
 * @param ks 含有PHP数组形式值列表的zval指针
 * @param cks 保存转换结果的目标vector
 * @retval 转换成功时返回true，否则返回false
 * */
/* {{{ */
static bool convert_val_hashtable_to_vector(zval *vs, vector<Client::val_type> *cvs)
{
	HashPosition pos;

	// 遍历给定的键数组
	for(zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(vs), &pos);
			zend_hash_has_more_elements_ex(Z_ARRVAL_P(vs), &pos) == SUCCESS;
			zend_hash_move_forward_ex(Z_ARRVAL_P(vs), &pos)) {

		zval **ppzval;
		
		if(zend_hash_get_current_data_ex(Z_ARRVAL_P(vs), (void**)&ppzval, &pos) == FAILURE) {
			// 当前迭代器对应的元素不存在？一般不应该出现该错误。跳过当前元素以绕过该问题。
			continue;
		}

		// 根据当前数组元素的类型进行对应形式的键格式转换
		switch(Z_TYPE_PP(ppzval)) {
			case IS_STRING:	// string value
				{
					Client::val_type cv = Client::make_val(string(Z_STRVAL_PP(ppzval), Z_STRLEN_PP(ppzval)));
#if defined(DEBUG)
					php_printf("val: %s\n", Client::get_value(cv).c_str());
#endif
					cvs->push_back(cv);
				}
				break;
			default:
				return false;
		}
	}

	return true;
}
/* }}} */

/**
 * 获取指定的多个键对应的值。
 * <code>
 * 		list(string $err, array $vs) = bht_mget(resource $handle, array $ks);
 * </code>
 * @param $handle BHT连接句柄
 * @param $ks 待获取的键列表。其中每个数组元素可以是单个字符串或含有2个字符串的数组；对于前者，
 * 该字符串就是主键；对于后者，第1个字符串为主键，第2个字符串为子键。例如：
 * <code>
 * 		array("pkey1","pkey2",array("pkey3","skey1"),array("pkey1","skey1"));
 * </code>
 * @retval 所有键操作均成功时$err为false，否则为首个出错操作的错误信息字符串；操作成功时$vs为值字符串列表，
 * 顺序对应每个给定的键，当某键不存在时对应值为null。
 * @see get
 * */
/* {{{ */
PHP_FUNCTION(bht_mget)
{
	zval *rsrc;
	zval *ks;

	// 解析函数参数
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ra", &rsrc, &ks) == FAILURE) {
		// 参数解析错误，类型或个数不对，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid paramter format!");
		RETURN_NULL();
	}

	if(!return_value_used) {
		// 调用本函数时未使用返回值，不用费劲进行实际操作，提示后返回null
		php_error_docref(NULL TSRMLS_CC, E_NOTICE, "function called without processing return value, no actual work will be done!");
		RETURN_NULL();
	}

	// 将给定的多个键转换为 BHT::Client 要求的键结构
	vector<Client::key_type> cks;
	if(!convert_key_hashtable_to_vector(ks, &cks)) {
		// 数组元素不存在或类型有误，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid key format, should be either a string or an array with 2 strings!");
		RETURN_NULL();
	}

	// 根据 PHP 资源标识符获取 BHT::Client 实例指针，若资源标识符无效则会直接返回null，可以保证取得的实例指针非空
	Client *cli;
	ZEND_FETCH_RESOURCE2(cli, Client*, &rsrc, -1, PHP_BHT_RESOURCE_NAME, le_bht_resource, le_bht_resource_persistent);

	// 准备返回列表值
	array_init(return_value);

	try {
		map<Client::key_type, Client::val_type> cvs;

		cli->mget(cks, &cvs);

		// 将 BHT::Client 返回的键值结构转换为 PHP 数组
		zval *vs;
		MAKE_STD_ZVAL(vs);
		array_init(vs);
		for(size_t i = 0; i < cks.size(); ++i) {
			// 寻找当前键对应的值
			map<Client::key_type, Client::val_type>::const_iterator cit = cvs.find(cks[i]);
			if(cit != cvs.end()) {
				// 找到了当前键对应的值，转换成字符串插入 PHP 数组
				const string &str = Client::get_value(cit->second);
				add_next_index_stringl(vs, (char*)(str.data()), str.size(), 1/*dup*/);
			} else {
				// 未找到当前键对应的值，将 null 插入 PHP 数组
				add_next_index_null(vs);
			}
		}

		add_next_index_bool(return_value, 0);	// err 为 false
		add_next_index_zval(return_value, vs);	// vs 为构造出的 PHP 数组
	} catch(const runtime_error &e) {
		// 操作失败，返回值 list(errmsg, null)
		add_next_index_string(return_value, (char*)(e.what()), 1/*dup*/);	// err 为错误信息字符串
		add_next_index_null(return_value);							// vs 为 null
	} catch(...) {
		// 未知类型异常
		add_next_index_string(return_value,
				"unknown exception caught", 1/*dup*/);	// err 为错误信息字符串
		add_next_index_null(return_value);				// vs 为 null
	}
}
/* }}} */

/**
 * 设置指定的多个键值对。
 * <code>
 * 		string $err = bht_mset(resource $handle, array $ks, array $vs);
 * </code>
 * @param $handle BHT连接句柄
 * @param $ks 待设置的键列表。其中每个数组元素可以是单个字符串或含有2个字符串的数组；对于前者，
 * 该字符串就是主键；对于后者，第1个字符串为主键，第2个字符串为子键。例如：
 * <code>
 * 		array("pkey1","pkey2",array("pkey3","skey1"),array("pkey1","skey1"));
 * </code>
 * @param $vs 待设置的值列表。其中的元素对应每个键的值字符串。
 * @retval 所有键操作均成功时$err为false，否则为首个出错操作的错误信息字符串。
 * @see set
 * */
/* {{{ */
PHP_FUNCTION(bht_mset)
{
	zval *rsrc;
	zval *ks;
	zval *vs;

	// 解析函数参数
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "raa", &rsrc, &ks, &vs) == FAILURE) {
		// 参数解析错误，类型或个数不对，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid paramter format!");
		RETURN_NULL();
	}

	// 将给定的多个键值对转换为 BHT::Client 要求的键值结构
	vector<Client::key_type> cks;
	vector<Client::val_type> cvs;
	if(!convert_key_hashtable_to_vector(ks, &cks)) {
		// 数组元素不存在或类型有误，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid key format, should be either a string or an array with 2 strings!");
		RETURN_NULL();
	}
	if(!convert_val_hashtable_to_vector(vs, &cvs)) {
		// 数组元素不存在或类型有误，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid val format, should be a string!");
		RETURN_NULL();
	}
	if(cks.size() != cvs.size()) {
		// 键值数量不匹配，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "key/val number not equal! key number: %d, val number: %d\n", (int)cks.size(), (int)cvs.size());
		RETURN_NULL();
	}

	// 根据 PHP 资源标识符获取 BHT::Client 实例指针，若资源标识符无效则会直接返回null，可以保证取得的实例指针非空
	Client *cli;
	ZEND_FETCH_RESOURCE2(cli, Client*, &rsrc, -1, PHP_BHT_RESOURCE_NAME, le_bht_resource, le_bht_resource_persistent);

	try {
		map<Client::key_type, Client::val_type> ckvs;
		for(size_t i = 0; i < cks.size(); ++i) {
			ckvs.insert(pair<Client::key_type, Client::val_type>(cks[i], cvs[i]));
		}

		cli->mset(ckvs);

		RETURN_FALSE;	// err 为 false
	} catch(const runtime_error &e) {
		// 操作失败，返回值 err
		RETURN_STRING((char*)(e.what()), 1/*dup*/);	// err 为错误信息字符串
	} catch(...) {
		// 未知类型异常
		RETURN_STRING("unknown exception caught", 1/*dup*/);	// err 为错误信息字符串
	}
}
/* }}} */

/**
 * 删除指定的多个键。
 * <code>
 * 		string $err = bht_mdel(resource $handle, array $ks)
 * </code>
 * @param $handle BHT连接句柄
 * @param $ks 待删除的键列表。其中每个数组元素可以是单个字符串或含有2个字符串的数组；对于前者，
 * 该字符串就是主键；对于后者，第1个字符串为主键，第2个字符串为子键。例如：
 * <code>
 * 		array("pkey1","pkey2",array("pkey3","skey1"),array("pkey1","skey1"));
 * </code>
 * @retval 所有键操作均成功时$err为false，否则为首个出错操作的错误信息字符串。
 * @see del
 * */
/* {{{ */
PHP_FUNCTION(bht_mdel)
{
	zval *rsrc;
	zval *ks;

	// 解析函数参数
	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ra", &rsrc, &ks) == FAILURE) {
		// 参数解析错误，类型或个数不对，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid paramter format!");
		RETURN_NULL();
	}

	// 将给定的多个键转换为 BHT::Client 要求的键结构
	vector<Client::key_type> cks;
	if(!convert_key_hashtable_to_vector(ks, &cks)) {
		// 数组元素不存在或类型有误，提示错误信息并返回null
		php_error_docref(NULL TSRMLS_CC, E_ERROR, "invalid key format, should be either a string or an array with 2 strings!");
		RETURN_NULL();
	}

	// 根据 PHP 资源标识符获取 BHT::Client 实例指针，若资源标识符无效则会直接返回null，可以保证取得的实例指针非空
	Client *cli;
	ZEND_FETCH_RESOURCE2(cli, Client*, &rsrc, -1, PHP_BHT_RESOURCE_NAME, le_bht_resource, le_bht_resource_persistent);

	try {
		cli->mdel(cks);

		RETURN_FALSE;	// err 为 false
	} catch(const runtime_error &e) {
		// 操作失败，返回值 err
		RETURN_STRING((char*)(e.what()), 1/*dup*/);	// err 为错误信息字符串
	} catch(...) {
		// 未知类型异常
		RETURN_STRING("unknown exception caught", 1/*dup*/);	// err 为错误信息字符串
	}
}
/* }}} */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */

