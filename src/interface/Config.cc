#include <iostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Config.h"
#include "Logging.h"
#include "CusHandler.h"
#include "SeqHandler.h"
#include "TBBHandler.h"
#include "NulHandler.h"
#include "MemoryBackend.h"
#include "MemcachedBackend.h"
#include "TokyoTyrantBackend.h"
#include "RelayBackend.h"
#include "RelayNode.h"
#include "Validator.h"

#define DEFAULT_CONF_PATH "/home/z/conf/bht/bht.conf"
#define TEST_CONF_PATH "./testconf.conf"

#define BHT_LUA_HANDLER_NAME "bht_handler"
#define BHT_LUA_CRING_NAME "bht_cache_ring"
#define BHT_LUA_LRING_NAME "bht_lookup_ring"
#define BHT_LUA_SRING_NAME "bht_storage_ring"
#define BHT_LUA_RRING_NAME "bht_relay_ring"
#define BHT_LUA_RDOMAIN_NAME "bht_relay_domain"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.Config")

Config* Config::_instance = NULL;

Config& Config::getInstance(const string &conf)
{
	ScopedLoggingCtx logging_ctx("<Config::getInstance>");
	if(!_instance) {
		_instance = new Config(conf);
	}
	return *_instance;
}

void Config::destroyInstance()
{
	ScopedLoggingCtx logging_ctx("<Config::destroyInstance>");
	if(_instance) {
		delete _instance;
		_instance = NULL;
	}
}

Config::Config(const string &conf)
	:_conf_path(conf),
	_storage_ring_ver(0),
	_lookup_ring_ver(0),
	_cache_ring_ver(0),
	_relay_ring_ver(0),
	_relay_domain()
{
	ScopedLoggingCtx logging_ctx("<Config::Config>");
	if(_conf_path.size() == 0) {
		_conf_path = DEFAULT_CONF_PATH;
	}

	struct stat st;
	// 若指定的配置文件不存在，或者存在但不是普通文件或符号链接，则抛出异常终止程序
	if(stat(_conf_path.c_str(), &st) || !(S_ISREG(st.st_mode) || S_ISLNK(st.st_mode))) {
		_conf_path = TEST_CONF_PATH;

		if(stat(_conf_path.c_str(), &st) || !(S_ISREG(st.st_mode) || S_ISLNK(st.st_mode))) {
			cerr << "Invalid configuration file: "<<_conf_path<<endl;
			throw "Invalid configuration file";
		}
	}

	_storage_ring = shared_ptr<Ring>((Ring*)NULL);
	_storage_pool = shared_ptr<BackendPool>((BackendPool*)NULL);
	_lookup_ring = shared_ptr<Ring>((Ring*)NULL);
	_lookup_pool = shared_ptr<BackendPool>((BackendPool*)NULL);
	_cache_ring = shared_ptr<Ring>((Ring*)NULL);
	_cache_pool = shared_ptr<BackendPool>((BackendPool*)NULL);
	_relay_ring = shared_ptr<Ring>((Ring*)NULL);
	_relay_pool = shared_ptr<BackendPool>((BackendPool*)NULL);

	initConfig();
}

Config::~Config()
{
	ScopedLoggingCtx ctx("<Config::~Config>");
}

void Config::initConfig()
{
	ScopedLoggingCtx ctx("<Config::initConfig>");
	Lua l;
	int ec;

	// 载入 Lua 格式配置文件并运行
	ec = luaL_loadfile(l, _conf_path.c_str()) || lua_pcall(l, 0, 0, 0);
	if(ec) {
		FATAL("Invalid config file: " + string(lua_tostring(l, -1)));
		lua_pop(l, 1);
		exit(-1);
	}

	setHandlerFromLua(l);
	setCacheRingFromLua(l);
	setLookupRingFromLua(l);
	setStorageRingFromLua(l);
	setRelayRingFromLua(l);
	setRelayDomainFromLua(l);
}

shared_ptr<BackendFactory> Config::getBackendFactory(const string &backend_id)
{
	ScopedLoggingCtx logging_ctx("<Config::getBackendFactory>");
	if(backend_id == "memory") {
		return shared_ptr<BackendFactory>(new MemoryBackendFactory());
	} else if(backend_id == "memcached") {
		return shared_ptr<BackendFactory>(new MemcachedBackendFactory());
	} else if(backend_id == "tokyotyrant") {
		return shared_ptr<BackendFactory>(new TokyoTyrantBackendFactory());
	}

	return shared_ptr<BackendFactory>((BackendFactory*)NULL);
}

/**
 * handler type 在配置文件中的格式：
 * <code>
 * 	bht_handler = {
 * 		type = <"seq" | "tbb" | "cus" | "nul">,
 * 		[worker_num = <n>,]
 * 	}
 * </code>
 * 其中 "seq" 代表多键操作用简单的顺序循环实现的，"tbb" 代表多键操作用 Intel TBB 库提供的
 * 线程化并行框架实现，"cus" 代表多键操作用自定制的并行方案实现，"nul" 代表完全空操作。
 * 当 handler type 取为 "tbb" 时，还可以用 worker_num 参数指定创建并行运算线程的数量，若
 * 不指定则默认为自动根据当前可用 CPU 核数创建。
 * */
void Config::setHandlerFromLua(Lua &l)
{
	ScopedLoggingCtx logging_ctx("<Config::setHandlerFromLua>");
	lua_getglobal(l, BHT_LUA_HANDLER_NAME);	// 获取 handler table 并压入栈中
	if(!lua_istable(l, -1)) {
		// 栈顶的 handler 类型有误
		ERROR("Invalid handler configuration. Please check config file...");
		lua_pop(l, 1);	// 弹出 handler 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		exit(-1);
	}

	// 读取 handler type 字符串
	lua_pushstring(l, "type");	// 压入 "type" 字符串
	lua_gettable(l, -2);	// 弹出 "type" 字符串，获得 handler table 中的 type 属性值压入栈中
	if(!lua_isstring(l, -1)) {
		ERROR("Invalid handler type, must be one of 'seq', 'tbb', 'cus' or 'nul'. Please check config file...");
		lua_pop(l, 2);	// 弹出 handler 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		exit(-1);
	}

	_handler_type = lua_tostring(l, -1);
	lua_pop(l, 1);	// 弹出 type 属性值

	if(_handler_type != "seq"
			&& _handler_type != "tbb"
			&& _handler_type != "cus"
			&& _handler_type != "nul") {
		// 无效的 handler 类型
		ERROR("Invalid handler type '" + _handler_type + "', must be one of 'seq', 'tbb', 'cus' or 'nul'. Please check config file...");
		lua_pop(l, 1);	// 弹出 handler table
		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		exit(-1);
	}

	if(_handler_type == "tbb" || _handler_type == "cus") {
		// 当前指定的 handler type 为 TBB Handler 或 Custom，尝试获取 worker_num 属性值
		_handler_worker_num = 0;	// 默认 worker_num 为运行时自动确定

		lua_pushstring(l, "worker_num");
		lua_gettable(l, -2);
		if(!lua_isnil(l, -1)) {
			if(!lua_isnumber(l, -1)) {
				ERROR("Invalid worker number '" + string(lua_tostring(l, -1)) + "', must be a non-negative integer. Please check config file...");
				lua_pop(l, 2);
				assert(lua_gettop(l) == 0);
				exit(-1);
			}

			_handler_worker_num = (int32_t)lua_tonumber(l, -1);
		}
		lua_pop(l, 1);

		if(_handler_worker_num < 0) {
			ERROR("Invalid worker number '" + lexical_cast<string>(_handler_worker_num) + "', must be a non-negative integer. Please check config file...");
			lua_pop(l, 1);
			assert(lua_gettop(l) == 0);
			exit(-1);
		}
	}

	lua_pop(l, 1);	// 弹出 handler table
	assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
}

/**
 * cache ring 在配置文件中的格式：
 * <code>
 * 	bht_cache_ring = {
 * 		ver = <uint32_t>,
 * 		[backend = <"memory" | "memcached" | "tokyotyrant">,]
 *		ring = {
 *			{
 *				id = <uint32_t>,
 *				addr = {
 *					{ host = <str>, port = <int> },
 *					...
 *				}
 *			},
 *			...
 *		}
 *	}
 * </code>
 * */
void Config::setCacheRingFromLua(Lua &l)
{
	ScopedLoggingCtx logging_ctx("<Config::setCacheRingFromLua>");
	lua_getglobal(l, BHT_LUA_CRING_NAME);	// 获取 cache ring table 压入栈中
	if(!lua_istable(l, -1)) {
		// 栈顶的 cache ring table 值类型有误
		ERROR("Invalid cache ring configuration. Please check config file...");
		lua_pop(l, 1);	// 弹出 cache ring table 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		return;
	}

	uint32_t cache_ring_ver = 0;

	// 读取 cache ring 中的 ver 属性
	lua_pushstring(l, "ver");	// 压入 "ver" 字符串
	lua_gettable(l, -2);	// 弹出 "ver" 字符串，获得 cache ring table 的 ver 属性值压入栈中
	if(!lua_isnumber(l, -1)) {
		// ver 属性值类型错误
		ERROR("Incorrect cache ring version, should be a positive integer: " + string(lua_tostring(l, -1)));
		lua_pop(l, 2);	// 弹出 ver 属性值和 cache ring table 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		return;
	}
	cache_ring_ver = (uint32_t)lua_tonumber(l, -1);	// 获取 ver 属性值(栈内容不变)
	lua_pop(l, 1);	// 弹出 ver 属性的值

	// 若配置文件中 cache ring 版本大于当前使用的 cache ring 版本，则需要更新内部数据
	if(cache_ring_ver > _cache_ring_ver) {
		// 读取 cache ring 中的 backend 属性
		const char *backend = "memcached";	// cache ring 默认后端为 memcached
		lua_pushstring(l, "backend");	// 压入 "backend" 字符串
		lua_gettable(l, -2);	// 弹出 "backend" 字符串，获得 cache ring table 的 backend 属性值压入栈中
		if(lua_isstring(l, -1)) {
			backend = lua_tostring(l, -1);
		}

		// 根据 backend 属性获取对应的后端连接工厂对象
		shared_ptr<BackendFactory> factory = getBackendFactory(backend);
		if(!factory) {
			// 无法构造后端连接工厂对象
			ERROR("Incorrect cache ring backend, should be one of memory/memcached/tokyotyrant: " + string(backend));
			lua_pop(l, 2);	// 弹出 backend 属性值和 cache ring table 值

			assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
			return;
		}
		lua_pop(l, 1);	// 弹出 backend 属性的值

		if(!_cache_ring) {
			_cache_ring = shared_ptr<Ring>(new Ring());
		} else {
			_cache_ring->clear();
		}

		_cache_pool = shared_ptr<BackendPool>(new BackendPool(factory));

		// 读取 cache ring 中的 ring 属性
		lua_pushstring(l, "ring");	// 压入 "ring" 字符串
		lua_gettable(l, -2);	// 弹出 "ring" 字符串，获得 cache ring table 的 ring 属性值压入栈中
		if(!lua_istable(l, -1)) {
			// 错误的 ring 属性值类型
			ERROR("Invalid cache ring node settings, should be a Lua table fill with node settings");
			lua_pop(l, 2);	// 弹出 ring 属性值和 cache ring table 值

			assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
			return;
		}

		// 构造 cache ring
		constructRingFromLua(l, _cache_ring, _cache_pool);

		lua_pop(l, 1);	// 弹出 ring 属性值

		_cache_ring_ver = cache_ring_ver;	// 更新内部配置数据版本
	}

	lua_pop(l, 1);	// 弹出 cache ring table 值
	assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
}

/**
 * lookup ring 在配置文件中的格式：
 * <code>
 * 	bht_lookup_ring = {
 * 		ver = <uint32_t>,
 * 		[backend = <"memory" | "memcached" | "tokyotyrant">,]
 *		ring = {
 *			{
 *				id = <uint32_t>,
 *				addr = {
 *					{ host = <str>, port = <int> },
 *					...
 *				}
 *			},
 *			...
 *		}
 *	}
 * </code>
 * */
void Config::setLookupRingFromLua(Lua &l)
{
	ScopedLoggingCtx logging_ctx("<Config::setLookupRingFromLua>");
	lua_getglobal(l, BHT_LUA_LRING_NAME);	// 获取 lookup ring table 压入栈中
	if(!lua_istable(l, -1)) {
		// 栈顶的 lookup ring table 值类型有误
		ERROR("Invalid lookup ring configuration. Please check config file...");
		lua_pop(l, 1);	// 弹出 lookup ring table 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		return;
	}

	uint32_t lookup_ring_ver = 0;

	// 读取 lookup ring 中的 ver 属性
	lua_pushstring(l, "ver");	// 压入 "ver" 字符串
	lua_gettable(l, -2);	// 弹出 "ver" 字符串，获得 lookup ring table 的 ver 属性值压入栈中
	if(!lua_isnumber(l, -1)) {
		// ver 属性值类型错误
		ERROR("Incorrect lookup ring version, should be a positive integer: " + string(lua_tostring(l, -1)));
		lua_pop(l, 2);	// 弹出 ver 属性值和 lookup ring table 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		return;
	}
	lookup_ring_ver = (uint32_t)lua_tonumber(l, -1);	// 获取 ver 属性值(栈内容不变)
	lua_pop(l, 1);	// 弹出 ver 属性的值

	// 若配置文件中 lookup ring 版本大于当前使用的 lookup ring 版本，则需要更新内部数据
	if(lookup_ring_ver > _lookup_ring_ver) {
		// 读取 lookup ring 中的 backend 属性
		const char *backend = "tokyotyrant";	// lookup ring 默认后端为 tokyotyrant
		lua_pushstring(l, "backend");	// 压入 "backend" 字符串
		lua_gettable(l, -2);	// 弹出 "backend" 字符串，获得 lookup ring table 的 backend 属性值压入栈中
		if(lua_isstring(l, -1)) {
			backend = lua_tostring(l, -1);
		}

		// 根据 backend 属性获取对应的后端连接工厂对象
		shared_ptr<BackendFactory> factory = getBackendFactory(backend);
		if(!factory) {
			// 无法构造后端连接工厂对象
			ERROR("Incorrect lookup ring backend, should be one of memory/memcached/tokyotyrant: " + string(backend));
			lua_pop(l, 2);	// 弹出 backend 属性值和 lookup ring table 值

			assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
			return;
		}
		lua_pop(l, 1);	// 弹出 backend 属性的值

		if(!_lookup_ring) {
			_lookup_ring = shared_ptr<Ring>(new Ring());
		} else {
			_lookup_ring->clear();
		}

		_lookup_pool = shared_ptr<BackendPool>(new BackendPool(factory));

		// 读取 lookup ring 中的 ring 属性
		lua_pushstring(l, "ring");	// 压入 "ring" 字符串
		lua_gettable(l, -2);	// 弹出 "ring" 字符串，获得 lookup ring table 的 ring 属性值压入栈中
		if(!lua_istable(l, -1)) {
			// 错误的 ring 属性值类型
			ERROR("Invalid lookup ring node settings, should be a Lua table fill with node settings");
			lua_pop(l, 2);	// 弹出 ring 属性值和 lookup ring table 值

			assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
			return;
		}

		// 构造 lookup ring
		constructRingFromLua(l, _lookup_ring, _lookup_pool);

		lua_pop(l, 1);	// 弹出 ring 属性值

		_lookup_ring_ver = lookup_ring_ver;	// 更新内部配置数据版本
	}

	lua_pop(l, 1);	// 弹出 lookup ring table 值
	assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
}

/**
 * storage ring 在配置文件中的格式：
 * <code>
 * 	bht_storage_ring = {
 * 		ver = <uint32_t>,
 * 		[backend = <"memory" | "memcached" | "tokyotyrant">,]
 *		ring = {
 *			{
 *				id = <uint32_t>,
 *				addr = {
 *					{ host = <str>, port = <int> },
 *					...
 *				}
 *			},
 *			...
 *		}
 *	}
 * </code>
 * */
void Config::setStorageRingFromLua(Lua &l)
{
	ScopedLoggingCtx logging_ctx("<Config::setStorageRingFromLua>");
	lua_getglobal(l, BHT_LUA_SRING_NAME);	// 获取 storage ring table 压入栈中
	if(!lua_istable(l, -1)) {
		// 栈顶的 storage ring table 值类型有误
		ERROR("Invalid storage ring configuration. Please check config file...");
		lua_pop(l, 1);	// 弹出 storage ring table 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		return;
	}

	uint32_t storage_ring_ver = 0;

	// 读取 storage ring 中的 ver 属性
	lua_pushstring(l, "ver");	// 压入 "ver" 字符串
	lua_gettable(l, -2);	// 弹出 "ver" 字符串，获得 storage ring table 的 ver 属性值压入栈中
	if(!lua_isnumber(l, -1)) {
		// ver 属性值类型错误
		ERROR("Incorrect storage ring version, should be a positive integer: " + string(lua_tostring(l, -1)));
		lua_pop(l, 2);	// 弹出 ver 属性值和 storage ring table 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		return;
	}
	storage_ring_ver = (uint32_t)lua_tonumber(l, -1);	// 获取 ver 属性值(栈内容不变)
	lua_pop(l, 1);	// 弹出 ver 属性的值

	// 若配置文件中 storage ring 版本大于当前使用的 storage ring 版本，则需要更新内部数据
	if(storage_ring_ver > _storage_ring_ver) {
		// 读取 storage ring 中的 backend 属性
		const char *backend = "tokyotyrant";	// storage ring 默认后端为 tokyotyrant
		lua_pushstring(l, "backend");	// 压入 "backend" 字符串
		lua_gettable(l, -2);	// 弹出 "backend" 字符串，获得 storage ring table 的 backend 属性值压入栈中
		if(lua_isstring(l, -1)) {
			backend = lua_tostring(l, -1);
		}

		// 根据 backend 属性获取对应的后端连接工厂对象
		shared_ptr<BackendFactory> factory = getBackendFactory(backend);
		if(!factory) {
			// 无法构造后端连接工厂对象
			ERROR("Incorrect storage ring backend, should be one of memory/memcached/tokyotyrant: " + string(backend));
			lua_pop(l, 2);	// 弹出 backend 属性值和 storage ring table 值

			assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
			return;
		}
		lua_pop(l, 1);	// 弹出 backend 属性的值

		if(!_storage_ring) {
			_storage_ring = shared_ptr<Ring>(new Ring());
		} else {
			_storage_ring->clear();
		}

		_storage_pool = shared_ptr<BackendPool>(new BackendPool(factory));

		// 读取 storage ring 中的 ring 属性
		lua_pushstring(l, "ring");	// 压入 "ring" 字符串
		lua_gettable(l, -2);	// 弹出 "ring" 字符串，获得 storage ring table 的 ring 属性值压入栈中
		if(!lua_istable(l, -1)) {
			// 错误的 ring 属性值类型
			ERROR("Invalid storage ring node settings, should be a Lua table fill with node settings");
			lua_pop(l, 2);	// 弹出 ring 属性值和 storage ring table 值

			assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
			return;
		}

		// 构造 storage ring
		constructRingFromLua(l, _storage_ring, _storage_pool);

		lua_pop(l, 1);	// 弹出 ring 属性值

		_storage_ring_ver = storage_ring_ver;	// 更新内部配置数据版本
	}

	lua_pop(l, 1);	// 弹出 storage ring table 值
	assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
}

/**
 * relay ring 在配置文件中的格式：
 * <code>
 * 	bht_relay_ring = {
 * 		ver = <uint32_t>,
 *		ring = {
 *			{
 *				id = <uint32_t>,
 *				addr = {
 *					{ host = <str>, port = <int> },
 *					...
 *				}
 *			},
 *			...
 *		}
 *	}
 * </code>
 * */
void Config::setRelayRingFromLua(Lua &l)
{
	ScopedLoggingCtx logging_ctx("<Config::setRelayRingFromLua>");
	lua_getglobal(l, BHT_LUA_RRING_NAME);	// 获取 relay ring table 压入栈中
	if(!lua_istable(l, -1)) {
		// 栈顶的 relay ring table 值类型有误
		ERROR("Invalid relay ring configuration. Please check config file...");
		lua_pop(l, 1);	// 弹出 relay ring table 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		return;
	}

	uint32_t relay_ring_ver = 0;

	// 读取 relay ring 中的 ver 属性
	lua_pushstring(l, "ver");	// 压入 "ver" 字符串
	lua_gettable(l, -2);	// 弹出 "ver" 字符串，获得 relay ring table 的 ver 属性值压入栈中
	if(!lua_isnumber(l, -1)) {
		// ver 属性值类型错误
		ERROR("Incorrect relay ring version, should be a positive integer: " + string(lua_tostring(l, -1)));
		lua_pop(l, 2);	// 弹出 ver 属性值和 relay ring table 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		return;
	}
	relay_ring_ver = (uint32_t)lua_tonumber(l, -1);	// 获取 ver 属性值(栈内容不变)
	lua_pop(l, 1);	// 弹出 ver 属性的值

	// 若配置文件中 relay ring 版本大于当前使用的 relay ring 版本，则需要更新内部数据
	if(relay_ring_ver > _relay_ring_ver) {
		// 创建新的 relay 后端连接工厂
		shared_ptr<BackendFactory> factory(new RelayBackendFactory());
		if(!factory) {
			// 无法构造后端连接工厂对象
			ERROR("Failed to create relay ring backend factory!");
			lua_pop(l, 1);	// 弹出 relay ring table 值

			assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
			return;
		}

		if(!_relay_ring) {
			_relay_ring = shared_ptr<Ring>(new Ring());
		} else {
			_relay_ring->clear();
		}

		_relay_pool = shared_ptr<BackendPool>(new BackendPool(factory));

		// 读取 relay ring 中的 ring 属性
		lua_pushstring(l, "ring");	// 压入 "ring" 字符串
		lua_gettable(l, -2);	// 弹出 "ring" 字符串，获得 relay ring table 的 ring 属性值压入栈中
		if(!lua_istable(l, -1)) {
			// 错误的 ring 属性值类型
			ERROR("Invalid relay ring node settings, should be a Lua table fill with node settings");
			lua_pop(l, 2);	// 弹出 ring 属性值和 relay ring table 值

			assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
			return;
		}

		// 构造 relay ring
		constructRingFromLua(l, _relay_ring, _relay_pool, true);

		lua_pop(l, 1);	// 弹出 ring 属性值

		_relay_ring_ver = relay_ring_ver;	// 更新内部配置数据版本
	}

	lua_pop(l, 1);	// 弹出 relay ring table 值
	assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
}

/**
 * 假定当前 Lua 栈顶元素为包含所有 node 的 table，根据其内容创建对应的 node 添加到 ring 中。
 * */
void Config::constructRingFromLua(Lua &l, shared_ptr<Ring> &ring, shared_ptr<BackendPool> &pool, bool is_relay)
{
	ScopedLoggingCtx logging_ctx("<Config::constructRingFromLua>");
	size_t n = lua_objlen(l, -1);
	if(n > 0) {
		// 遍历 node table
		lua_pushnil(l);	// first key
		while(lua_next(l, -2) != 0) {
			// use 'key' (at index -2) and 'value' (at index -1)
			if(lua_istable(l, -1)) {
				// 栈顶 value 为 table，作为 node 处理
				uint32_t id;
				vector<pair<string,int> > addrs;
				
				// 获取 node id
				lua_pushstring(l, "id");	// 压入 "id" 字符串
				lua_gettable(l, -2);	// 弹出 "id" 字符串，获取 node 的 id 属性，压入 id 属性值
				if(!lua_isnumber(l, -1)) {
					// node 的 id 属性不是数值，报错并跳过当前 node
					ERROR("Invalid node id: " + string(lua_tostring(l, -1)));
					lua_pop(l, 1);	// 弹出 id 属性值
					goto next;
				}
				id = (uint32_t)lua_tonumber(l, -1);
				lua_pop(l, 1);

				// 获取 node addr
				lua_pushstring(l, "addr");	// 压入 "addr" 字符串
				lua_gettable(l, -2);	// 弹出 "addr" 字符串，获取 node 的 addr 属性，压入 addr 属性值
				if(!lua_istable(l, -1)) {
					// node 的 addr 属性不是 table，报错并跳过当前 node
					ERROR("Invalid node addr: id = " + lexical_cast<string>(id));
					lua_pop(l, 1);	// 弹出 addr 属性值
					goto next;
				}
				{
					// 遍历当前 node 的 addr table
					lua_pushnil(l);	// first key
					while(lua_next(l, -2) != 0) {
						// use 'key' (at index -2) and 'value' (at index -1)
						if(lua_istable(l, -1)) {
							// 当前项为 table
							int port;
							string host;

							// 获取 addr port
							lua_pushstring(l, "port");	// 压入 "port" 字符串
							lua_gettable(l, -2);	// 弹出 "port" 字符串，获取 addr 的 port 属性，压入 port 属性值
							if(!lua_isnumber(l, -1)) {
								// addr 的 port 属性不是数值，报错并跳过当前 addr
								ERROR("Invalid addr port: " + string(lua_tostring(l, -1)));
								lua_pop(l, 1);	// 弹出 port 属性值
								goto inner_next;
							}
							port = (int)lua_tonumber(l, -1);
							lua_pop(l, 1);	// 弹出 port 属性值
							if(port < 0) {
								// port 属性值有误，报错并跳过当前 addr
								ERROR("Invalid addr port: " + lexical_cast<string>(port));
								goto inner_next;
							}

							// 获取 addr host
							lua_pushstring(l, "host");	// 压入 "host" 字符串
							lua_gettable(l, -2);	// 弹出 "host" 字符串，获取 addr 的 host 属性，压入 host 属性值
							if(!lua_isstring(l, -1)) {
								// addr 的 host 属性不是字符串，报错并跳过当前 addr
								ERROR("Invalid addr host: id = " + lexical_cast<string>(id));
								lua_pop(l, 1);	// 弹出 host 属性值
								goto inner_next;
							}
							host = string(lua_tostring(l, -1));
							lua_pop(l, 1);	// 弹出 host 属性值

							// 将当前 <host,port> 对插入 node 的地址列表
							addrs.push_back(pair<string,int>(host, port));
						}
inner_next:
						lua_pop(l, 1);	// removes 'value'; keeps 'key' for next iteration
					}
				}

				lua_pop(l, 1);	// 弹出 addr 属性值

				// 创建 node 对象插入 ring
				if(!is_relay) {
					// 创建普通 node
					shared_ptr<Node> node(new Node(pool, id, addrs));
					ring->addNode(node);
				} else {
					// 创建 relay node
					shared_ptr<Node> node(new RelayNode(pool, id, addrs));
					ring->addNode(node);
				}
			}
next:
			lua_pop(l, 1);	// removes 'value'; keeps 'key' for next iteration
		}
	}
}

void Config::setRelayDomainFromLua(Lua &l)
{
	ScopedLoggingCtx logging_ctx("<Config::setRelayDomainFromLua>");
	lua_getglobal(l, BHT_LUA_RDOMAIN_NAME);	// 获取 relay domain table 压入栈中
	if(!lua_istable(l, -1)) {
		// 栈顶的 relay domain table 值类型有误
		ERROR("Invalid relay domain configuration. Please check config file...");
		lua_pop(l, 1);	// 弹出 relay domain table 值

		assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
		return;
	}

	// 检查 relay domain table 中的元素是否合法
	{
		// 遍历 relay domain table
		lua_pushnil(l);	// first key
		while(lua_next(l, -2) != 0) {
			// use 'key' (at index -2) and 'value' (at index -1)
			size_t len;
			const char *s = lua_tolstring(l, -1, &len);
			if(!s) {
				// 当前元素无法表达为字符串
				ERROR("Invalid domain name in relay domain list, please check config files...");
				lua_pop(l, 3);	// 弹出 key、val 和 table

				assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
				return;
			}

			string domain(s, len);
			if(!Validator::isValidDomain(domain)) {
				// 当前元素不是合法的 domain
				ERROR("Invalid domain name in relay domain list, please check config files: " + domain);
				lua_pop(l, 3);	// 弹出 key、val 和 table

				assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
				return;
			}

			lua_pop(l, 1);	// removes 'value'; keeps 'key' for next iteration
		}
	}

	// 清空内部使用的 relay domain 集合
	_relay_domain.clear();

	// 设置内部使用的 relay domain 集合
	{
		lua_pushnil(l);	// first key
		while(lua_next(l, -2) != 0) {
			// use 'key' (at index -2) and 'value' (at index -1)
			size_t len;
			const char *s = lua_tolstring(l, -1, &len);
			string domain(s, len);

			// 将当前元素插入 relay domain 集合
			_relay_domain.insert(domain);

			lua_pop(l, 1);
		}
	}

	lua_pop(l,1);	// 弹出 relay domain table 值
	assert(lua_gettop(l) == 0);	// 确保 Lua 栈正确
}

shared_ptr<Ring> Config::getStorageRing()
{
	RWGuard guard(_conf_rwlock);

	return _storage_ring;
}

shared_ptr<Ring> Config::getLookupRing()
{
	RWGuard guard(_conf_rwlock);

	return _lookup_ring;
}

shared_ptr<Ring> Config::getCacheRing()
{
	RWGuard guard(_conf_rwlock);

	return _cache_ring;
}

shared_ptr<Ring> Config::getRelayRing()
{
	RWGuard guard(_conf_rwlock);

	return _relay_ring;
}

bool Config::isRelayDomain(const string &domain)
{
	if(_relay_domain.find(domain) != _relay_domain.end()) {
		return true;
	}

	return false;
}

void Config::updateConfig()
{
	ScopedLoggingCtx logging_ctx("<Config::updateConfig>");
	RWGuard guard(_conf_rwlock, true);
	initConfig();
}

const string& Config::getHandlerType()
{
	RWGuard guard(_conf_rwlock);

	return _handler_type;
}

shared_ptr<BHTIf> Config::getHandlerInstance()
{
	ScopedLoggingCtx logging_ctx("<Config::getHandlerInstance>");
	RWGuard guard(_conf_rwlock);

	if(_handler_type == "seq") {	// sequential handler
		return shared_ptr<BHTIf>(new SeqHandler());
	} else if(_handler_type == "tbb") {	// tbb parallel handler
		return shared_ptr<BHTIf>(new TBBHandler());
	} else if(_handler_type == "cus") {	// customized parallel handler
		return shared_ptr<BHTIf>(new CusHandler());
	} else if(_handler_type == "nul") {	// null handler
		return shared_ptr<BHTIf>(new NulHandler());
	}

	return shared_ptr<BHTIf>((BHTIf*)NULL);
}

int32_t Config::getHandlerWorkerNumber()
{
	RWGuard guard(_conf_rwlock);

	return _handler_worker_num;
}

BHT_CODE_END

