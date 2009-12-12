/**
 * Config 模块负责解析 BHT 配置文件、远程获取集中配置数据及本地配置数据缓存管理。使用 Thrift 的
 * TNonblockingServer 作为主体服务框架时，由于多个 worker 以 thread 方式存在，Config 作为公用资源
 * 应使用 rwlock 进行互斥保护，防止出现竞态条件。
 * */
#ifndef BHT_CONFIG_H__
#define BHT_CONFIG_H__

#include "Common.h"
#include "Ring.h"
#include "Node.h"
#include "BackendPool.h"
#include "LuaWrapper.h"

BHT_CODE_BEGIN

class Config {
public:
	static Config& getInstance(const ::std::string &conf = ::std::string());
	static void destroyInstance();

	~Config();

	::boost::shared_ptr<Ring> getStorageRing();
	::boost::shared_ptr<Ring> getLookupRing();
	::boost::shared_ptr<Ring> getCacheRing();
	::boost::shared_ptr<Ring> getRelayRing();

	::boost::shared_ptr<BHTIf> getHandlerInstance();	// 获得配置文件中指定类型的一个 Handler 实例
	const ::std::string& getHandlerType();	// 获得配置文件中指定的 Handler 类型字符串
	int32_t getHandlerWorkerNumber();

	bool isRelayDomain(const ::std::string &domain);

	void updateConfig();

private:
	Config(const ::std::string &conf);

	static ::boost::shared_ptr<BackendFactory> getBackendFactory(const ::std::string &backend_id);

	void initConfig();
	void setHandlerFromLua(Lua &l);
	void setCacheRingFromLua(Lua &l);
	void setLookupRingFromLua(Lua &l);
	void setStorageRingFromLua(Lua &l);
	void setRelayRingFromLua(Lua &l);
	void setRelayDomainFromLua(Lua &l);
	void constructRingFromLua(Lua &l, ::boost::shared_ptr<Ring> &ring, ::boost::shared_ptr<BackendPool> &pool, bool is_relay = false);

	::apache::thrift::concurrency::ReadWriteMutex _conf_rwlock;
	::std::string _conf_path;

	::std::string _handler_type;
	int32_t _handler_worker_num;

	uint32_t _storage_ring_ver;
	::boost::shared_ptr<Ring> _storage_ring;
	::boost::shared_ptr<BackendPool> _storage_pool;

	uint32_t _lookup_ring_ver;
	::boost::shared_ptr<Ring> _lookup_ring;
	::boost::shared_ptr<BackendPool> _lookup_pool;

	uint32_t _cache_ring_ver;
	::boost::shared_ptr<Ring> _cache_ring;
	::boost::shared_ptr<BackendPool> _cache_pool;

	uint32_t _relay_ring_ver;
	::boost::shared_ptr<Ring> _relay_ring;
	::boost::shared_ptr<BackendPool> _relay_pool;

	::std::set< ::std::string > _relay_domain;

	static Config *_instance;
};

BHT_CODE_END

#endif

