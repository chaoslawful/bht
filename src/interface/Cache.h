/**
 * Cache 模块负责进行记录缓存，通过简单的内存散列表实现即可。使用 Thrift 的 TNonblockingServer
 * 作为主体服务框架时，由于多个 worker 以 thread 方式存在，Cache 作为公用资源应使用 rwlock 进行
 * 互斥保护，防止出现竞态条件。
 * */
#ifndef BHT_CACHE_H__
#define BHT_CACHE_H__

#include "Common.h"
#include "InternalKey.h"

BHT_CODE_BEGIN

class Cache {
public:
	enum {
		CACHE_TIMEOUT = 0
	};

	static Cache& getInstance();
	static void destroyInstance();

	bool get(const InternalKey &key, ::std::string *val);
	void mget(::std::map<InternalKey*, ::std::pair<bool, ::std::string> >* kvs);
	void set(const InternalKey &key, const ::std::string &val, int timeout = 0);
	void del(const InternalKey &key);

	~Cache() {}

private:
	Cache() {}

	static Cache *_instance;
};

BHT_CODE_END

#endif

