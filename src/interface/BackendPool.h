#ifndef BHT_BACKENDPOOL_H__
#define BHT_BACKENDPOOL_H__

#include "Common.h"
#include "Backend.h"

BHT_CODE_BEGIN

/// 后端主机连接池(单链表实现)
class ListPool {
public:
	ListPool(::boost::shared_ptr<BackendFactory> factory, const ::std::string &host, int port);
	~ListPool();

	::boost::shared_ptr<Backend> allocBackend();
	void freeBackend(::boost::shared_ptr<Backend> backend);

private:
	::std::string _host;
	int _port;
	::boost::shared_ptr<BackendFactory> _factory;

	::apache::thrift::concurrency::Mutex _pool_mtx;
	::std::list< ::boost::shared_ptr<Backend> > _free_pool;
};

class BackendPool {
public:
	typedef ::std::map< ::std::pair< ::std::string, int>, ::boost::shared_ptr<ListPool> > PoolType;

	BackendPool(::boost::shared_ptr<BackendFactory> factory);
	~BackendPool();

	::boost::shared_ptr<Backend> allocBackend(const ::std::string &host, int port);
	void freeBackend(::boost::shared_ptr<Backend> backend);

	void clear();

private:
	::boost::shared_ptr<BackendFactory> _factory;

	::apache::thrift::concurrency::ReadWriteMutex _pool_rwlock;
	PoolType _host_pool;
};

BHT_CODE_END

#endif

