#include "BackendPool.h"
#include "Logging.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.BackendPool")

BackendPool::BackendPool(shared_ptr<BackendFactory> factory)
	:_factory(factory)
{
}

BackendPool::~BackendPool()
{
}

shared_ptr<Backend> BackendPool::allocBackend(const string &host, int port)
{
	ScopedLoggingCtx logging_ctx("<BackendPool::allocBackend>");
	PoolType::iterator it;

	{
		RWGuard guard(_pool_rwlock);

		// 在后端主机池中查找给定后端连接对应的主机连接池记录
		it = _host_pool.find(pair<string, int>(host, port));
		if(it != _host_pool.end()) {
			// 找到了给定后端连接对应的主机连接池，从其中分配连接实例
			return it->second->allocBackend();
		}
	}

	// 未发现给定后端连接对应的主机连接池记录，可能需要添加新记录
	RWGuard guard(_pool_rwlock, true);

	// 这里获取到锁时可能已经被其他线程创建了所需记录或清空了记录，故需要先检测一下
	it = _host_pool.find(pair<string, int>(host, port));
	if(it == _host_pool.end()) {
		// 所需记录尚未被创建
		shared_ptr<ListPool> p(new ListPool(_factory, host, port));
		_host_pool[pair<string, int>(host, port)] = p;
		return p->allocBackend();
	} else {
		// 所需记录已经被创建
		return it->second->allocBackend();
	}
}

void BackendPool::freeBackend(shared_ptr<Backend> backend)
{
	ScopedLoggingCtx logging_ctx("<BackendPool::freeBackend>");
	RWGuard guard(_pool_rwlock);

	string host = backend->host();
	int port = backend->port();

	// 在后端主机池中查找给定后端连接对应的主机连接池记录
	PoolType::iterator it = _host_pool.find(pair<string, int>(host, port));
	if(it != _host_pool.end()) {
		// 找到了给定后端连接对应的主机连接池，将连接交还回去
		it->second->freeBackend(backend);
	} else {
		// 未发现给定后端连接对应的主机连接池记录，记录警告信息并忽略给定连接实例
		WARN("Cannot find backend pool for the given backend instance, discarded: host = " + host + ", port = " + lexical_cast<string>(port));
	}
}

void BackendPool::clear()
{
	ScopedLoggingCtx logging_ctx("<BackendPool::clear>");
	RWGuard guard(_pool_rwlock, true);
	_host_pool.clear();
}

ListPool::ListPool(shared_ptr<BackendFactory> factory, const string &host, int port)
	:_host(host), _port(port)
{
	_factory = factory;
}

ListPool::~ListPool()
{
}

shared_ptr<Backend> ListPool::allocBackend()
{
	ScopedLoggingCtx logging_ctx("<ListPool::allocBackend>");
	{
		Guard guard(_pool_mtx);
		if(!_free_pool.empty()) {
			shared_ptr<Backend> p = _free_pool.front();
			_free_pool.pop_front();
			return p;
		}
	}

	shared_ptr<Backend> p = _factory->getBackendInstance(_host, _port);
	return p;
}

void ListPool::freeBackend(shared_ptr<Backend> backend)
{
	ScopedLoggingCtx logging_ctx("<ListPool::freeBackend>");
	if(backend->port() != _port || backend->host() != _host) {
		WARN("Incorrect backend freed: given host = " + backend->host() + ", port = " + lexical_cast<string>(backend->port())
				+ "; pool host = " + _host + ", port = " + lexical_cast<string>(_port));
		return;
	}

	Guard guard(_pool_mtx);
	_free_pool.push_back(backend);
}

BHT_CODE_END

