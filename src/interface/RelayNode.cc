#include "RelayNode.h"
#include "RelayBackend.h"
#include "Logging.h"

#include <boost/cast.hpp>

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.RelayNode")

RelayNode::RelayNode(shared_ptr<BackendPool> pool, uint32_t id, vector<pair<string,int> > &hosts)
	:Node(pool, id, hosts)
{
}

RelayNode::~RelayNode()
{
}

void RelayNode::onSet(const string &domain, const Key &k, const Val &v)
{
	ScopedLoggingCtx logging_ctx("<RelayNode::onSet>");
	string host;
	int port;

	for(vector<pair<string,int> >::iterator it = _node_hosts.begin(); it != _node_hosts.end();) {
		host = it->first;
		port = it->second;
		++it;

		try {
			shared_ptr<Backend> p = _pool->allocBackend(host, port);
			if(p) {
				// 重做 Backend 对象指针为 RelayBackend 对象指针，以使用扩展接口
				RelayBackend *q = polymorphic_cast<RelayBackend*>(p.get());

				// 调用 Relay 后端接口
				q->onSet(domain, k, v);
				_pool->freeBackend(p);

				return;
			}

			ERROR("Failed to allocate relay backend connection: host = " + host + ", port = " + lexical_cast<string>(port));
		} catch(InvalidOperation &e) {
			ERROR("Error occured during operation: " + string(e.msg));
			// 若当前尝试的已经是最后一个主机，则抛出最后的异常
			if(it == _node_hosts.end()) {
				throw e;
			}
		}
	}
}

void RelayNode::onDel(const string &domain, const Key &k)
{
	ScopedLoggingCtx logging_ctx("<RelayNode::onDel>");
	string host;
	int port;

	for(vector<pair<string,int> >::iterator it = _node_hosts.begin(); it != _node_hosts.end();) {
		host = it->first;
		port = it->second;
		++it;

		try {
			shared_ptr<Backend> p = _pool->allocBackend(host, port);
			if(p) {
				// 重做 Backend 对象指针为 RelayBackend 对象指针，以使用扩展接口
				RelayBackend *q = polymorphic_cast<RelayBackend*>(p.get());

				// 调用 Relay 后端接口
				q->onDel(domain, k);
				_pool->freeBackend(p);

				return;
			}

			ERROR("Failed to allocate relay backend connection: host = " + host + ", port = " + lexical_cast<string>(port));
		} catch(InvalidOperation &e) {
			ERROR("Error occured during operation: " + string(e.msg));
			// 若当前尝试的已经是最后一个主机，则抛出最后的异常
			if(it == _node_hosts.end()) {
				throw e;
			}
		}
	}
}

void RelayNode::onMSet(const string &domain, const map<Key, Val> &kvs)
{
	ScopedLoggingCtx logging_ctx("<RelayNode::onMSet>");
	string host;
	int port;

	for(vector<pair<string,int> >::iterator it = _node_hosts.begin(); it != _node_hosts.end();) {
		host = it->first;
		port = it->second;
		++it;

		try {
			shared_ptr<Backend> p = _pool->allocBackend(host, port);
			if(p) {
				// 重做 Backend 对象指针为 RelayBackend 对象指针，以使用扩展接口
				RelayBackend *q = polymorphic_cast<RelayBackend*>(p.get());

				// 调用 Relay 后端接口
				q->onMSet(domain, kvs);
				_pool->freeBackend(p);

				return;
			}

			ERROR("Failed to allocate relay backend connection: host = " + host + ", port = " + lexical_cast<string>(port));
		} catch(InvalidOperation &e) {
			ERROR("Error occured during operation: " + string(e.msg));
			// 若当前尝试的已经是最后一个主机，则抛出最后的异常
			if(it == _node_hosts.end()) {
				throw e;
			}
		}
	}
}

void RelayNode::onMDel(const string &domain, const vector<Key> &ks)
{
	ScopedLoggingCtx logging_ctx("<RelayNode::onMDel>");
	string host;
	int port;

	for(vector<pair<string,int> >::iterator it = _node_hosts.begin(); it != _node_hosts.end();) {
		host = it->first;
		port = it->second;
		++it;

		try {
			shared_ptr<Backend> p = _pool->allocBackend(host, port);
			if(p) {
				// 重做 Backend 对象指针为 RelayBackend 对象指针，以使用扩展接口
				RelayBackend *q = polymorphic_cast<RelayBackend*>(p.get());

				// 调用 Relay 后端接口
				q->onMDel(domain, ks);
				_pool->freeBackend(p);

				return;
			}

			ERROR("Failed to allocate relay backend connection: host = " + host + ", port = " + lexical_cast<string>(port));
		} catch(InvalidOperation &e) {
			ERROR("Error occured during operation: " + string(e.msg));
			// 若当前尝试的已经是最后一个主机，则抛出最后的异常
			if(it == _node_hosts.end()) {
				throw e;
			}
		}
	}
}

BHT_CODE_END

