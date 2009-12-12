#include "Node.h"
#include "Logging.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.Node")

Node::Node(shared_ptr<BackendPool> pool, uint32_t id, vector<pair<string,int> > &hosts)
	:_id(id), _node_hosts(hosts)
{
	_pool = pool;
}

Node::~Node()
{
}

bool Node::get(const InternalKey &key, string *val)
{
	ScopedLoggingCtx logging_ctx("<Node::get>");
	string host;
	int port;

	for(vector<pair<string,int> >::iterator it = _node_hosts.begin(); it != _node_hosts.end();) {
		host = it->first;
		port = it->second;
		++it;

		try {
			shared_ptr<Backend> p = _pool->allocBackend(host, port);
			if(p) {
				string k = key.toString();
				string v;

				// 调用后端存储接口
				bool flag = p->get(k, &v);
				_pool->freeBackend(p);

				if(flag) {
					if(v[0] == 0) {
						return false;
					}
					// 去除返回值前的逻辑删除标志
					*val = v.substr(1);
				}

				return flag;
			}

			ERROR("Failed to allocate backend connection: host = " + host + ", port = " + lexical_cast<string>(port));
		} catch(InvalidOperation &e) {
			ERROR("Error occured during operation: " + string(e.msg));
			// 若当前尝试的已经是最后一个主机，则抛出最后的异常
			if(it == _node_hosts.end()) {
				throw e;
			}
		}
	}

	return false;
}

void Node::mget(map<InternalKey*, pair<bool, string> > *kvs)
{
	ScopedLoggingCtx logging_ctx("<Node::mget>");
	string host;
	int port;

	for(vector<pair<string,int> >::iterator it = _node_hosts.begin(); it != _node_hosts.end();) {
		host = it->first;
		port = it->second;
		++it;

		try {
			shared_ptr<Backend> p = _pool->allocBackend(host, port);
			if(p) {
				map<string, pair<bool, string> > skvs;

				map<InternalKey*, pair<bool, string> >::iterator it = kvs->begin();
				for(;it != kvs->end(); ++it) {
					string k = it->first->toString();
					skvs[k] = pair<bool, string>(false, string());
				}

				// 调用后端存储接口
				p->mget(&skvs);
				_pool->freeBackend(p);

				it = kvs->begin();
				for(;it != kvs->end(); ++it) {
					string k = it->first->toString();
					bool flag = skvs[k].first;
					const string &v = skvs[k].second;

					if(flag) {
						if(v[0] == 0) {
							// 记录已经被逻辑删除
							it->second.first = false;
							it->second.second = string();
						} else {
							// 记录确实存在
							it->second.first = true;
							// 去除记录值前的逻辑删除标志
							it->second.second = v.substr(1);
						}
					} else {
						it->second.first = false;
						it->second.second = string();
					}
				}

				return;
			}

			ERROR("Failed to allocate backend connection: host = " + host + ", port = " + lexical_cast<string>(port));
		} catch(InvalidOperation &e) {
			ERROR("Error occured during operation: " + string(e.msg));
			// 若当前尝试的已经是最后一个主机，则抛出最后的异常
			if(it == _node_hosts.end()) {
				throw e;
			}
		}
	}
}

void Node::set(const InternalKey &key, const string &val, bool overwrite, int timeout)
{
	ScopedLoggingCtx logging_ctx("<Node::set>");
	string host;
	int port;

	for(vector<pair<string,int> >::iterator it = _node_hosts.begin(); it != _node_hosts.end();) {
		host = it->first;
		port = it->second;
		++it;

		try {
			shared_ptr<Backend> p = _pool->allocBackend(host, port);
			if(p) {
				string k = key.toString();
				// 在给定数据之前添加逻辑删除标志
				string v(1, '\001');
				v.append(val);

				// 调用后端存储接口
				p->set(k, v, overwrite, timeout);
				_pool->freeBackend(p);

				return;
			}

			ERROR("Failed to allocate backend connection: host = " + host + ", port = " + lexical_cast<string>(port));
		} catch(InvalidOperation &e) {
			ERROR("Error occured during operation: " + string(e.msg));
			// 若当前尝试的已经是最后一个主机，则抛出最后的异常
			if(it == _node_hosts.end()) {
				throw e;
			}
		}
	}
}

void Node::out(const InternalKey &key, bool overwrite)
{
	ScopedLoggingCtx logging_ctx("<Node::out>");
	string host;
	int port;

	for(vector<pair<string,int> >::iterator it = _node_hosts.begin(); it != _node_hosts.end();) {
		host = it->first;
		port = it->second;
		++it;

		try {
			shared_ptr<Backend> p = _pool->allocBackend(host, port);
			if(p) {
				string k = key.toString();
				// 构造逻辑删除记录
				string v(1, '\000');

				// 调用后端存储接口
				p->set(k, v, overwrite, 0);
				_pool->freeBackend(p);

				return;
			}

			ERROR("Failed to allocate backend connection: host = " + host + ", port = " + lexical_cast<string>(port));
		} catch(InvalidOperation &e) {
			ERROR("Error occured during operation: " + string(e.msg));
			// 若当前尝试的已经是最后一个主机，则抛出最后的异常
			if(it == _node_hosts.end()) {
				throw e;
			}
		}
	}
}

void Node::del(const InternalKey &key)
{
	ScopedLoggingCtx logging_ctx("<Node::del>");
	string host;
	int port;

	for(vector<pair<string,int> >::iterator it = _node_hosts.begin(); it != _node_hosts.end();) {
		host = it->first;
		port = it->second;
		++it;

		try {
			shared_ptr<Backend> p = _pool->allocBackend(host, port);
			if(p) {
				string k = key.toString();

				// 调用后端存储接口
				p->del(k);
				_pool->freeBackend(p);

				return;
			}

			ERROR("Failed to allocate backend connection: host = " + host + ", port = " + lexical_cast<string>(port));
		} catch(InvalidOperation &e) {
			ERROR("Error occured during operation: " + string(e.msg));
			// 若当前尝试的已经是最后一个主机，则抛出最后的异常
			if(it == _node_hosts.end()) {
				throw e;
			}
		}
	}
}

bool Node::operator<(const Node& rhs) const
{
	return _id < rhs._id;
}

bool Node::operator==(const Node& rhs) const
{
	return _id == rhs._id;
}

BHT_CODE_END

