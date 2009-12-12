#include "Cache.h"
#include "Logging.h"
#include "Ring.h"
#include "Node.h"
#include "Config.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.Cache")

Cache* Cache::_instance = NULL;

Cache& Cache::getInstance()
{
	ScopedLoggingCtx logging_ctx("<Cache::getInstance>");
	if(!_instance) {
		_instance = new Cache();
	}
	return *_instance;
}

void Cache::destroyInstance()
{
	ScopedLoggingCtx logging_ctx("<Cache::destroyInstance>");
	if(_instance) {
		delete _instance;
		_instance = NULL;
	}
}

bool Cache::get(const InternalKey &key, string *val)
{
	ScopedLoggingCtx logging_ctx("<Cache::get>");
	Config& conf = Config::getInstance();
	shared_ptr<Ring> c_ring = conf.getCacheRing();
	shared_ptr<Node> c_node = c_ring->getNode(key);

	if(!c_node) {
		return false;
	}

	try {
		return c_node->get(key, val);
	} catch(...) {
		return false;
	}
}

void Cache::mget(::std::map<InternalKey*, ::std::pair<bool, ::std::string> >* kvs)
{
	typedef map<InternalKey*, pair<bool, string> > KvMapT;
	typedef map<shared_ptr<Node>, KvMapT> NodeGroup;

	ScopedLoggingCtx logging_ctx("<Cache::mget>");
	Config& conf = Config::getInstance();
	shared_ptr<Ring> c_ring = conf.getCacheRing();

	KvMapT::iterator it = kvs->begin(), end = kvs->end();
	NodeGroup node_group;
	// 把key按照哈希到得cache node分组
	for (; it!=end; ++it) {
		shared_ptr<Node> c_node = c_ring->getNode(*it->first);
		NodeGroup::iterator pos;
		if ((pos = node_group.find(c_node)) == node_group.end()) {
			KvMapT kvmap;
			pos = node_group.insert(make_pair(c_node, kvmap)).first;
		}
		KvMapT& kvmap = pos->second;
		kvmap[it->first] = it->second;
	}

	NodeGroup::iterator nit = node_group.begin(), nend = node_group.end();
	// 针对每个分组进行多键获取操作
	for (; nit!=nend; ++nit) {
		shared_ptr<Node> c_node = nit->first;
		KvMapT& kvmap = nit->second;
		c_node->mget(&kvmap);

		// 将从cache中获取的结果赋值给out参数
		it = kvmap.begin(), end = kvmap.end();
		for(; it!=end; ++it) {
			if (it->second.first) {
				(*kvs)[it->first] = it->second;
			}
		}
	}
}

void Cache::set(const InternalKey &key, const string &val, int timeout)
{
	ScopedLoggingCtx logging_ctx("<Cache::set>");
	Config& conf = Config::getInstance();
	shared_ptr<Ring> c_ring = conf.getCacheRing();
	shared_ptr<Node> c_node = c_ring->getNode(key);

	if(!c_node) {
		return;
	}

	try {
		c_node->set(key, val, true, timeout);
	} catch(...) {
	}
}

void Cache::del(const InternalKey &key)
{
	ScopedLoggingCtx logging_ctx("<Cache::del>");
	Config& conf = Config::getInstance();
	shared_ptr<Ring> c_ring = conf.getCacheRing();
	shared_ptr<Node> c_node = c_ring->getNode(key);

	if(!c_node) {
		return;
	}

	try {
		c_node->del(key);
	} catch(...) {
	}
}

BHT_CODE_END

