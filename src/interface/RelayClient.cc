#include "RelayClient.h"
#include "Logging.h"
#include "Ring.h"
#include "Node.h"
#include "RelayNode.h"
#include "Config.h"

#include <boost/cast.hpp>

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.RelayClient")

RelayClient* RelayClient::_instance = NULL;

RelayClient& RelayClient::getInstance()
{
	ScopedLoggingCtx logging_ctx("<RelayClient::getInstance>");
	if(!_instance) {
		_instance = new RelayClient();
	}
	return *_instance;
}

void RelayClient::destroyInstance()
{
	ScopedLoggingCtx logging_ctx("<RelayClient::destroyInstance>");
	if(_instance) {
		delete _instance;
		_instance = NULL;
	}
}

void RelayClient::onSet(const string &domain, const Key &k, const Val &v)
{
	ScopedLoggingCtx logging_ctx("<RelayClient::onSet>");
	Config& conf = Config::getInstance();
	shared_ptr<Ring> r_ring = conf.getRelayRing();
	shared_ptr<Node> r_node = r_ring->getNode(domain);

	if(!r_node) {
		return;
	}

	try {
		// 将 Node 对象指针重做为 RelayNode 对象指针以便使用扩展接口
		RelayNode *node = polymorphic_cast<RelayNode*>(r_node.get());
		node->onSet(domain, k, v);
	} catch(...) {
	}
}

void RelayClient::onDel(const string &domain, const Key &k)
{
	ScopedLoggingCtx logging_ctx("<RelayClient::onDel>");
	Config& conf = Config::getInstance();
	shared_ptr<Ring> r_ring = conf.getRelayRing();
	shared_ptr<Node> r_node = r_ring->getNode(domain);

	if(!r_node) {
		return;
	}

	try {
		// 将 Node 对象指针重做为 RelayNode 对象指针以便使用扩展接口
		RelayNode *node = polymorphic_cast<RelayNode*>(r_node.get());
		node->onDel(domain, k);
	} catch(...) {
	}
}

void RelayClient::onMSet(const string &domain, const map<Key, Val> &kvs)
{
	ScopedLoggingCtx logging_ctx("<RelayClient::onMSet>");
	Config& conf = Config::getInstance();
	shared_ptr<Ring> r_ring = conf.getRelayRing();
	shared_ptr<Node> r_node = r_ring->getNode(domain);

	if(!r_node) {
		return;
	}

	try {
		// 将 Node 对象指针重做为 RelayNode 对象指针以便使用扩展接口
		RelayNode *node = polymorphic_cast<RelayNode*>(r_node.get());
		node->onMSet(domain, kvs);
	} catch(...) {
	}
}

void RelayClient::onMDel(const string &domain, const vector<Key> &ks)
{
	ScopedLoggingCtx logging_ctx("<RelayClient::onMDel>");
	Config& conf = Config::getInstance();
	shared_ptr<Ring> r_ring = conf.getRelayRing();
	shared_ptr<Node> r_node = r_ring->getNode(domain);

	if(!r_node) {
		return;
	}

	try {
		// 将 Node 对象指针重做为 RelayNode 对象指针以便使用扩展接口
		RelayNode *node = polymorphic_cast<RelayNode*>(r_node.get());
		node->onMDel(domain, ks);
	} catch(...) {
	}
}

BHT_CODE_END

