#include "NulHandler.h"
#include "Error.h"
#include "Node.h"
#include "Cache.h"
#include "Config.h"
#include "Logging.h"
#include "Validator.h"
#include "Error.h"
#include "RelayClient.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.NulHandler")

void NulHandler::MDel(const string &domain, const vector<Key> &ks, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<NulHandler::MDel>");
	// TODO
}

void NulHandler::MGet(map<Key, Val> &kvs, const string &domain, const vector<Key> &ks)
{
	ScopedLoggingCtx logging_ctx("<NulHandler::MGet>");
	// TODO
}

void NulHandler::MSet(const string &domain, const map<Key, Val> &kvs, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<NulHandler::MSet>");
	// TODO
}

void NulHandler::Set(const string &domain, const Key &k, const Val &v, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<NulHandler::Set>");
  // Null NulHandler!
}

void NulHandler::Del(const string &domain, const Key &k, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<NulHandler::Del>");
  // Null NulHandler!
}

void NulHandler::Get(Val &v, const string &domain, const Key &k)
{
	ScopedLoggingCtx logging_ctx("<NulHandler::Get>");
  // Null NulHandler!
	
}

void NulHandler::UpdateConfig()
{
	ScopedLoggingCtx logging_ctx("<NulHandler::UpdateConfig>");
	Config &conf = Config::getInstance();
	conf.updateConfig();
}

void NulHandler::cleanupRing(const PartitionKey &pkey, const string &s_id)
{
	ScopedLoggingCtx logging_ctx("<NulHandler::cleanupRing>");
	Config &conf = Config::getInstance();
	shared_ptr<Ring> l_ring = conf.getLookupRing();

	int cnt;
	Ring::iterator it = l_ring->getIterator(pkey);

	for(cnt = 0; cnt < 2; ++cnt, ++it) {
		shared_ptr<Node> l_node = *it;
		if(!l_node) {
			break;
		}

		if(cnt == 0) {
			// 当前 L-node 为新 L-ring 中负责指定 key 的节点，
			// 将指定的 key 同 S-node ID 对应记录在该节点上
			l_node->set(pkey, s_id);
		} else {
			// 当前 L-node 在新 L-ring 中不负责指定 key，
			// 从其上物理删除指定 key 对应的记录
			l_node->del(pkey);
		}
	}
}

shared_ptr<Node> NulHandler::locateNode(const PartitionKey &pkey)
{
	ScopedLoggingCtx logging_ctx("<NulHandler::locateNode>");
	Config &conf = Config::getInstance();
	shared_ptr<Ring> l_ring = conf.getLookupRing();

	// 以线性探查方式在 L-ring 上查找指定 key 对应的 S-node 记录
	string s_id;
	int cnt;
	Ring::iterator it = l_ring->getIterator(pkey);

	for(cnt = 0; cnt < 2 && it; ++cnt, ++it) {
		shared_ptr<Node> l_node = *it;
		if(!l_node) {
			return shared_ptr<Node>((Node*)NULL);
		}

		if(l_node->get(pkey, &s_id)) {
			// 在当前 L-node 上找到了指定 key 对应的 S-node ID
			break;
		}
	}

	if(s_id.size() == 0) {
		// 线性探查完毕仍未在 L-ring 上找到指定 key 对应的 S-node 记录，
		// 认为指定 key 对应记录不存在。
		return shared_ptr<Node>((Node*)NULL);
	}

	shared_ptr<Ring> s_ring = conf.getStorageRing();
	if(cnt != 0) {
		// 进行线性探查后才在其他 L-node 上发现了对应记录，说明 L-ring
		// 进行了扩容操作，先进行新老 L-node 之间的数据迁移
		cleanupRing(pkey, s_id);
	}
	return s_ring->getNodeById(lexical_cast<uint32_t>(s_id));
}

shared_ptr<Node> NulHandler::locateNodeOrInit(const PartitionKey &pkey)
{
	ScopedLoggingCtx logging_ctx("<NulHandler::locateNodeOrInit>");

	shared_ptr<Node> p = locateNode(pkey);
	if(!p) {
		// 无法从 L-ring 上找到指定的 key，从 S-ring 上定位负责指定 key
		// 的 S-node 并将其记录到 L-ring 中
		Config &conf = Config::getInstance();	// throws(InvalidOperation)

		shared_ptr<Ring> s_ring = conf.getStorageRing();
		shared_ptr<Node> s_node = s_ring->getNode(pkey);
		if(!s_node) {
			return shared_ptr<Node>((Node*)NULL);
		}

		shared_ptr<Ring> l_ring = conf.getLookupRing();
		shared_ptr<Node> l_node = l_ring->getNode(pkey);
		if(!l_node) {
			return shared_ptr<Node>((Node*)NULL);
		}

		l_node->set(pkey, lexical_cast<string>(s_node->id()));	// throws(InvalidOperation)

		// 返回负责指定 key 的 S-node
		return s_node;
	}

	return p;
}

BHT_CODE_END

