#include "CusHandler.h"
#include "Error.h"
#include "Node.h"
#include "Cache.h"
#include "Config.h"
#include "Logging.h"
#include "Validator.h"
#include "Error.h"
#include "RelayClient.h"
#include "Processor.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.CusHandler")

void CusHandler::MDel(const string &domain, const vector<Key> &ks, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<CusHandler::MDel>");

	vector<Key> nks;
	for (vector<Key>::const_iterator it = ks.begin(), end = ks.end(); it != end; ++it) {
		const Key& k = *it;

		// 验证 domain 有效性
		if(!Validator::isValidDomain(domain)) {
			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_DOMAIN;
			ex.msg = "Invalid domain given to del";
			throw ex;
		}

		if(!k.__isset.key) {
			// 没有给出待删除的 key，抛出异常
			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_KEY;
			ex.msg = "Necessary argument 'key' not given";
			throw ex;
		}

		// 验证 key 有效性
		if(!Validator::isValidKey(k.key)) {
			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_KEY;
			ex.msg = "Invalid key given to del";
			throw ex;
		}

		if(k.__isset.subkey) {
			// 验证 subkey 有效性
			if(!Validator::isValidSubkey(k.subkey)) {
				InvalidOperation ex;
				ex.ec = Error::BHT_EC_INVALID_SUBKEY;
				ex.msg = "Invalid subkey given to del";
				throw ex;
			}
		}

		if(!k.__isset.timestamp) {
			DEBUG("Timestamp not set in the given key, using current epoch instead");
		}
		Key nk;
		nk.key = k.key;
		nk.subkey = k.subkey;
		BHT_SET_CUR_EPOCH(nk.timestamp);
		nk.__isset.key = k.__isset.key;
		nk.__isset.subkey = k.__isset.subkey;
		nk.__isset.timestamp = nk.__isset.timestamp;
		nks.push_back(nk);
	}

	Config &conf = Config::getInstance();	// throws(InvalidOperation)

	// 若当前操作允许 relay，且 domain 在同步列表中，则使用 relay 服务将数据同步到远端
	if(relay && conf.isRelayDomain(domain)) {
		RelayClient &relay = RelayClient::getInstance();
		relay.onMDel(domain, nks);
	}

	shared_ptr<Processor> processor = Processor::getInstance();
	{
		TaskSynchronizer sync(static_cast<int32_t>(nks.size()));
		for (vector<Key>::iterator it = nks.begin(), end = nks.end(); it != end; ++it) {
			shared_ptr<Task> task(new TaskDel(domain, *it, overwrite));
			processor->Process(task, sync);
		}
		sync.Wait();
	}
}

void CusHandler::MGet(map<Key, Val> &kvs, const string &domain, const vector<Key> &ks)
{
	ScopedLoggingCtx logging_ctx("<CusHandler::MGet>");

	// 验证 domain 有效性
	if(!Validator::isValidDomain(domain)) {
		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_DOMAIN;
		ex.msg = "Invalid domain given to get";
		throw ex;
	}

	typedef map<StorageKey*, const Key*> StorageKeyMap;
	typedef map<shared_ptr<Node>, StorageKeyMap> NodeMap;
	vector<StorageKey> sks;

	StorageKeyMap skmap;
	// 验证各个 key 的有效性, 并生成 storage key
	for (vector<Key>::const_iterator it=ks.begin(), end=ks.end(); it != end; ++it) {
		string key, subkey;

		const Key& k = *it;
		if(k.__isset.key) {
			key = k.key;

			// 验证 key 有效性
			if(!Validator::isValidKey(key)) {
				InvalidOperation ex;
				ex.ec = Error::BHT_EC_INVALID_KEY;
				ex.msg = "Invalid key given to get";
				throw ex;
			}
		} else {
			// 没有给出待查询的 key，抛出异常
			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_KEY;
			ex.msg = "Necessary argument 'key' not given";
			throw ex;
		}

		if(k.__isset.subkey) {
			subkey = k.subkey;

			// 验证 subkey 有效性
			if(!Validator::isValidSubkey(subkey)) {
				InvalidOperation ex;
				ex.ec = Error::BHT_EC_INVALID_SUBKEY;
				ex.msg = "Invalid subkey given to get";
				throw ex;
			}
		}
		sks.push_back(StorageKey(domain, it->key, it->subkey));
		skmap.insert(make_pair(&(*sks.rbegin()), &k));
	}

	Config& conf = Config::getInstance();
	shared_ptr<Ring> c_ring = conf.getCacheRing();

	NodeMap c_node_map;
	// 把 key 按照 hash 所在的 cache node 分组
	for (StorageKeyMap::iterator it = skmap.begin(), end = skmap.end(); it!=end; ++it) {
		shared_ptr<Node> c_node = c_ring->getNode(*(it->first));
		if (!c_node) {
			continue;
		}
		NodeMap::iterator pos;
		if ((pos = c_node_map.find(c_node)) == c_node_map.end()) {
			pos = c_node_map.insert(make_pair(c_node, StorageKeyMap())).first;
		}
		pos->second.insert(make_pair(it->first, it->second));
	}
	
	Mutex mutex;
	shared_ptr<Processor> processor = Processor::getInstance();
	// 从各个 cache node 获取 key 值
	{
		TaskSynchronizer sync(static_cast<int32_t>(c_node_map.size()));
		for (NodeMap::iterator it = c_node_map.begin(), end = c_node_map.end(); it != end; ++it) {
			shared_ptr<Task> task(new TaskMGetFromCache(kvs, it->first, it->second, skmap, mutex));
			processor->Process(task, sync);
		}
		sync.Wait();
	}
	c_node_map.clear();

	// 此时 skmap 中为需要从 S-ring 中取出的 key
	if (skmap.empty()) {
		// 所有 key 都在缓存中找到了
		return;
	}

	shared_ptr<Ring> s_ring = conf.getStorageRing();
	NodeMap s_node_map;
	// 将 key 按照所在的 S-node 分组
	for (StorageKeyMap::iterator it = skmap.begin(), end = skmap.end(); it != end; ) {
		shared_ptr<Node> s_node = s_ring->getNode(*(it->first));
		if (!s_node) {
			skmap.erase(it->first);
			continue;
		}
		NodeMap::iterator pos;
		if ((pos = s_node_map.find(s_node)) == s_node_map.end()) {
			pos = s_node_map.insert(make_pair(s_node, StorageKeyMap())).first;
		}
		pos->second.insert(make_pair(it->first, it->second));
	}

	// 在多个线程中进行各个分组中 S-node 上的 mget 操作
	{
		TaskSynchronizer sync(static_cast<int32_t>(s_node_map.size()));
		for (NodeMap::iterator it = s_node_map.begin(), end = s_node_map.end(); it != end; ++it) {
			shared_ptr<Task> task(new TaskMGetFromSNode(kvs, it->first, it->second, skmap, mutex));
			processor->Process(task, sync);
		}
		sync.Wait();
	}
	s_node_map.clear();

	// 此时 skmap 中为需要从 L-ring 中重新定位的 key
	if (skmap.empty()) {
		// 所有 key 都在 S-ring 中找到了
		return;
	}

	NodeMap s_node_map2;
	// 在多个线程中进行各个 key 在 L-ring 中的重新定位操作
	{
		TaskSynchronizer sync(static_cast<int32_t>(skmap.size()));
		for (StorageKeyMap::iterator it = skmap.begin(), end = skmap.end(); it != end; ) {
			shared_ptr<Task> task(new TaskLocateNode(s_node_map2, domain, it->first, it->second, mutex));
			processor->Process(task, sync);
		}
		sync.Wait();
	}

	// 在多个线程中进行各个分组中 S-node 上的 mget 操作
	{
		TaskSynchronizer sync(static_cast<int32_t>(s_node_map2.size()));
		for (NodeMap::iterator it = s_node_map.begin(), end = s_node_map.end(); it != end; ++it) {
			shared_ptr<Task> task(new TaskMGetFromSNode(kvs, it->first, it->second, skmap, mutex));
			processor->Process(task, sync);
		}
		sync.Wait();
	}
}

void CusHandler::MSet(const string &domain, const map<Key, Val> &kvs, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<CusHandler::MSet>");

	// 验证 domain 有效性
	if(!Validator::isValidDomain(domain)) {
		ERROR("Invalid domain: '" + domain + "'");

		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_DOMAIN;
		ex.msg = "Invalid domain given to set";
		throw ex;
	}

	map<Key, Val> kvmap;
	// 验证 key-value 有效性，生成新的 key-value对，存入kvmap
	for (map<Key, Val>::const_iterator it = kvs.begin(), end = kvs.end(); it != end; ++it) {
		const Key& k = it->first;
		const Val& v = it->second;

		// 验证 value 有效性
		if(!Validator::isValidValue(v.value)) {
			ERROR("Invalid value: length = " + lexical_cast<string>(v.value.size()) + ", first 32 bytes are '" + v.value.substr(0,32) + "'");

			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_VALUE;
			ex.msg = "Invalid value given to set";
			throw ex;
		}

		if(!k.__isset.key) {
			ERROR("No valid key given");

			// 没有给出待设置的 key，抛出异常
			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_KEY;
			ex.msg = "Necessary argument 'key' not given";
			throw ex;
		}

		// 验证 key 有效性
		if(!Validator::isValidKey(k.key)) {
			ERROR("Invalid key: '" + k.key + "'");

			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_KEY;
			ex.msg = "Invalid key given to set";
			throw ex;
		}

		if(k.__isset.subkey) {
			// 验证 subkey 有效性
			if(!Validator::isValidSubkey(k.subkey)) {
				ERROR("Invalid subkey: '" + k.subkey + "'");

				InvalidOperation ex;
				ex.ec = Error::BHT_EC_INVALID_SUBKEY;
				ex.msg = "Invalid subkey given to set";
				throw ex;
			}
		}

		if(!k.__isset.timestamp) {
			DEBUG("Timestamp not set in the given key, using current epoch instead");
		}
		Key nk;
		nk.key = k.key;
		nk.subkey = k.subkey;
		BHT_SET_CUR_EPOCH(nk.timestamp);
		nk.__isset.key = true;
		nk.__isset.subkey = true;
		nk.__isset.timestamp = true;
		kvmap.insert(make_pair(nk, v));
	}

	Config &conf = Config::getInstance();	// throws(InvalidOperation)

	// 若当前操作允许 relay，且 domain 在同步列表中，则使用 relay 服务将数据同步到远端
	if(relay && conf.isRelayDomain(domain)) {
		RelayClient &relay = RelayClient::getInstance();
		relay.onMSet(domain, kvs);
	}

	shared_ptr<Processor> processor = Processor::getInstance();
	{
		TaskSynchronizer sync(static_cast<int32_t>(kvmap.size()));
		for (map<Key, Val>::iterator it = kvmap.begin(), end = kvmap.end(); it != end; ++it) {
			shared_ptr<Task> task(new TaskSet(domain, it->first, it->second, overwrite));
			processor->Process(task, sync);
		}
		sync.Wait();
	}
}

void CusHandler::Set(const string &domain, const Key &k, const Val &v, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<CusHandler::Set>");
	string key, subkey;
	int64_t timestamp;

	// 验证 domain 有效性
	if(!Validator::isValidDomain(domain)) {
		ERROR("Invalid domain: '" + domain + "'");

		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_DOMAIN;
		ex.msg = "Invalid domain given to set";
		throw ex;
	}

	// 验证 value 有效性
	if(!Validator::isValidValue(v.value)) {
		ERROR("Invalid value: length = " + lexical_cast<string>(v.value.size()) + ", first 32 bytes are '" + v.value.substr(0,32) + "'");

		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_VALUE;
		ex.msg = "Invalid value given to set";
		throw ex;
	}

	if(k.__isset.key) {
		key = k.key;

		// 验证 key 有效性
		if(!Validator::isValidKey(key)) {
			ERROR("Invalid key: '" + key + "'");

			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_KEY;
			ex.msg = "Invalid key given to set";
			throw ex;
		}

		if(k.__isset.subkey) {
			subkey = k.subkey;

			// 验证 subkey 有效性
			if(!Validator::isValidSubkey(subkey)) {
				ERROR("Invalid subkey: '" + subkey + "'");

				InvalidOperation ex;
				ex.ec = Error::BHT_EC_INVALID_SUBKEY;
				ex.msg = "Invalid subkey given to set";
				throw ex;
			}
		}

		if(k.__isset.timestamp) {
			timestamp = k.timestamp;
		} else {
			DEBUG("Timestamp not set in the given key, using current epoch instead");
			// 设置操作未指定时戳，默认为当前 us 级 UNIX 时戳
			BHT_SET_CUR_EPOCH(timestamp);
		}

		Config &conf = Config::getInstance();	// throws(InvalidOperation)

		// 若当前操作允许 relay，且 domain 在同步列表中，则使用 relay 服务将数据同步到远端
		if(relay && conf.isRelayDomain(domain)) {
			Key nk = k;
			Val nv = v;

			nk.__isset.timestamp = true;
			nk.timestamp = timestamp;

			RelayClient &relay = RelayClient::getInstance();
			relay.onSet(domain, nk, nv);
		}

		PartitionKey pkey = PartitionKey(domain, key);
		shared_ptr<Node> s_node;

		// 在 L-ring 中查找负责指定 key 的 S-node，若未找到则从 S-ring 中挑选一个 S-node
		// 并将其同指定 key 的关联关系设置到 L-ring 中
		s_node = locateNodeOrInit(pkey);

		// 清除 cache 中缓存的指定 key 对应记录
		Cache &cache = Cache::getInstance();
		cache.del(StorageKey(domain, key, subkey));

		// 将指定 key/val 对插入到负责的 S-node 中
		if(s_node) {
			s_node->set(StorageKey(domain, key, subkey, timestamp), v.value, overwrite);
		}
	} else {
		ERROR("No valid key given");

		// 没有给出待设置的 key，抛出异常
		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_KEY;
		ex.msg = "Necessary argument 'key' not given";
		throw ex;
	}
}

void CusHandler::Del(const string &domain, const Key &k, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<CusHandler::Del>");
	string key, subkey;
	int64_t timestamp;

	// 验证 domain 有效性
	if(!Validator::isValidDomain(domain)) {
		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_DOMAIN;
		ex.msg = "Invalid domain given to del";
		throw ex;
	}

	if(k.__isset.key) {
		key = k.key;

		// 验证 key 有效性
		if(!Validator::isValidKey(key)) {
			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_KEY;
			ex.msg = "Invalid key given to del";
			throw ex;
		}

		if(k.__isset.subkey) {
			subkey = k.subkey;

			// 验证 subkey 有效性
			if(!Validator::isValidSubkey(subkey)) {
				InvalidOperation ex;
				ex.ec = Error::BHT_EC_INVALID_SUBKEY;
				ex.msg = "Invalid subkey given to del";
				throw ex;
			}
		}

		if(k.__isset.timestamp) {
			timestamp = k.timestamp;
		} else {
			DEBUG("Timestamp not set in the given key, using current epoch instead");
			// 删除操作未指定时戳，默认为当前 us 级 UNIX 时戳
			BHT_SET_CUR_EPOCH(timestamp);
		}

		Config &conf = Config::getInstance();	// throws(InvalidOperation)

		// 若当前操作允许 relay，且 domain 在同步列表中，则使用 relay 服务将数据同步到远端
		if(relay && conf.isRelayDomain(domain)) {
			Key nk = k;

			nk.__isset.timestamp = true;
			nk.timestamp = timestamp;

			RelayClient &relay = RelayClient::getInstance();
			relay.onDel(domain, nk);
		}

		PartitionKey pkey = PartitionKey(domain, key);
		shared_ptr<Node> s_node;
		
		// 尝试在 L-ring 中查找指定 key 对应的 S-node，若未找到则直接在 S-ring 中查找
		// XXX: 这里除了扩容时迁移数据之外不再从 L-ring 中主动删除任何记录，以简化处理逻辑。
		s_node = locateNode(pkey);
		if(!s_node) {
			shared_ptr<Ring> s_ring = conf.getStorageRing();
			s_node = s_ring->getNode(PartitionKey(domain, key));
		}

		if(!s_node) {
			return;
		}

		// 向负责指定 key 的 S-node 插入一条模拟删除记录
		s_node->out(StorageKey(domain, key, subkey, timestamp), overwrite);

		// 从 cache 中删除指定 key 对应的缓存记录
		Cache &cache = Cache::getInstance();
		cache.del(StorageKey(domain, key, subkey));
	} else {
		// 没有给出待删除的 key，抛出异常
		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_KEY;
		ex.msg = "Necessary argument 'key' not given";
		throw ex;
	}
}

void CusHandler::Get(Val &v, const string &domain, const Key &k)
{
	ScopedLoggingCtx logging_ctx("<CusHandler::Get>");
	string key, subkey, val;

	// 验证 domain 有效性
	if(!Validator::isValidDomain(domain)) {
		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_DOMAIN;
		ex.msg = "Invalid domain given to get";
		throw ex;
	}

	if(k.__isset.key) {
		key = k.key;

		// 验证 key 有效性
		if(!Validator::isValidKey(key)) {
			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_KEY;
			ex.msg = "Invalid key given to get";
			throw ex;
		}
	} else {
		// 没有给出待查询的 key，抛出异常
		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_KEY;
		ex.msg = "Necessary argument 'key' not given";
		throw ex;
	}

	if(k.__isset.subkey) {
		subkey = k.subkey;

		// 验证 subkey 有效性
		if(!Validator::isValidSubkey(subkey)) {
			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_SUBKEY;
			ex.msg = "Invalid subkey given to get";
			throw ex;
		}
	}

	// 从 cache 中查找指定 key
	Cache &cache = Cache::getInstance();
	if(cache.get(StorageKey(domain, key, subkey), &val)) {
		// 在 cache 中找到了指定 key 对应记录
		v.__isset.value = true;
		v.value = val;
		return;
	}

	Config &conf=Config::getInstance();	// throws(InvalidOperation)

	v.__isset.value = false;

	// 首先直接尝试从 S-ring 中查找指定 key
	shared_ptr<Ring> s_ring=conf.getStorageRing();
	shared_ptr<Node> s_node=s_ring->getNode(PartitionKey(domain, key));

	if(!s_node) {
		return;
	}

	if(s_node->get(StorageKey(domain, key, subkey), &val)) {	// throws(InvalidOperation)
		// 在 S-ring 中找到了指定 key，查找成功
		v.__isset.value = true;
		v.value = val;
	} else {
		// 在 S-ring 中未能找到指定 key，尝试从 L-ring 中定位指定 key 对应的 S-node
		s_node = locateNode(PartitionKey(domain, key));
		if(s_node) {
			// 在 L-ring 中找到了负责指定 key 的 S-node
			if(s_node->get(StorageKey(domain, key, subkey), &val)) {
				v.__isset.value = true;
				v.value = val;
			}
		}
	}

	// 若获取指定 key 的值成功则将其设置到 cache 中
	if(v.__isset.value) {
		cache.set(StorageKey(domain, key, subkey), v.value, Cache::CACHE_TIMEOUT);
	}
}

void CusHandler::UpdateConfig()
{
	ScopedLoggingCtx logging_ctx("<CusHandler::UpdateConfig>");
	Config &conf = Config::getInstance();
	conf.updateConfig();
}

void CusHandler::cleanupRing(const PartitionKey &pkey, const string &s_id)
{
	ScopedLoggingCtx logging_ctx("<CusHandler::cleanupRing>");
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

shared_ptr<Node> CusHandler::locateNode(const PartitionKey &pkey)
{
	ScopedLoggingCtx logging_ctx("<CusHandler::locateNode>");
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

shared_ptr<Node> CusHandler::locateNodeOrInit(const PartitionKey &pkey)
{
	ScopedLoggingCtx logging_ctx("<CusHandler::locateNodeOrInit>");

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

