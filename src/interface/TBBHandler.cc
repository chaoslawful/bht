#include <tbb/task_scheduler_init.h>
#include <tbb/parallel_reduce.h>
#include <tbb/blocked_range.h>
#include <tbb/tick_count.h>

#include "TBBHandler.h"
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
using namespace ::tbb;

BHT_INIT_LOGGING("BHT.Interface.TBBHandler")

TBBHandler::TBBHandler()
{
}

TBBHandler::~TBBHandler()
{
}

// 多键删除用任务定义
class DelReducer {
	TBBHandler *h;	// BHT TBBHandler 实例指针
	const string &domain;	// 待删除的 key 所在 domain
	const vector<Key> &sks;	// 待删除的原始 key 列表
	int64_t ts;	// 操作起始时戳，用于重整未指定时戳的 key
	bool relay;	// 操作中继标志
	bool overwrite;	// 覆盖删除标志
public:
	list<Key> nks;	// 重整时戳后的 key 列表，需对外公开以便使用

	DelReducer(TBBHandler *handler,
			const int64_t &timestamp,
			const string &dmn,
			const vector<Key> &ks,
			const bool &owflag,
			const bool &rflag)
		:h(handler), domain(dmn), sks(ks), nks()
	{
		ts = timestamp;
		overwrite = owflag;
		relay = rflag;
	}

	// 分解任务构造函数
	DelReducer(const DelReducer &o, split)
		:h(o.h), domain(o.domain), sks(o.sks), nks()
	{
		ts = o.ts;
		overwrite = o.overwrite;
		relay = o.relay;
	}

	// 合并子任务中重整时戳后的 key 列表到当前列表中
	void join(const DelReducer &o)
	{
		nks.insert(nks.end(), o.nks.begin(), o.nks.end());
	}

	// 删除任务操作函数
	void operator() (const blocked_range<size_t> &r)
	{
		for(size_t i = r.begin(); i != r.end(); ++i) {
			Key nk = sks[i];

			if(!nk.__isset.timestamp) {
				// 当前 key 未指定时戳，默认为当前 us 级 UNIX 时戳
				nk.timestamp = ts;
			}

			// 若操作需要 relay 则保存带有时戳的 key
			if(relay) {
				nks.push_back(nk);
			}

			h->Del(domain, nk, overwrite, false);
		}
	}
};

void TBBHandler::MDel(const string &domain, const vector<Key> &ks, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<TBBHandler::MDel>");
	int64_t timestamp;
	bool relay_flag = false;	// 操作是否需要 relay
	vector<Key> nks;

	BHT_SET_CUR_EPOCH(timestamp);

	Config &conf = Config::getInstance();	// throws(InvalidOperation)

	// 若当前操作允许 relay，且 domain 在同步列表中，则使用 relay 服务将数据同步到远端
	relay_flag = relay && conf.isRelayDomain(domain);

	tick_count t0 = tick_count::now();

	// 并发删除指定的各个 key，单独的操作不进行 relay
	DelReducer reducer(this, timestamp, domain, ks, overwrite, relay_flag);
	parallel_reduce(blocked_range<size_t>(0, ks.size()), reducer, auto_partitioner());

	tick_count t1 = tick_count::now();

	DEBUG("Done parallel del "
			+ lexical_cast<string>(ks.size())
			+ " records in "
			+ lexical_cast<string>((t1-t0).seconds())
			+ " secs");

	// 所有操作均成功时进行一次 relay
	if(relay_flag) {
		copy(reducer.nks.begin(), reducer.nks.end(), back_inserter(nks));

		RelayClient &relay = RelayClient::getInstance();
		relay.onMDel(domain, nks);
	}
}

// 多键设置用任务定义
class SetReducer {
	TBBHandler *h;	// BHT TBBHandler 实例指针
	const string &domain;	// 待设置的 key 所在 domain
	const vector< map<Key, Val>::const_iterator > &skvs;	// 待设置的原始 key-val 迭代器列表
	int64_t ts;	// 操作起始时戳，用于重整未指定时戳的 key
	bool relay;	// 操作中继标志
	bool overwrite;	// 覆盖删除标志
public:
	map<Key, Val> nkvs;	// 重整时戳后的 key-val 列表，需对外公开以便使用

	SetReducer(TBBHandler *handler,
			const int64_t &timestamp,
			const string &dmn,
			const vector< map<Key, Val>::const_iterator > &kvs,
			const bool &owflag,
			const bool &rflag)
		:h(handler), domain(dmn), skvs(kvs), nkvs()
	{
		ts = timestamp;
		overwrite = owflag;
		relay = rflag;
	}

	// 分解任务构造函数
	SetReducer(const SetReducer &o, split)
		:h(o.h), domain(o.domain), skvs(o.skvs), nkvs()
	{
		ts = o.ts;
		overwrite = o.overwrite;
		relay = o.relay;
	}

	// 合并子任务中重整时戳后的 key-val 列表到当前列表中
	void join(const SetReducer &o)
	{
		nkvs.insert(o.nkvs.begin(), o.nkvs.end());
	}

	// 设置任务操作函数
	void operator() (const blocked_range<size_t> &r)
	{
		for(size_t i = r.begin(); i != r.end(); ++i) {
			map<Key, Val>::const_iterator it = skvs[i];
			Key nk = it->first;

			if(!nk.__isset.timestamp) {
				// 当前 key 未指定时戳，默认为当前 us 级 UNIX 时戳
				nk.timestamp = ts;
			}

			// 若操作需要 relay 则保存带有时戳的 key
			if(relay) {
				nkvs[nk] = it->second;
			}

			h->Set(domain, nk, it->second, overwrite, false);
		}
	}
};

void TBBHandler::MSet(const string &domain, const map<Key, Val> &kvs, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<TBBHandler::MSet>");
	int64_t timestamp;
	bool relay_flag = false;	// 操作是否需要 relay
	map<Key, Val> nkvs;

	BHT_SET_CUR_EPOCH(timestamp);

	Config &conf = Config::getInstance();	// throws(InvalidOperation)

	// 若当前操作允许 relay，且 domain 在同步列表中，则使用 relay 服务将数据同步到远端
	relay_flag = relay && conf.isRelayDomain(domain);

	// 创建便于任务切分的所有 key-val 对迭代器列表
	vector< map<Key, Val>::const_iterator > ikvs;
	for(map<Key, Val>::const_iterator it = kvs.begin(); it != kvs.end(); ++it) {
		ikvs.push_back(it);
	}

	tick_count t0 = tick_count::now();

	// 并发设置指定的各个 key-val 对，单独的操作不进行 relay
	SetReducer reducer(this, timestamp, domain, ikvs, overwrite, relay_flag);
	parallel_reduce(blocked_range<size_t>(0, ikvs.size()), reducer, auto_partitioner());

	tick_count t1 = tick_count::now();

	DEBUG("Done parallel set "
			+ lexical_cast<string>(kvs.size())
			+ " records in "
			+ lexical_cast<string>((t1-t0).seconds())
			+ " secs");

	// 所有操作均成功时进行一次 relay
	if(relay_flag) {
		RelayClient &relay = RelayClient::getInstance();
		relay.onMSet(domain, reducer.nkvs);
	}
}

// 多键查询用任务定义
class GetReducer {
	TBBHandler *h;	// BHT TBBHandler 实例指针
	const string &domain;	// 待删除的 key 所在 domain
	const vector<Key> &sks;	// 待删除的原始 key 列表
public:
	map<Key, Val> nkvs;	// 获取成功的 key-val 映射表，需对外公开以便使用

	GetReducer(TBBHandler *handler, const string &dmn, const vector<Key> &ks)
		:h(handler), domain(dmn), sks(ks), nkvs()
	{
	}

	// 分解任务构造函数
	GetReducer(const GetReducer &o, split)
		:h(o.h), domain(o.domain), sks(o.sks), nkvs()
	{
	}

	// 合并子任务中的 key-val 映射表到当前映射表中
	void join(const GetReducer &o)
	{
		nkvs.insert(o.nkvs.begin(), o.nkvs.end());
	}

	// 查询任务操作函数
	void operator() (const blocked_range<size_t> &r)
	{
		for(size_t i = r.begin(); i != r.end(); ++i) {
			Val v;
			h->Get(v, domain, sks[i]);
			if(v.__isset.value) {
				// 找到了当前 key 对应的 val
				nkvs[sks[i]] = v;
			}
		}
	}
};

void TBBHandler::MGet(map<Key, Val> &kvs, const string &domain, const vector<Key> &ks)
{
	ScopedLoggingCtx logging_ctx("<TBBHandler::MGet>");

	tick_count t0 = tick_count::now();

	// 并发获取指定的各个 key 对应的 val
	GetReducer reducer(this, domain, ks);
	parallel_reduce(blocked_range<size_t>(0, ks.size()), reducer, auto_partitioner());

	tick_count t1 = tick_count::now();

	DEBUG("Done parallel get "
			+ lexical_cast<string>(ks.size())
			+ " records in "
			+ lexical_cast<string>((t1-t0).seconds())
			+ " secs");

	// 复制查询结果
	kvs.insert(reducer.nkvs.begin(), reducer.nkvs.end());
}

void TBBHandler::Set(const string &domain, const Key &k, const Val &v, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<TBBHandler::Set>");
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

	// 若没有给出待设置的 key 则抛出异常
	if(!k.__isset.key) {
		ERROR("No valid key given");

		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_KEY;
		ex.msg = "Necessary argument 'key' not given";
		throw ex;
	}

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

	// 操作正常完成，若当前操作允许 relay，且 domain 在同步列表中，则使用 relay 服务将数据同步到远端
	if(relay && conf.isRelayDomain(domain)) {
		Key nk = k;
		Val nv = v;

		nk.__isset.timestamp = true;
		nk.timestamp = timestamp;

		RelayClient &relay = RelayClient::getInstance();
		relay.onSet(domain, nk, nv);
	}
}

void TBBHandler::Del(const string &domain, const Key &k, const bool overwrite, const bool relay)
{
	ScopedLoggingCtx logging_ctx("<TBBHandler::Del>");
	string key, subkey;
	int64_t timestamp;

	// 验证 domain 有效性
	if(!Validator::isValidDomain(domain)) {
		ERROR("Invalid domain: '" + domain + "'");

		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_DOMAIN;
		ex.msg = "Invalid domain given to del";
		throw ex;
	}

	// 若没有给出待删除的 key 则抛出异常
	if(!k.__isset.key) {
		ERROR("No valid key given");

		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_KEY;
		ex.msg = "Necessary argument 'key' not given";
		throw ex;
	}

	key = k.key;

	// 验证 key 有效性
	if(!Validator::isValidKey(key)) {
		ERROR("Invalid key: '" + key + "'");

		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_KEY;
		ex.msg = "Invalid key given to del";
		throw ex;
	}

	if(k.__isset.subkey) {
		subkey = k.subkey;

		// 验证 subkey 有效性
		if(!Validator::isValidSubkey(subkey)) {
			ERROR("Invalid subkey: '" + subkey + "'");

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
		// 未能获取负责指定 key 的 S-Node，不进行任何操作
		return;
	}

	// 向负责指定 key 的 S-node 插入一条模拟删除记录
	s_node->out(StorageKey(domain, key, subkey, timestamp), overwrite);

	// 从 cache 中删除指定 key 对应的缓存记录
	Cache &cache = Cache::getInstance();
	cache.del(StorageKey(domain, key, subkey));

	// 操作正常完成，若当前操作允许 relay，且 domain 在同步列表中，则使用 relay 服务将数据同步到远端
	if(relay && conf.isRelayDomain(domain)) {
		Key nk = k;

		nk.__isset.timestamp = true;
		nk.timestamp = timestamp;

		RelayClient &relay = RelayClient::getInstance();
		relay.onDel(domain, nk);
	}
}

void TBBHandler::Get(Val &v, const string &domain, const Key &k)
{
	ScopedLoggingCtx logging_ctx("<TBBHandler::Get>");
	string key, subkey, val;

	// 验证 domain 有效性
	if(!Validator::isValidDomain(domain)) {
		ERROR("Invalid domain: '" + domain + "'");

		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_DOMAIN;
		ex.msg = "Invalid domain given to get";
		throw ex;
	}

	// 若没有给出待查询的 key 则抛出异常
	if(!k.__isset.key) {
		ERROR("No valid key given");

		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_KEY;
		ex.msg = "Necessary argument 'key' not given";
		throw ex;
	}

	key = k.key;

	// 验证 key 有效性
	if(!Validator::isValidKey(key)) {
		ERROR("Invalid key: '" + key + "'");

		InvalidOperation ex;
		ex.ec = Error::BHT_EC_INVALID_KEY;
		ex.msg = "Invalid key given to get";
		throw ex;
	}

	if(k.__isset.subkey) {
		subkey = k.subkey;

		// 验证 subkey 有效性
		if(!Validator::isValidSubkey(subkey)) {
			ERROR("Invalid subkey: '" + subkey + "'");

			InvalidOperation ex;
			ex.ec = Error::BHT_EC_INVALID_SUBKEY;
			ex.msg = "Invalid subkey given to get";
			throw ex;
		}
	}

	// 从 cache 中查找指定 key
	Cache &cache = Cache::getInstance();

	if(cache.get(StorageKey(domain, key, subkey), &val)) {
		// 在 cache 中找到了指定 key 对应记录则返回之
		v.__isset.value = true;
		v.value = val;
		return;
	}

	// 在 cache 中未找到指定 key 对应记录
	Config &conf=Config::getInstance();	// throws(InvalidOperation)

	v.__isset.value = false;

	// 首先直接尝试从 S-ring 中查找指定 key
	shared_ptr<Ring> s_ring=conf.getStorageRing();
	shared_ptr<Node> s_node=s_ring->getNode(PartitionKey(domain, key));

	if(!s_node) {
		// 未能获取负责指定 key 的 S-Node，不进行任何操作
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

void TBBHandler::UpdateConfig()
{
	ScopedLoggingCtx logging_ctx("<TBBHandler::UpdateConfig>");
	Config &conf = Config::getInstance();
	conf.updateConfig();
}

void TBBHandler::cleanupRing(const PartitionKey &pkey, const string &s_id)
{
	ScopedLoggingCtx logging_ctx("<TBBHandler::cleanupRing>");
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

shared_ptr<Node> TBBHandler::locateNode(const PartitionKey &pkey)
{
	ScopedLoggingCtx logging_ctx("<TBBHandler::locateNode>");
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

shared_ptr<Node> TBBHandler::locateNodeOrInit(const PartitionKey &pkey)
{
	ScopedLoggingCtx logging_ctx("<TBBHandler::locateNodeOrInit>");

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

