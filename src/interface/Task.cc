/*
 * =====================================================================================
 *
 *       Filename:  Task.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/10/2009 03:38:11 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Task.h"
#include "Config.h"
#include "Cache.h"
#include "CusHandler.h"


BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

void TaskMGetFromCache::Run()
{
	map<InternalKey*, pair<bool, string> > kvs;
	for (map<StorageKey*, const Key*>::const_iterator it = _ks.begin(), end = _ks.end(); it != end; ++it) {
		kvs.insert(make_pair(static_cast<InternalKey*>(it->first), make_pair(false, string())));
	}
	_node->mget(&kvs);
	for (map<InternalKey*, pair<bool, string> >::iterator it = kvs.begin(), end = kvs.end(); it != end; ++it) {
		if (it->second.first) {
			map<StorageKey*, const Key*>::const_iterator pos = _ks.find(dynamic_cast<StorageKey*>(it->first));
			const Key& k = *(pos->second);
			Val v;
			v.__isset.value = true;
			v.value = it->second.second;

			Guard lock(_mutex);
			_kvs[k] = v;
			_all_ks.erase(dynamic_cast<StorageKey*>(it->first));
		}
	}
}


void TaskMGetFromSNode::Run()
{
	map<InternalKey*, pair<bool, string> > kvs;
	for (map<StorageKey*, const Key*>::const_iterator it = _ks.begin(), end = _ks.end(); it != end; ++it) {
		kvs.insert(make_pair(static_cast<InternalKey*>(it->first), make_pair(false, string())));
	}
	_node->mget(&kvs);
	for (map<InternalKey*, pair<bool, string> >::iterator it = kvs.begin(), end = kvs.end(); it != end; ++it) {
		if (it->second.first) {
			map<StorageKey*, const Key*>::const_iterator pos = _ks.find(dynamic_cast<StorageKey*>(it->first));
			const Key& k = *(pos->second);
			Val v;
			v.__isset.value = true;
			v.value = it->second.second;

			Cache& cache = Cache::getInstance();
			cache.set(*dynamic_cast<StorageKey*>(it->first), v.value, Cache::CACHE_TIMEOUT);

			Guard lock(_mutex);
			_kvs[k] = v;
			_all_ks.erase(dynamic_cast<StorageKey*>(it->first));
		}
	}
}

void TaskLocateNode::Run()
{
	shared_ptr<Node> s_node = CusHandler::locateNode(PartitionKey(_domain, _k->key));
	if (!s_node) {
		return;
	}
	::std::map<shared_ptr<Node>, ::std::map<StorageKey*, const Key*> >::iterator pos;
	Guard lock(_mutex);
	if ((pos = _node_map.find(s_node)) == _node_map.end()) {
		pos = _node_map.insert(make_pair(s_node, ::std::map<StorageKey*, const Key*>())).first;
	}
	pos->second.insert(make_pair(const_cast<StorageKey*>(_sk), _k));
}

void TaskSet::Run()
{
	PartitionKey pkey = PartitionKey(_domain, _k.key);
	shared_ptr<Node> s_node;

	// 在 L-ring 中查找负责指定 key 的 S-node，若未找到则从 S-ring 中挑选一个 S-node
	// 并将其同指定 key 的关联关系设置到 L-ring 中
	s_node = CusHandler::locateNodeOrInit(pkey);

	// 清除 cache 中缓存的指定 key 对应记录
	Cache &cache = Cache::getInstance();
	cache.del(StorageKey(_domain, _k.key, _k.subkey));

	// 将指定 key/val 对插入到负责的 S-node 中
	if(s_node) {
		s_node->set(StorageKey(_domain, _k.key, _k.subkey, _k.timestamp), _v.value, _overwrite);
	}
}

void TaskDel::Run()
{
	PartitionKey pkey = PartitionKey(_domain, _k.key);
	shared_ptr<Node> s_node;
	
	// 尝试在 L-ring 中查找指定 key 对应的 S-node，若未找到则直接在 S-ring 中查找
	// XXX: 这里除了扩容时迁移数据之外不再从 L-ring 中主动删除任何记录，以简化处理逻辑。
	Config &conf = Config::getInstance();	// throws(InvalidOperation)
	s_node = CusHandler::locateNode(pkey);
	if(!s_node) {
		shared_ptr<Ring> s_ring = conf.getStorageRing();
		s_node = s_ring->getNode(PartitionKey(_domain, _k.key));
	}

	if(!s_node) {
		return;
	}

	// 向负责指定 key 的 S-node 插入一条模拟删除记录
	s_node->out(StorageKey(_domain, _k.key, _k.subkey, _k.timestamp), _overwrite);

	// 从 cache 中删除指定 key 对应的缓存记录
	Cache &cache = Cache::getInstance();
	cache.del(StorageKey(_domain, _k.key, _k.subkey));
}

BHT_CODE_END

