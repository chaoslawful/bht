#include "Ring.h"
#include "Logging.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.Ring")

Ring::Ring()
{
}

Ring::~Ring()
{
}

shared_ptr<Node> Ring::getNodeById(uint32_t id)
{
	ScopedLoggingCtx logging_ctx("<Ring::getNodeById>");
	RWGuard guard(_nodes_rwlock);

	int lo = 0, hi = _nodes.size() - 1;
	int mid;

	while(hi >= lo) {
		mid = (lo + hi) >> 1;

		if(_nodes[mid]->id() == id) {
			return _nodes[mid];
		} else if(_nodes[mid]->id() < id) {
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}

	return shared_ptr<Node>((Node*)NULL);
}

shared_ptr<Node> Ring::getNode(const InternalKey &k)
{
	return getNode(k.toString());
}

shared_ptr<Node> Ring::getNode(const string &s)
{
	ScopedLoggingCtx logging_ctx("<Ring::getNode>");
	RWGuard guard(_nodes_rwlock);

	int idx = getNodeIdx(s);
	if(idx == -1) {
		return shared_ptr<Node>((Node*)NULL);
	}

	return _nodes[idx];
}

Ring::iterator Ring::getIterator(const InternalKey &k)
{
	ScopedLoggingCtx logging_ctx("<Ring::getIterator>");
	RWGuard guard(_nodes_rwlock);

	int idx = getNodeIdx(k.toString());
	if(idx == -1) {
		return RingIterator(this, 0);
	}

	return RingIterator(this, idx);
}

void Ring::clear()
{
	ScopedLoggingCtx logging_ctx("<Ring::clear>");
	RWGuard guard(_nodes_rwlock, true);

	_nodes.clear();
}

void Ring::addNode(shared_ptr<Node> node)
{
	ScopedLoggingCtx logging_ctx("<Ring::addNode>");
	RWGuard guard(_nodes_rwlock, true);

	if(_nodes.size() == 0) {
		_nodes.push_back(node);
		return;
	}

	int max = _nodes.size() - 1;
	int lo = 0, hi = max;
	int mid;
	uint32_t id = node->id();

	// 以对分法有序插入新 node
	while(hi >= lo) {
		mid = (lo + hi) >> 1;

		if(id == _nodes[mid]->id()) {
			// 给定 ID 的 node 已经存在，忽略添加行为
			WARN("Given node existed in the ring: id = " + lexical_cast<string>(id));
			return;
		} else if(id < _nodes[mid]->id()) {
			if(mid == 0) {
				// 将给定 node 插入到起始位置之前
				_nodes.insert(_nodes.begin(), node);
				return;
			} else if(_nodes[mid - 1]->id() < id) {
				// 将给定 node 插入到当前位置之前
				_nodes.insert(_nodes.begin() + mid, node);
				return;
			} else {
				hi = mid - 1;
			}
		} else {
			if(mid == max) {
				// 到达结束位置，将给定 node 附加到结束位置上
				_nodes.push_back(node);
				return;
			} else {
				lo = mid + 1;
			}
		}
	}

	ERROR("Should not reach here! Cannot find appropriate location to insert new node: lo = "
			+ lexical_cast<string>(lo) + ", hi = " + lexical_cast<string>(hi));
}

uint32_t Ring::hash(const string &s)
{
	return (hash_value(s) & 0xffffffffULL);
}

int Ring::getNodeIdx(const string &s)
{
	ScopedLoggingCtx logging_ctx("<Ring::getNodeIdx>");
	uint32_t id = Ring::hash(s);

	if(_nodes.size() == 0) {
		return -1;
	}

	int max = _nodes.size() - 1;
	int lo = 0, hi = max;
	int mid;
	int cand;

	while(1) {
		mid = (lo + hi) >> 1;
		
		if(id <= _nodes[mid]->id()) {
			if(mid == 0) {
				// 当前 node 已经是首 node，因此负责给定 key 的就是当前 node
				cand = 0;
				break;
			} else if(_nodes[mid - 1]->id() < id) {
				// 给定 key 的散列值落在当前 node 的负责区间，因此负责给定 key 的就是当前 node
				cand = mid;
				break;
			} else {
				hi = mid - 1;
			}
		} else {
			if(mid == max) {
				// 当前 node 已经是末 node，但给定 key 的散列值仍大于当前 node ID，因此负责给定 key 的就是首 node
				cand = 0;
				break;
			} else {
				lo = mid + 1;
			}
		}
	}

	return cand;
}

void RingIterator::operator ++ ()
{
	RWGuard guard(_ring->_nodes_rwlock);

	if(_cnt < (int)(_ring->_nodes.size())) {
		++_idx;
		if(_idx >= (int)(_ring->_nodes.size())) {
			_idx = 0;
		}
		++_cnt;
	}
}

shared_ptr<Node> RingIterator::operator * ()
{
	RWGuard guard(_ring->_nodes_rwlock);

	if(_idx < (int)(_ring->_nodes.size())) {
		return _ring->_nodes[_idx];
	}
	return shared_ptr<Node>((Node*)NULL);
}

RingIterator::operator bool ()
{
	RWGuard guard(_ring->_nodes_rwlock);

	int len = _ring->_nodes.size();
	if(_idx >= 0 && _idx < len && _cnt < len) {
		return true;
	}

	return false;
}

BHT_CODE_END

