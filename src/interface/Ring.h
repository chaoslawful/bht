#ifndef BHT_RING_H__
#define BHT_RING_H__

#include "Common.h"
#include "Node.h"

BHT_CODE_BEGIN

class Ring;

/// ring 迭代器类 (迭代器实例不能跨线程使用！)
class RingIterator {
public:
	RingIterator(Ring *r, int idx)
		:_ring(r), _idx(idx), _cnt(0) {}
	RingIterator(const RingIterator &it):_ring(it._ring), _idx(it._idx), _cnt(it._cnt) {}
	RingIterator operator = (const RingIterator &it)
	{
		_ring = it._ring;
		_idx = it._idx;
		_cnt = it._cnt;
		return *this;
	}

	void operator ++ ();							//< (读锁)
	::boost::shared_ptr<Node> operator * ();		//< (读锁)
	operator bool ();								//< (读锁)

private:
	Ring *_ring;
	int _idx;
	int _cnt;
};

class Ring {
public:
	typedef ::std::vector< ::boost::shared_ptr<Node> > RingType;
	typedef RingIterator iterator;
	friend class RingIterator;

	Ring();
	~Ring();

	/**
	 * 获取 id 为给定值的 node 对象。若给定 id 没有对应的 node 则返回 NULL (读锁)
	 * */
	::boost::shared_ptr<Node> getNodeById(uint32_t id);

	/**
	 * 遍历 ring 查找负责给定 key 的 node 对象(读锁)
	 * */
	::boost::shared_ptr<Node> getNode(const InternalKey &k);
	::boost::shared_ptr<Node> getNode(const ::std::string &s);

	/**
	 * 获取一个遍历 ring 的迭代器，起始位置为负责给定 key 的 node (读锁)
	 * */
	Ring::iterator getIterator(const InternalKey &k);

	/**
	 * 清空 ring 中的所有 node 实例(读锁)
	 * */
	void clear();

	/**
	 * 向 ring 中添加一个新的 node 实例(写锁)
	 * */
	void addNode(::boost::shared_ptr<Node> node);

private:
	// 计算给定字符串的散列值
	static uint32_t hash(const ::std::string &s);

	/// 遍历 ring 查找负责给定字符串的 node (无锁)
	int getNodeIdx(const ::std::string &s);

	::apache::thrift::concurrency::ReadWriteMutex _nodes_rwlock;
	RingType _nodes;
};

BHT_CODE_END

#endif

