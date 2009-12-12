/*
 * =====================================================================================
 *
 *       Filename:  PoolBase.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/26/2009 07:39:29 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_POOL_BASE_H
#define BHT_RELAY_POOL_BASE_H

#include "Common.h"
#include <queue>
#include <boost/shared_ptr.hpp>
#include <boost/assert.hpp>
#include <thrift/concurrency/Mutex.h>

BHT_RELAY_BEGIN

template <class T>
struct PoolBase
{
	typedef boost::shared_ptr<T> ElemT;
	typedef boost::shared_ptr<PoolBase> Ptr;
protected:
	typedef std::queue<ElemT> PoolType;
public:
	virtual ~PoolBase() {}
	ElemT Alloc() {
		{
			apache::thrift::concurrency::RWGuard wlock(rwmutex_, true);
			if (!pool_.empty()) {
				ElemT s = pool_.front();
				pool_.pop();
				return s;
			}
		}
		return DoAlloc();
	}
	void Free(ElemT e){
		BOOST_ASSERT(e);
		apache::thrift::concurrency::RWGuard rlock(rwmutex_);
		pool_.push(e);
	}
	void Clear() {
		apache::thrift::concurrency::RWGuard wlock(rwmutex_, true);
		pool_.clear();
	}
	size_t Size() const {
		apache::thrift::concurrency::RWGuard rlock(rwmutex_);
		return pool_.size();
	}
	bool Empty() const {
		apache::thrift::concurrency::RWGuard rlock(rwmutex_);
		return pool_.empty();
	}
protected:
	virtual ElemT DoAlloc() const = 0;
protected:
	PoolType pool_;
	apache::thrift::concurrency::ReadWriteMutex rwmutex_;
};

BHT_RELAY_END

#endif // BHT_RELAY_POOL_BASE_H

