/*
 * =====================================================================================
 *
 *       Filename:  PoolMgr.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/27/2009 02:40:39 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_POOL_MGR_H
#define BHT_RELAY_POOL_MGR_H

#include "PoolMgrBase.h"
#include <map>
#include <boost/tuple/tuple.hpp>
#include <boost/tuple/tuple_comparison.hpp>
#include <boost/preprocessor/punctuation/paren.hpp>

BHT_RELAY_BEGIN

#if POOL_MAX_ARG_NUM > 0

template <class T, 
	class BaseT = T
	BOOST_PP_COMMA_IF(POOL_MAX_ARG_NUM)
	BOOST_PP_ENUM_BINARY_PARAMS(POOL_MAX_ARG_NUM, class A, = PoolNoneArg BOOST_PP_INTERCEPT)
>
struct PoolMgr : public PoolMgrBase<BaseT, BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, A)>
{
	typedef PoolMgrBase<BaseT, BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, A)> ThisBaseT;
	typedef boost::shared_ptr<PoolMgr> Ptr;
	typedef Pool<T, BaseT
		BOOST_PP_COMMA_IF(POOL_MAX_ARG_NUM)
		BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, A)
	> PoolT;
	typedef typename PoolT::ElemT ElemT;
	typedef boost::tuple<
		BOOST_PP_ENUM_BINARY_PARAMS(POOL_MAX_ARG_NUM, typename boost::remove_const<typename boost::remove_reference<A, >::type>::type BOOST_PP_INTERCEPT)
	> KeyT;
	typedef boost::tuple<
		BOOST_PP_ENUM_BINARY_PARAMS(POOL_MAX_ARG_NUM, typename boost::add_const<typename boost::add_reference<A, >::type>::type BOOST_PP_INTERCEPT)
	> ConstRefKeyT;
	typedef std::map<KeyT, typename PoolT::Ptr> PoolContT;
public:
	explicit PoolMgr(uint32_t initialize = 0): initialize_(initialize) {}
	typename ThisBaseT::PoolT::Ptr MakePool(BOOST_PP_ENUM_BINARY_PARAMS(POOL_MAX_ARG_NUM, A, a)) {
		ConstRefKeyT key(BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, a));
		apache::thrift::concurrency::RWGuard wlock(rwmutex_, true);
		typename PoolContT::iterator it = pools_.find(key);
		if (it == pools_.end()) {
			typename PoolT::Ptr pool(new PoolT(BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, a), initialize_));
			it = pools_.insert(std::make_pair(key, pool)).first;
		}
		return it->second;
	}
	typename ThisBaseT::PoolT::Ptr GetPool(BOOST_PP_ENUM_BINARY_PARAMS(POOL_MAX_ARG_NUM, A, a)) {
		ConstRefKeyT key(BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, a));
		apache::thrift::concurrency::RWGuard rlock(rwmutex_);
		typename PoolContT::iterator it = pools_.find(key);
		if (it != pools_.end()) {
			return it->second;
		} else {
			return typename PoolT::Ptr();
		}
	}
private:
	PoolContT pools_;
	apache::thrift::concurrency::ReadWriteMutex rwmutex_;
	uint32_t initialize_;
};

#define POOL_GET_3RD(unused1, unused2, x) x
#define BOOST_PP_LOCAL_LIMITS (1, POOL_MAX_ARG_NUM - 1)
#define BOOST_PP_LOCAL_MACRO(n) \
template <class T, \
	class BaseT \
	BOOST_PP_COMMA_IF(n) \
	BOOST_PP_ENUM_PARAMS(n, class A) \
> \
struct PoolMgr < \
	T,  \
	BaseT \
	BOOST_PP_COMMA_IF(n) \
	BOOST_PP_ENUM_PARAMS(n, A) \
	BOOST_PP_COMMA_IF(BOOST_PP_SUB(POOL_MAX_ARG_NUM, n)) \
	BOOST_PP_ENUM(BOOST_PP_SUB(POOL_MAX_ARG_NUM, n), POOL_GET_3RD, PoolNoneArg) \
> : public PoolMgrBase<BaseT, BOOST_PP_ENUM_PARAMS(n, A)> \
{ \
	typedef PoolMgrBase<BaseT, BOOST_PP_ENUM_PARAMS(n, A)> ThisBaseT; \
	typedef boost::shared_ptr<PoolMgr> Ptr; \
	typedef Pool<T, BaseT \
		BOOST_PP_COMMA_IF(n) \
		BOOST_PP_ENUM_PARAMS(n, A) \
	> PoolT; \
	typedef typename PoolT::ElemT ElemT; \
	typedef boost::tuple< \
		BOOST_PP_ENUM_BINARY_PARAMS(n, typename boost::remove_const<typename boost::remove_reference<A, >::type>::type BOOST_PP_INTERCEPT) \
	> KeyT; \
	typedef boost::tuple< \
		BOOST_PP_ENUM_BINARY_PARAMS(n, typename boost::add_const<typename boost::add_reference<A, >::type>::type BOOST_PP_INTERCEPT) \
	> ConstRefKeyT; \
	typedef std::map<KeyT, typename PoolT::Ptr> PoolContT; \
public: \
	explicit PoolMgr(uint32_t initialize = 0): initialize_(initialize) {} \
	typename ThisBaseT::PoolT::Ptr MakePool(BOOST_PP_ENUM_BINARY_PARAMS(n, A, a)) { \
		ConstRefKeyT key(BOOST_PP_ENUM_PARAMS(n, a)); \
		apache::thrift::concurrency::RWGuard wlock(rwmutex_, true); \
		typename PoolContT::iterator it = pools_.find(key); \
		if (it == pools_.end()) { \
			typename PoolT::Ptr pool(new PoolT(BOOST_PP_ENUM_PARAMS(n, a), initialize_)); \
			it = pools_.insert(std::make_pair(key, pool)).first; \
		} \
		return it->second; \
	} \
	typename ThisBaseT::PoolT::Ptr GetPool(BOOST_PP_ENUM_BINARY_PARAMS(n, A, a)) { \
		ConstRefKeyT key(BOOST_PP_ENUM_PARAMS(n, a)); \
		apache::thrift::concurrency::RWGuard rlock(rwmutex_); \
		typename PoolContT::iterator it = pools_.find(key); \
		if (it != pools_.end()) { \
			return it->second; \
		} else { \
			return typename PoolT::Ptr(); \
		} \
	} \
private: \
	PoolContT pools_; \
	apache::thrift::concurrency::ReadWriteMutex rwmutex_; \
	uint32_t initialize_; \
};
#include BOOST_PP_LOCAL_ITERATE()
#undef POOL_GET_3RD

#endif // POOL_MAX_ARG_NUM > 0

BHT_RELAY_END

#endif // BHT_RELAY_POOL_MGR_H

