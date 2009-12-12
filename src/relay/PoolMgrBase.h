/*
 * =====================================================================================
 *
 *       Filename:  PoolMgrBase.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/29/2009 10:45:01 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_REALY_POOL_MGR_BASE_H
#define BHT_REALY_POOL_MGR_BASE_H

#include "Pool.h"

BHT_RELAY_BEGIN

#if POOL_MAX_ARG_NUM > 0

template <class T
	BOOST_PP_COMMA_IF(POOL_MAX_ARG_NUM)
	BOOST_PP_ENUM_BINARY_PARAMS(POOL_MAX_ARG_NUM, class A, = PoolNoneArg BOOST_PP_INTERCEPT)
>
struct PoolMgrBase
{
	typedef boost::shared_ptr<PoolMgrBase> Ptr;
	typedef PoolBase<T> PoolT;
	typedef typename PoolT::ElemT ElemT;
	virtual typename PoolT::Ptr MakePool(BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, A)) = 0;
	virtual typename PoolT::Ptr GetPool(BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, A)) = 0;
	virtual ~PoolMgrBase() {}

	ElemT Alloc(BOOST_PP_ENUM_BINARY_PARAMS(POOL_MAX_ARG_NUM, A, a)) {
		typename PoolT::Ptr pool = GetPool(BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, a));
		if (!pool) {
			pool = MakePool(BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, a));
		}
		return pool->Alloc();
	}
	void Free(ElemT e) {
		typename PoolT::Ptr pool = GetPool(BOOST_PP_ENUM_BINARY_PARAMS(POOL_MAX_ARG_NUM, e->a, () BOOST_PP_INTERCEPT));
		if (pool) {
			return pool->Free(e);
		}
	}
};

#define POOL_GET_3RD(unused1, unused2, x) x
#define BOOST_PP_LOCAL_LIMITS (1, POOL_MAX_ARG_NUM - 1)
#define BOOST_PP_LOCAL_MACRO(n) \
template <class T \
	BOOST_PP_COMMA_IF(n) \
	BOOST_PP_ENUM_PARAMS(n, class A) \
> \
struct PoolMgrBase < \
	T \
	BOOST_PP_COMMA_IF(n) \
	BOOST_PP_ENUM_PARAMS(n, A) \
	BOOST_PP_COMMA_IF(BOOST_PP_SUB(POOL_MAX_ARG_NUM, n)) \
	BOOST_PP_ENUM(BOOST_PP_SUB(POOL_MAX_ARG_NUM, n), POOL_GET_3RD, PoolNoneArg) \
> \
{ \
	typedef boost::shared_ptr<PoolMgrBase> Ptr; \
	typedef PoolBase<T> PoolT; \
	typedef typename PoolT::ElemT ElemT; \
	virtual typename PoolT::Ptr MakePool(BOOST_PP_ENUM_PARAMS(n, A)) = 0; \
	virtual typename PoolT::Ptr GetPool(BOOST_PP_ENUM_PARAMS(n, A)) = 0; \
	virtual ~PoolMgrBase() {} \
\
	ElemT Alloc(BOOST_PP_ENUM_BINARY_PARAMS(n, A, a)) { \
		typename PoolT::Ptr pool = GetPool(BOOST_PP_ENUM_PARAMS(n, a)); \
		if (!pool) { \
			pool = MakePool(BOOST_PP_ENUM_PARAMS(n, a)); \
		} \
		return pool->Alloc(); \
	} \
	void Free(ElemT e, BOOST_PP_ENUM_BINARY_PARAMS(n, A, a)) { \
		typename PoolT::Ptr pool = GetPool(BOOST_PP_ENUM_PARAMS(n, a)); \
		if (pool) { \
			return pool->Free(e); \
		} \
	} \
};
#include BOOST_PP_LOCAL_ITERATE()
#undef POOL_GET_3RD


#endif // POOL_MAX_ARG_NUM > 0

BHT_RELAY_END

#endif // BHT_REALY_POOL_MGR_BASE_H

