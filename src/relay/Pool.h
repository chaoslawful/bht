/*
 * =====================================================================================
 *
 *       Filename:  Pool.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/27/2009 02:29:40 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_POOL_H
#define BHT_RELAY_POOL_H

#include "PoolBase.h"

#include <boost/preprocessor/repetition.hpp>
#include <boost/preprocessor/arithmetic/sub.hpp>
#include <boost/preprocessor/punctuation/comma_if.hpp>
#include <boost/preprocessor/iteration/local.hpp>
#include <boost/preprocessor/facilities/intercept.hpp>
#include <boost/preprocessor/comparison/not_equal.hpp>
#include <boost/type_traits/add_const.hpp>
#include <boost/type_traits/add_reference.hpp>
#include <boost/type_traits/remove_const.hpp>
#include <boost/type_traits/remove_reference.hpp>

BHT_RELAY_BEGIN

struct PoolNoneArg;

#ifndef POOL_MAX_ARG_NUM
#	define POOL_MAX_ARG_NUM 3
#endif

#define POOL_DECL_MEMBER(unused, i, z) typename boost::remove_const<typename boost::remove_reference<A##i>::type>::type _a##i;
#define POOL_DECL_MEMBER_GETTER(unused, i, z) typename boost::add_const<typename boost::add_reference<A##i>::type>::type a##i() const { return _a##i; }
#define POOL_CTOR_INIT_LIST(unused, i, z) _a##i(a##i) BOOST_PP_COMMA_IF(BOOST_PP_NOT_EQUAL(BOOST_PP_SUB(z, 1), i))

template <
	class T,
	class BaseT = T
	BOOST_PP_COMMA_IF(POOL_MAX_ARG_NUM)
	BOOST_PP_ENUM_BINARY_PARAMS(POOL_MAX_ARG_NUM, class A, = PoolNoneArg BOOST_PP_INTERCEPT)
>
struct Pool : public PoolBase<BaseT>
{
	typedef typename PoolBase<BaseT>::ElemT ElemT;
	typedef boost::shared_ptr<Pool> Ptr;
public:
	explicit Pool(BOOST_PP_ENUM_BINARY_PARAMS(POOL_MAX_ARG_NUM, A, a) BOOST_PP_COMMA_IF(POOL_MAX_ARG_NUM) uint32_t initialize=0)
		BOOST_PP_IF(POOL_MAX_ARG_NUM, :, BOOST_PP_EMPTY())
			BOOST_PP_REPEAT(POOL_MAX_ARG_NUM, POOL_CTOR_INIT_LIST, POOL_MAX_ARG_NUM)
	{
		for(uint32_t i=0; i<initialize; ++i) { this->Free(DoAlloc()); }
	}
	BOOST_PP_REPEAT(POOL_MAX_ARG_NUM, POOL_DECL_MEMBER_GETTER, ~)
private:
	ElemT DoAlloc() const { return ElemT(new T(BOOST_PP_ENUM_PARAMS(POOL_MAX_ARG_NUM, _a))); }
private:
	BOOST_PP_REPEAT(POOL_MAX_ARG_NUM, POOL_DECL_MEMBER, ~)
};

#if POOL_MAX_ARG_NUM > 0
#define POOL_GET_3RD(unused1, unused2, x) x
#define BOOST_PP_LOCAL_LIMITS (0, BOOST_PP_SUB(POOL_MAX_ARG_NUM, 1))
#define BOOST_PP_LOCAL_MACRO(n) \
template < \
	class T, \
	class BaseT \
	BOOST_PP_COMMA_IF(n) \
	BOOST_PP_ENUM_PARAMS(n, class A) \
> \
struct Pool < \
	T, \
	BaseT \
	BOOST_PP_COMMA_IF(n) \
	BOOST_PP_ENUM_PARAMS(n, A) \
	BOOST_PP_COMMA_IF(BOOST_PP_SUB(POOL_MAX_ARG_NUM, n)) \
	BOOST_PP_ENUM(BOOST_PP_SUB(POOL_MAX_ARG_NUM, n), POOL_GET_3RD, PoolNoneArg) \
> : public PoolBase<BaseT> \
{ \
	typedef typename PoolBase<BaseT>::ElemT ElemT; \
	typedef boost::shared_ptr<Pool> Ptr; \
public: \
	explicit Pool(BOOST_PP_ENUM_BINARY_PARAMS(n, A, a) BOOST_PP_COMMA_IF(n) uint32_t initialize=0) \
		BOOST_PP_IF(n, :, BOOST_PP_EMPTY()) \
			BOOST_PP_REPEAT(n, POOL_CTOR_INIT_LIST, n) \
	{ \
		for(uint32_t i=0; i<initialize; ++i) { Free(DoAlloc()); } \
	} \
	BOOST_PP_REPEAT(n, POOL_DECL_MEMBER_GETTER, ~) \
private: \
	ElemT DoAlloc() const { \
		return ElemT(new T(BOOST_PP_ENUM_PARAMS(n, _a))); \
	} \
private: \
	BOOST_PP_REPEAT(n, POOL_DECL_MEMBER, ~) \
};

#include BOOST_PP_LOCAL_ITERATE()
#undef POOL_GET_3RD
#undef POOL_DECL_MEMBER
#undef POOL_DECL_MEMBER_GETTER
#undef POOL_CTOR_INIT_LIST
#endif // POOL_MAX_ARG_NUM > 0

BHT_RELAY_END

#endif // BHT_RELAY_POOL_H

