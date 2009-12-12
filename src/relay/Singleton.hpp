/*
 * =====================================================================================
 *
 *       Filename:  Singleton.hpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2008年11月10日 11时53分39秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef SINGLETON_HPP
#define SINGLETON_HPP

#include <boost/shared_ptr.hpp>

template <typename T>
class Singleton
{
public:
	static bool Create() {
		if (!instance__) instance__.reset(new T());
		return instance__.get() != 0;
	}
	static boost::shared_ptr<T> Get() {
		return instance__;
	}
	static void Destroy() {
		instance__.reset();
	}

	template <typename Arg0>
	static bool Create(const Arg0& arg0) {
		if (!instance__) instance__.reset(new T(arg0));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1>
	static bool Create(const Arg0& arg0, const Arg1& arg1) {
		if (!instance__) instance__.reset(new T(arg0, arg1));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2>
	static bool Create(const Arg0& arg0, const Arg1& arg1, const Arg2& arg2) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3>
	static bool Create(const Arg0& arg0, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4>
	static bool Create(const Arg0& arg0, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5>
	static bool Create(const Arg0& arg0, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4, arg5));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6>
	static bool Create(const Arg0& arg0, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4, arg5, arg6));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6, typename Arg7>
	static bool Create(const Arg0& arg0, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6, typename Arg7, typename Arg8>
	static bool Create(const Arg0& arg0, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6, typename Arg7, typename Arg8, typename Arg9>
	static bool Create(const Arg0& arg0, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8, const Arg9& arg9) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9));
		return instance__.get() != 0;
	}

	template <typename Arg0>
	static bool Create(Arg0& arg0) {
		if (!instance__) instance__.reset(new T(arg0));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1>
	static bool Create(Arg0& arg0, Arg1& arg1) {
		if (!instance__) instance__.reset(new T(arg0, arg1));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2>
	static bool Create(Arg0& arg0, Arg1& arg1, Arg2& arg2) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3>
	static bool Create(Arg0& arg0, Arg1& arg1, Arg2& arg2, Arg3& arg3) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4>
	static bool Create(Arg0& arg0, Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5>
	static bool Create(Arg0& arg0, Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4, arg5));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6>
	static bool Create(Arg0& arg0, Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5, Arg6& arg6) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4, arg5, arg6));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6, typename Arg7>
	static bool Create(Arg0& arg0, Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5, Arg6& arg6, Arg7& arg7) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6, typename Arg7, typename Arg8>
	static bool Create(Arg0& arg0, Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5, Arg6& arg6, Arg7& arg7, Arg8& arg8) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8));
		return instance__.get() != 0;
	}
	template <typename Arg0, typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6, typename Arg7, typename Arg8, typename Arg9>
	static bool Create(Arg0& arg0, Arg1& arg1, Arg2& arg2, Arg3& arg3, Arg4& arg4, Arg5& arg5, Arg6& arg6, Arg7& arg7, Arg8& arg8, Arg9& arg9) {
		if (!instance__) instance__.reset(new T(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9));
		return instance__.get() != 0;
	}
private:
	static boost::shared_ptr<T> instance__;
};

template <typename T>
boost::shared_ptr<T> Singleton<T>::instance__;

#endif // SINGLETON_HPP

