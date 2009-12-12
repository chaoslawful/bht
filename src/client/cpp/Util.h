#ifndef BHT_UTIL_H__
#define BHT_UTIL_H__

#include <functional>
#include "Common.h"
#include "Client.h"
#include "Validator.h"
#include "gen-cpp/BHT.h"

BHT_CODE_BEGIN

////////////////////////// 格式转换函子类模板 /////////////////////////////
template<class S, class T>
struct Converter : public ::std::unary_function<S, T> {
	T operator()(const S &e) const
	{
		return convert(e);
	}

	/*
	static T convert(const S&)
	{
		throw ::std::runtime_error("No specialized converter found: S = "
				+ ::std::string(typeid(S).name())
				+ ", T = " + ::std::string(typeid(S).name()));
	}
	*/
	static T convert(const S&);
};

// const Key -> Client::key_type
template<> Client::key_type Converter<Key, Client::key_type>::convert(const Key &k)
{
	Client::key_type ck;
	if(k.__isset.key) {
		ck.first = k.key;
	}
	if(k.__isset.subkey) {
		ck.second = k.subkey;
	}
	return ck;
}

// const Client::key_type -> Key
template<> Key Converter<Client::key_type, Key>::convert(const Client::key_type &ck)
{
	Key k;
	if(ck.first.size()) {
		k.__isset.key = true;
		k.key = ck.first;
	}
	if(ck.second.size()) {
		k.__isset.subkey = true;
		k.subkey = ck.second;
	}
	return k;
}

// const Val -> Client::val_type
template<> Client::val_type Converter<Val, Client::val_type>::convert(const Val &v)
{
	Client::val_type cv = v.value;
	return cv;
}

// const Client::val_type -> Val
template<> Val Converter<Client::val_type, Val>::convert(const Client::val_type &cv)
{
	Val v;
	v.__isset.value = true;
	v.value = cv;
	return v;
}

// const pair<Key, Val> -> pair<Client::key_type, Client::val_type>
template<>
::std::pair<Client::key_type, Client::val_type>
	Converter<
		::std::pair<Key, Val>,
		::std::pair<Client::key_type, Client::val_type>
	>::convert(const ::std::pair<Key, Val> &kv)
{
	Client::key_type ck = Converter<Key, Client::key_type>::convert(kv.first);
	Client::val_type cv = Converter<Val, Client::val_type>::convert(kv.second);
	return ::std::pair<Client::key_type, Client::val_type>(ck, cv);
}

// const pair<Client::key_type, Client::val_type> -> pair<Key, Val>
template<>
::std::pair<Key, Val>
	Converter<
		::std::pair<Client::key_type, Client::val_type>,
		::std::pair<Key, Val>
	>::convert(const ::std::pair<Client::key_type, Client::val_type> &ckv)
{
	Key k = Converter<Client::key_type, Key>::convert(ckv.first);
	Val v = Converter<Client::val_type, Val>::convert(ckv.second);
	return ::std::pair<Key, Val>(k, v);
}

////////////////////////// 参数检查函子类模板 /////////////////////////////
template<class S>
struct ClientValidator : public ::std::unary_function<S, bool> {
	bool operator()(const S &e) const
	{
		return is_valid(e);
	}

	/*
	static bool is_valid(const S&)
	{
		throw ::std::runtime_error("No specialized validator found: S = " + ::std::string(typeid(S).name()));
	}
	*/
	static bool is_valid(const S&);
};

// const Client::key_type -> bool
template<> bool ClientValidator<Client::key_type>::is_valid(const Client::key_type &k)
{
	return Validator::isValidKey(k.first) && Validator::isValidSubkey(k.second);
}

// const Client::val_type -> bool
template<> bool ClientValidator<Client::val_type>::is_valid(const Client::val_type &v)
{
	return Validator::isValidValue(v);
}

// const pair<Client::key_type, Client::val_type> -> bool
template<>
bool ClientValidator< ::std::pair<Client::key_type, Client::val_type> >
	::is_valid(const ::std::pair<Client::key_type, Client::val_type> &kv)
{
	return ClientValidator<Client::key_type>::is_valid(kv.first)
			&& ClientValidator<Client::val_type>::is_valid(kv.second);
}

BHT_CODE_END

#endif

