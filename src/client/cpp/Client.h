#ifndef BHT_CLIENT_H__
#define BHT_CLIENT_H__

#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include <boost/shared_ptr.hpp>
#include "Common.h"

BHT_CODE_BEGIN

class ClientImpl;

/// BHT 客户端接口类
/**
 * 封装了 BHT 对外部用户提供的操作接口。一个客户端接口类实例对应一个持久连接，因此在内部所有操作都是线程互斥的。
 * */
class Client {
public:
	typedef ::std::pair< ::std::string/* key*/, ::std::string/* subkey*/> key_type;	//< 键类型
	typedef ::std::string val_type;	//< 值类型

	/**
	 * BHT 客户端接口构造函数。
	 * @param domain BHT客户端对应域
	 * @param host BHT服务主机地址
	 * @param port BHT服务端口
	 * @param conn_timeout
	 * @param send_timeout
	 * @param recv_timeout
	 * */
	Client(
			const ::std::string &domain,
			const ::std::string &host = "localhost",
			int port = 9090,
			int conn_timeout = 500,		// 默认连接超时时间为 500 ms
			int send_timeout = 1000,	// 默认数据发送超时时间为 1000 ms
			int recv_timeout = 1000		// 默认数据接收超时时间为 1000 ms
		);
	Client(const Client&);	//< 拷贝构造函数
	const Client& operator=(const Client&);	//< 赋值操作
	~Client();	//< 非虚析构函数，提高效率

	/// 返回当前 BHT Client 使用的 domain 字符串
	const ::std::string& domain() const;
	
	/// 返回当前 BHT Client 使用的 host 字符串
	const ::std::string& host() const;

	/// 返回当前 BHT Client 使用的 port 数值
	int port() const;

	/**
	 * 开启对 BHT 服务的连接
	 * @throw std::runtime_error 后端服务错误异常
	 * */
	void open() const;

	/**
	 * 关闭对 BHT 服务的连接
	 * @throw std::runtime_error 后端服务错误异常
	 * */
	void close() const;

	/**
	 * 查询给定键对应值。
	 * @param k 待查询的键结构。结构中首元素为主键，必须非空；次元素为子键，可以为空。
	 * @param v 查询值字符串指针
	 * @retval 若给定键存在则返回 true，否则返回 false。
	 * @throw std::runtime_error 后端服务错误异常
	 * */
	bool get(const key_type &k, val_type *v) const;

	/**
	 * 设置给定键对应值。
	 * @param k 待设置的键结构。结构中首元素为主键，必须非空；次元素为子键，可以为空。
	 * @param v 待设置的对应值字符串。
	 * @throw std::runtime_error 后端服务错误异常
	 * */
	void set(const key_type &k, const val_type &v) const;

	/**
	 * 删除给定键。
	 * @param k 待删除的键结构。结构中首元素为主键，必须非空；次元素为子键，可以为空。
	 * @throw std::runtime_error 后端服务错误异常
	 * */
	void del(const key_type &k) const;

	/**
	 * 查询多个键对应的值。
	 * @param ks 待查询的键结构数组。
	 * @param ret 查询结果映射结构指针。其中将包含所有存在的键对应的值。
	 * @throw std::runtime_error 后端服务错误异常
	 * */
	void mget(const ::std::vector<key_type> &ks, ::std::map<key_type, val_type> *ret) const;

	/**
	 * 设置多个键对应的值。
	 * @param kvs 待设置键值对映射结构。
	 * @throw std::runtime_error 后端服务错误异常
	 * */
	void mset(const ::std::map<key_type, val_type> &kvs) const;

	/**
	 * 删除多个键。
	 * @param ks 待删除的键结构数组。
	 * @throw std::runtime_error 后端服务错误异常
	 * */
	void mdel(const ::std::vector<key_type> &ks) const;

	/**
	 * 构造 BHT 客户端使用的键结构。
	 * @param pk 主键字符串，必选。应该非空。
	 * @param sk 子键字符串，可选。默认为空。
	 * @retval 对应的 BHT 客户端键结构。
	 * */
	static key_type make_key(const ::std::string &pk, const ::std::string &sk = ::std::string())
	{
		return ::std::pair< ::std::string, ::std::string >(pk, sk);
	}

	/**
	 * 构造 BHT 客户端使用的值结构。
	 * @param v 值字符串。
	 * @retval 对应的 BHT 客户端值结构。
	 * */
	static const val_type& make_val(const ::std::string &v) { return v; }

	/**
	 * 获取 BHT 客户端键结构中的主键字符串。
	 * @param k BHT 客户端键结构。
	 * @retval 给定键结构中的主键字符串。若主键不存在则该字符串为空。
	 * */
	static const ::std::string& get_primary_key(const key_type &k) { return k.first; }

	/**
	 * 获取 BHT 客户端键结构中的子键字符串。
	 * @param k BHT 客户端键结构。
	 * @retval 给定键结构中的子键字符串。若子键不存在则该字符串为空。
	 * */
	static const ::std::string& get_secondary_key(const key_type &k) { return k.second; }

	/**
	 * 获取 BHT 客户端值结构中的值字符串。
	 * @param v BHT 客户端值结构。
	 * @retval 给定值结构中的值字符串。
	 * */
	static const ::std::string& get_value(const val_type &v) { return v; }

private:
	::boost::shared_ptr<ClientImpl> _impl;
};

BHT_CODE_END

#endif

