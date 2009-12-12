#ifndef BHT_CLIENTIMPL_H__
#define BHT_CLIENTIMPL_H__

#include <string>
#include <vector>
#include <map>
#include <boost/shared_ptr.hpp>
#include <thrift/concurrency/Mutex.h>
#include <thrift/transport/TTransport.h>
#include "Common.h"
#include "Client.h"
#include "gen-cpp/BHT.h"

BHT_CODE_BEGIN

class ClientImpl {
public:
	/**
	 * BHT 客户端接口实现类构造函数
	 * @param domain BHT客户端对应域
	 * @param host BHT服务主机地址
	 * @param port BHT服务端口
	 * @param conn_timeout 底层socket连接超时时间(ms)
	 * @param send_timeout 底层socket数据发送超时时间(ms)
	 * @param recv_timeout 底层socket数据接收超时时间(ms)
	 * */
	ClientImpl(const ::std::string &domain, const ::std::string &host, int port,
			int conn_timeout, int send_timeout, int recv_timeout);
	~ClientImpl();	//< 非虚析构函数，提高效率

	const ::std::string& domain() const { return _domain; }
	const ::std::string& host() const { return _host; }
	int port() const { return _port; }

	void open();
	void close();
	bool get(const Client::key_type &k, Client::val_type *v);
	void set(const Client::key_type &k, const Client::val_type &v);
	void del(const Client::key_type &k);
	void mget(const ::std::vector<Client::key_type> &ks, ::std::map<Client::key_type, Client::val_type> *ret);
	void mset(const ::std::map<Client::key_type, Client::val_type> &kvs);
	void mdel(const ::std::vector<Client::key_type> &ks);

private:
	void ping();

	::apache::thrift::concurrency::Mutex _mtx;

	::std::string _domain;
	::std::string _host;
	int _port;
	int _conn_timeout;
	int _send_timeout;
	int _recv_timeout;

	::boost::shared_ptr< ::apache::thrift::transport::TTransport > _transport;
	::boost::shared_ptr<BHTClient> _client;
};

BHT_CODE_END

#endif

