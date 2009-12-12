#ifndef BHT_RELAYBACKEND_H__
#define BHT_RELAYBACKEND_H__

#include "Common.h"
#include "Backend.h"
#include "gen-cpp/Local.h"

BHT_CODE_BEGIN

class RelayBackend:virtual public Backend {
public:
	RelayBackend(const ::std::string &host, int port);
	~RelayBackend();

	// Relay 服务交互方法
	void onSet(const ::std::string &domain, const Key &k, const Val &v);
	void onDel(const ::std::string &domain, const Key &k);
	void onMSet(const ::std::string &domain, const ::std::map<Key, Val> &kvs);
	void onMDel(const ::std::string &domain, const ::std::vector<Key> &ks);
	
	// 用于兼容 Backend 接口的空方法
	bool get(const ::std::string &key, ::std::string *val) { return false; }
	void mget(::std::map< ::std::string, ::std::pair<bool, ::std::string> > *kvs) {}
	void set(const ::std::string &key, const ::std::string &val, bool overwrite, int timeout) {}
	void del(const ::std::string &key) {}

	::std::string host() const { return _host; }
	int port() const { return _port; }

private:
	::std::string _host;
	int _port;

	::boost::shared_ptr< ::apache::thrift::transport::TTransport > _transport;
	::boost::shared_ptr<Relay::LocalClient> _client;
};

class RelayBackendFactory:virtual public BackendFactory {
public:
	RelayBackendFactory() {}
	~RelayBackendFactory() {}

	::boost::shared_ptr<Backend> getBackendInstance(const ::std::string &host, int port);
};

BHT_CODE_END

#endif

