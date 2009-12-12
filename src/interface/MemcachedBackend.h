#ifndef BHT_MEMCACHEDBACKEND_H__
#define BHT_MEMCACHEDBACKEND_H__

#include "Common.h"
#include "Backend.h"
#include <libmemcached/memcached.h>

BHT_CODE_BEGIN

class MemcachedBackend:virtual public Backend {
public:
	MemcachedBackend(const ::std::string &host, int port);
	~MemcachedBackend();

	bool get(const ::std::string &key, ::std::string *val);
	void mget(::std::map< ::std::string, ::std::pair<bool, ::std::string> > *kvs);
	void set(const ::std::string &key, const ::std::string &val, bool overwrite, int timeout);
	void del(const ::std::string &key);

	::std::string host() const { return _host; }
	int port() const { return _port; }

private:
	memcached_st *_mc;
	::std::string _host;
	int _port;
};

class MemcachedBackendFactory:virtual public BackendFactory {
public:
	MemcachedBackendFactory() {}
	~MemcachedBackendFactory() {}

	::boost::shared_ptr<Backend> getBackendInstance(const ::std::string &host, int port);
};

BHT_CODE_END

#endif

