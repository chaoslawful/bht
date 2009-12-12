#ifndef BHT_MEMORYBACKEND_H__
#define BHT_MEMORYBACKEND_H__

#include "Common.h"
#include "Backend.h"

BHT_CODE_BEGIN

class MemoryBackend:virtual public Backend {
public:
	MemoryBackend(const ::std::string &host, int port);
	~MemoryBackend();

	bool get(const ::std::string &key, ::std::string *val);
	void mget(::std::map< ::std::string, ::std::pair<bool, ::std::string> > *kvs);
	void set(const ::std::string &key, const ::std::string &val, bool overwrite, int timeout);
	void del(const ::std::string &key);

	::std::string host() const { return _host; }
	int port() const { return _port; }

private:
	::std::map< ::std::string, ::std::string > _mem;
	::std::string _host;
	int _port;
};

class MemoryBackendFactory:virtual public BackendFactory {
public:
	MemoryBackendFactory() {}
	~MemoryBackendFactory() {}

	::boost::shared_ptr<Backend> getBackendInstance(const ::std::string &host, int port);
};

BHT_CODE_END

#endif

