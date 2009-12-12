#ifndef BHT_TOKYOTYRANTBACKEND_H__
#define BHT_TOKYOTYRANTBACKEND_H__

#include "Common.h"
#include "Backend.h"

#include <tcrdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

BHT_CODE_BEGIN

class TokyoTyrantBackend:virtual public Backend {
public:
	TokyoTyrantBackend(const ::std::string &host, int port);
	~TokyoTyrantBackend();

	bool get(const ::std::string &key, ::std::string *val);
	void mget(::std::map< ::std::string, ::std::pair<bool, ::std::string> > *kvs);
	void set(const ::std::string &key, const ::std::string &val, bool overwrite, int timeout);
	void del(const ::std::string &key);

	::std::string host() const { return _host; }
	int port() const { return _port; }

private:
	TCRDB *_rdb;
	::std::string _host;
	int _port;
};

class TokyoTyrantBackendFactory:virtual public BackendFactory {
public:
	TokyoTyrantBackendFactory() {}
	~TokyoTyrantBackendFactory() {}

	::boost::shared_ptr<Backend> getBackendInstance(const ::std::string &host, int port);
};

BHT_CODE_END

#endif

