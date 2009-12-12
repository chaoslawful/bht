#include "MemcachedBackend.h"
#include "Logging.h"
#include "Error.h"
#include <boost/scoped_array.hpp>

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.MemcachedBackend")

shared_ptr<Backend> MemcachedBackendFactory::getBackendInstance(const string &host, int port)
{
	ScopedLoggingCtx logging_ctx("<MemcachedBackendFactory::getBackendInstance>");
	return shared_ptr<Backend>(new MemcachedBackend(host, port));
}

MemcachedBackend::MemcachedBackend(const string &host, int port)
	:_host(host), _port(port)
{
	ScopedLoggingCtx logging_ctx("<MemcachedBackend::MemcachedBackend>");
	_mc = memcached_create(NULL);
	if(!_mc) {
		ERROR("Failed to allocate memory for memcached_st instance");
		InvalidOperation e;
		e.ec = Error::BHT_EC_OUT_OF_MEMORY;
		e.msg = "Failed to allocate memory for memcached_st instance";
		throw e;
	}

	memcached_return rc;

	// 强制使用 binary protocol 同 memcached 通信，要求 memcached-1.4.0 及以上版本
	rc = memcached_behavior_set(_mc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
	if(rc != MEMCACHED_SUCCESS) {
		InvalidOperation e;
		e.ec = Error::BHT_EC_TUNE_BACKEND;
		e.msg = "Failed to set libmemcached to use binary memcached protocol: " + string(memcached_strerror(_mc, rc));
		ERROR(e.msg);
		throw e;
	}

	rc = memcached_server_add(_mc, _host.c_str(), _port);
	if(rc != MEMCACHED_SUCCESS) {
		InvalidOperation e;
		e.ec = Error::BHT_EC_TUNE_BACKEND;
		e.msg = "Failed to add server into memcached_st instance: " + string(memcached_strerror(_mc, rc));
		ERROR(e.msg + ", host = " + _host + ", port = " + lexical_cast<string>(_port));
		throw e;
	}
}

MemcachedBackend::~MemcachedBackend()
{
	ScopedLoggingCtx logging_ctx("<MemcachedBackend::~MemcachedBackend>");
	memcached_free(_mc);
}

bool MemcachedBackend::get(const string &key, string *val)
{
	ScopedLoggingCtx logging_ctx("<MemcachedBackend::get>");
	size_t vlen;
	uint32_t flags;
	memcached_return rc;
	char *v = memcached_get(_mc, key.data(), key.size(), &vlen, &flags, &rc);

	if(rc == MEMCACHED_SUCCESS) {
		// found record
		*val = string(v, vlen);
		free(v);
		return true;
	}

	if(rc != MEMCACHED_NOTFOUND) {
		// error occured
		InvalidOperation e;
		e.ec = Error::BHT_EC_BACKEND_ERROR;
		e.msg = "Backend error: " + string(memcached_strerror(_mc, rc));
		ERROR("Get failed (host = " + _host
				+ ", port = " + lexical_cast<string>(_port)
				+ ", key = " + key
				+ "): " + e.msg);
		throw e;
	}

	// record not found
	return false;
}

void MemcachedBackend::mget(map<string,pair<bool,string> > *kvs)
{
	ScopedLoggingCtx logging_ctx("<MemcachedBackend::mget>");
	memcached_return rc;
	scoped_array<char*> keys;
	scoped_array<size_t> key_lens;
	int cnt;
	char *vbuf;
	size_t vbuf_len;
	scoped_array<char> kbuf;
	size_t kbuf_len = 0;
	uint32_t flags;

	keys.reset(new char*[kvs->size()]);
	key_lens.reset(new size_t[kvs->size()]);

	cnt = 0;
	for(map<string,pair<bool,string> >::iterator it = kvs->begin(); it != kvs->end(); ++it, ++cnt) {
		const string &key = it->first;
		keys[cnt] = (char*)key.data();
		key_lens[cnt] = key.size();
		if(kbuf_len < key.size()) {
			kbuf_len = key.size();
		}
	}

	rc = memcached_mget(_mc, keys.get(), key_lens.get(), cnt);
	if(rc != MEMCACHED_SUCCESS) {
		// error occured
		InvalidOperation e;
		e.ec = Error::BHT_EC_BACKEND_ERROR;
		e.msg = "Backend error: " + string(memcached_strerror(_mc, rc));
		ERROR("MGet failed (host = " + _host
				+ ", port = " + lexical_cast<string>(_port)
				+ ", nkey = " + lexical_cast<string>(kvs->size())
				+ "): " + e.msg);
		throw e;
	}

	kbuf.reset(new char[kbuf_len]);
	while(1) {
		vbuf = memcached_fetch(_mc, kbuf.get(), &kbuf_len, &vbuf_len, &flags, &rc);
		if(rc == MEMCACHED_END) {
			break;
		}
		if(rc == MEMCACHED_SUCCESS) {
			map<string,pair<bool,string> >::iterator it = kvs->find(string(kbuf.get(),kbuf_len));
			if(it != kvs->end()) {
				it->second.first = true;
				it->second.second = string(vbuf, vbuf_len);
				free(vbuf);
			}
		}
	}

}

void MemcachedBackend::set(const string &key, const string &val, bool overwrite, int timeout)
{
	ScopedLoggingCtx logging_ctx("<MemcachedBackend::set>");
	memcached_return rc;
	
	if(overwrite) {
		// overwriting set
		rc = memcached_set(_mc, key.data(), key.size(), val.data(), val.size(), timeout, 0);
		if(rc != MEMCACHED_SUCCESS) {
			// error occured
			InvalidOperation e;
			e.ec = Error::BHT_EC_BACKEND_ERROR;
			e.msg = "Backend error: " + string(memcached_strerror(_mc, rc));
			ERROR("Set (overwriting) failed (host = " + _host
					+ ", port = " + lexical_cast<string>(_port)
					+ ", key = " + key
					+ ", val = " + val
					+ "): " + e.msg);
			throw e;
		}
	} else {
		// non-overwriting set
		rc = memcached_add(_mc, key.data(), key.size(), val.data(), val.size(), timeout, 0);
		if(rc != MEMCACHED_SUCCESS && rc != MEMCACHED_DATA_EXISTS) {
			// error occured
			InvalidOperation e;
			e.ec = Error::BHT_EC_BACKEND_ERROR;
			e.msg = "Backend error: " + string(memcached_strerror(_mc, rc));
			ERROR("Set (non-overwriting) failed (host = " + _host
					+ ", port = " + lexical_cast<string>(_port)
					+ ", key = " + key
					+ ", val = " + val
					+ "): " + e.msg);
			throw e;
		}
	}
}

void MemcachedBackend::del(const string &key)
{
	ScopedLoggingCtx logging_ctx("<MemcachedBackend::del>");
	memcached_return rc;

	rc = memcached_delete(_mc, key.data(), key.size(), 0);
	if(rc != MEMCACHED_SUCCESS && rc != MEMCACHED_NOTFOUND) {
		// error occured
		InvalidOperation e;
		e.ec = Error::BHT_EC_BACKEND_ERROR;
		e.msg = "Backend error: " + string(memcached_strerror(_mc, rc));
		ERROR("Delete failed (host = " + _host
				+ ", port = " + lexical_cast<string>(_port)
				+ ", key = " + key
				+ "): " + e.msg);
		throw e;
	}
}

BHT_CODE_END

