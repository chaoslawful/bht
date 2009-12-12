#include "TokyoTyrantBackend.h"
#include "Logging.h"
#include "Error.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.TokyoTyrantBackend")

shared_ptr<Backend> TokyoTyrantBackendFactory::getBackendInstance(const string &host, int port)
{
	ScopedLoggingCtx logging_ctx("<TokyoTyrantBackendFactory::getBackendInstance>");
	return shared_ptr<Backend>(new TokyoTyrantBackend(host, port));
}

TokyoTyrantBackend::TokyoTyrantBackend(const string &host, int port)
	:_host(host), _port(port)
{
	ScopedLoggingCtx logging_ctx("<TokyoTyrantBackend::TokyoTyrantBackend>");
	_rdb = tcrdbnew();
	if(!_rdb) {
		ERROR("Cannot allocate memory for TCRDB object");

		InvalidOperation e;
		e.ec = Error::BHT_EC_OUT_OF_MEMORY;
		e.msg = "Cannot allocate memory for TCRDB object";
		throw e;
	}

	// 设置远程操作超时时间为1.0s，连接中断时自动重连
	if(!tcrdbtune(_rdb, 1.0, RDBTRECON)) {
		tcrdbdel(_rdb);

		ERROR("Cannot set option for TCRDB object");

		InvalidOperation e;
		e.ec = Error::BHT_EC_TUNE_BACKEND;
		e.msg = "Cannot set option for TCRDB object";
		throw e;
	}

	if(!tcrdbopen(_rdb, _host.c_str(), _port)) {
		tcrdbdel(_rdb);

		ERROR("Failed to connect to TokyoTyrant service: host = " + _host + ", port = " + lexical_cast<string>(_port));

		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Failed to connect to TokyoTyrant service";
		throw e;
	}
}

TokyoTyrantBackend::~TokyoTyrantBackend()
{
	ScopedLoggingCtx logging_ctx("<TokyoTyrantBackend::~TokyoTyrantBackend>");
	if(!tcrdbclose(_rdb)) {
		WARN("Failed to close TokyoTyrant connection normally: host = " + _host + ", port = " + lexical_cast<string>(_port));
	}

	tcrdbdel(_rdb);
}

bool TokyoTyrantBackend::get(const string &key, string *val)
{
	ScopedLoggingCtx logging_ctx("<TokyoTyrantBackend::get>");
	int sp;
	char *v;

	// TT 后端用 skel 重写 get 接口行为为前缀查找，因此这里可以直接调用
	v = (char*)tcrdbget(_rdb, key.data(), key.size(), &sp);
	if(!v) {
		int ec = tcrdbecode(_rdb);
		if(ec != TTESUCCESS && ec != TTEKEEP && ec != TTENOREC) {
			// 连接错误，抛出异常
			InvalidOperation e;
			e.ec = Error::BHT_EC_BACKEND_ERROR;
			e.msg = "Backend error: " + string(tcrdberrmsg(ec));
			ERROR("Operation failed (host = " + _host + ", port = " + lexical_cast<string>(_port) + "): " + e.msg);
			throw e;
		}

		// 未找到相关记录
		return false;
	}

	*val = string(v, sp);
	tcfree(v);

	return true;
}

void TokyoTyrantBackend::mget(map<string,pair<bool,string> > *kvs)
{
	ScopedLoggingCtx logging_ctx("<TokyoTyrantBackend::mget>");
	TCMAP *recs = tcmapnew();
	if(!recs) {
		ERROR("Cannot allocate memory for internal TCMAP object");
		InvalidOperation e;
		e.ec = Error::BHT_EC_OUT_OF_MEMORY;
		e.msg = "Cannot allocate memory for internal TCMAP object";
		throw e;
	}

	for(map<string,pair<bool,string> >::iterator it = kvs->begin(); it != kvs->end(); ++it) {
		const string &k = it->first;
		tcmapput(recs, k.data(), k.size(), NULL, 0);
	}

	if(!tcrdbget3(_rdb, recs)) {
		tcmapdel(recs);

		int ec = tcrdbecode(_rdb);
		if(ec != TTESUCCESS && ec != TTEKEEP && ec != TTENOREC) {
			// 连接错误，抛出异常
			InvalidOperation e;
			e.ec = Error::BHT_EC_BACKEND_ERROR;
			e.msg = "Backend error: " + string(tcrdberrmsg(ec));
			ERROR("Operation failed (host = " + _host + ", port = " + lexical_cast<string>(_port) + "): " + e.msg);
			throw e;
		}
		
		return;
	}

	for(map<string,pair<bool,string> >::iterator it = kvs->begin(); it != kvs->end(); ++it) {
		const string &k = it->first;
		string &v = it->second.second;
		int sp;
		const char *p = (const char*)tcmapget(recs, (const char*)k.data(), k.size(), &sp);
		
		if(p) {
			it->second.first = true;
			v = string(p, sp);
		} else {
			it->second.first = false;
			v = string();
		}
	}

	tcmapdel(recs);
}

void TokyoTyrantBackend::set(const string &key, const string &val, bool overwrite, int timeout)
{
	ScopedLoggingCtx logging_ctx("<TokyoTyrantBackend::set>");
	if(overwrite) {
		if(!tcrdbput(_rdb, key.data(), key.size(), val.data(), val.size())) {
			int ec = tcrdbecode(_rdb);
			if(ec != TTESUCCESS && ec != TTEKEEP && ec != TTENOREC) {
				// 连接错误，抛出异常
				InvalidOperation e;
				e.ec = Error::BHT_EC_BACKEND_ERROR;
				e.msg = "Backend error: " + string(tcrdberrmsg(ec));
				ERROR("Operation failed (host = " + _host + ", port = " + lexical_cast<string>(_port) + "): " + e.msg);
				throw e;
			}
		}
	} else {
		if(!tcrdbputkeep(_rdb, key.data(), key.size(), val.data(), val.size())) {
			int ec = tcrdbecode(_rdb);
			if(ec != TTESUCCESS && ec != TTEKEEP && ec != TTENOREC) {
				// 连接错误，抛出异常
				InvalidOperation e;
				e.ec = Error::BHT_EC_BACKEND_ERROR;
				e.msg = "Backend error: " + string(tcrdberrmsg(ec));
				ERROR("Operation failed (host = " + _host + ", port = " + lexical_cast<string>(_port) + "): " + e.msg);
				throw e;
			}
		}
	}
}

void TokyoTyrantBackend::del(const string &key)
{
	ScopedLoggingCtx logging_ctx("<TokyoTyrantBackend::del>");
	if(!tcrdbout(_rdb, key.data(), key.size())) {
		int ec = tcrdbecode(_rdb);
		if(ec != TTESUCCESS && ec != TTEKEEP && ec != TTENOREC) {
			// 连接错误，抛出异常
			InvalidOperation e;
			e.ec = Error::BHT_EC_BACKEND_ERROR;
			e.msg = "Backend error: " + string(tcrdberrmsg(ec));
			ERROR("Operation failed (host = " + _host + ", port = " + lexical_cast<string>(_port) + "): " + e.msg);
			throw e;
		}
	}
}

BHT_CODE_END

