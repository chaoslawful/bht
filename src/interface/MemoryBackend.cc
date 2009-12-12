#include "MemoryBackend.h"
#include "Logging.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

BHT_INIT_LOGGING("BHT.Interface.MemoryBackend")

shared_ptr<Backend> MemoryBackendFactory::getBackendInstance(const string &host, int port)
{
	ScopedLoggingCtx logging_ctx("<MemoryBackendFactory::getBackendInstance>");
	return shared_ptr<Backend>(new MemoryBackend(host, port));
}

MemoryBackend::MemoryBackend(const string &host, int port)
	:_host(host), _port(port)
{
}

MemoryBackend::~MemoryBackend()
{
}

bool MemoryBackend::get(const string &key, string *val)
{
	ScopedLoggingCtx logging_ctx("<MemoryBackend::get>");
	for(map<string,string>::iterator it = _mem.begin(); it != _mem.end(); ++it) {
		const string &k = it->first;
		const string &v = it->second;
		if(k.size() >= key.size() && !k.compare(0, key.size(), key)) {
			// 找到了首个匹配给定前缀的 key，返回其对应的 val
			*val = v;
			return true;
		}
	}

	INFO("No record found in memory: key = " + key);
	return false;
}

void MemoryBackend::mget(map<string, pair<bool, string> > *kvs)
{
	ScopedLoggingCtx logging_ctx("<MemoryBackend::mget>");
	for(map<string, pair<bool, string> >::iterator it = kvs->begin(); it != kvs->end(); ++it) {
		const string &k = it->first;
		string &v = it->second.second;
		it->second.first = get(k, &v);
	}
}

void MemoryBackend::set(const string &key, const string &val, bool overwrite, int timeout)
{
	ScopedLoggingCtx logging_ctx("<MemoryBackend::set>");
	(void)timeout;

	map<string, string>::iterator it = _mem.find(key);
	if(it != _mem.end()) {
		if(overwrite) {
			it->second = val;
			INFO("Record existed and overwriting demanded, overwriting...");
		} else {
			INFO("Record existed and no overwriting demanded, skipping...");
		}
	} else {
		INFO("Insert new record into memory");
		_mem[key] = val;
	}
}

void MemoryBackend::del(const string &key)
{
	ScopedLoggingCtx logging_ctx("<MemoryBackend::del>");
	_mem.erase(key);
	INFO("Record erased: key = " + key);
}

BHT_CODE_END

