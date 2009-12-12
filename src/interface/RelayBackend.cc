#include "RelayBackend.h"
#include "Logging.h"
#include "Error.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift;
using namespace ::apache::thrift::concurrency;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::protocol;

BHT_INIT_LOGGING("BHT.Interface.RelayBackend")

shared_ptr<Backend> RelayBackendFactory::getBackendInstance(const string &host, int port)
{
	ScopedLoggingCtx logging_ctx("<RelayBackendFactory::getBackendInstance>");
	return shared_ptr<Backend>(new RelayBackend(host, port));
}

RelayBackend::RelayBackend(const string &host, int port)
	:_host(host), _port(port)
{
	ScopedLoggingCtx logging_ctx("<RelayBackend::RelayBackend>");
	try {
		shared_ptr<TTransport> socket(new TSocket(host, port));
		_transport = shared_ptr<TTransport>(new TFramedTransport(socket));

		shared_ptr<TProtocol> protocol(new TBinaryProtocol(_transport));
		_client = shared_ptr<Relay::LocalClient>(new Relay::LocalClient(protocol));

		_transport->open();
	} catch(const InvalidOperation &tx) {
		ERROR("Caught exception: " + tx.msg);
		throw tx;
	} catch(const TException &tx) {
		ERROR("Caught exception: " + string(tx.what()));
		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Caught exception: " + string(tx.what());
		throw e;
	} catch(...) {
		ERROR("Caught unknown exception");
		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Caught unknown exception";
		throw e;
	}
}

RelayBackend::~RelayBackend()
{
	ScopedLoggingCtx logging_ctx("<RelayBackend::~RelayBackend>");
	try {
		_transport->close();
	} catch(const InvalidOperation &tx) {
		ERROR("Caught exception: " + tx.msg);
	} catch(const TException &tx) {
		ERROR("Caught exception: " + string(tx.what()));
	} catch(...) {
		ERROR("Caught unknown exception");
	}
	// 析构函数不允许抛出异常！
}

void RelayBackend::onSet(const string &domain, const Key &k, const Val &v)
{
	ScopedLoggingCtx logging_ctx("<RelayBackend::onSet>");
	try {
		_client->OnSet(domain, k, v);
	} catch(const InvalidOperation &tx) {
		ERROR("Caught exception: " + tx.msg);
		throw tx;
	} catch(const TException &tx) {
		ERROR("Caught exception: " + string(tx.what()));
		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Caught exception: " + string(tx.what());
		throw e;
	} catch(...) {
		ERROR("Caught unknown exception");
		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Caught unknown exception";
		throw e;
	}
}

void RelayBackend::onDel(const string &domain, const Key &k)
{
	ScopedLoggingCtx logging_ctx("<RelayBackend::onDel>");
	try {
		_client->OnDel(domain, k);
	} catch(const InvalidOperation &tx) {
		ERROR("Caught exception: " + tx.msg);
		throw tx;
	} catch(const TException &tx) {
		ERROR("Caught exception: " + string(tx.what()));
		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Caught exception: " + string(tx.what());
		throw e;
	} catch(...) {
		ERROR("Caught unknown exception");
		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Caught unknown exception";
		throw e;
	}
}

void RelayBackend::onMSet(const string &domain, const map<Key, Val> &kvs)
{
	ScopedLoggingCtx logging_ctx("<RelayBackend::onMSet>");
	try {
		_client->OnMSet(domain, kvs);
	} catch(const InvalidOperation &tx) {
		ERROR("Caught exception: " + tx.msg);
		throw tx;
	} catch(const TException &tx) {
		ERROR("Caught exception: " + string(tx.what()));
		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Caught exception: " + string(tx.what());
		throw e;
	} catch(...) {
		ERROR("Caught unknown exception");
		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Caught unknown exception";
		throw e;
	}
}

void RelayBackend::onMDel(const string &domain, const vector<Key> &ks)
{
	ScopedLoggingCtx logging_ctx("<RelayBackend::onMDel>");
	try {
		_client->OnMDel(domain, ks);
	} catch(const InvalidOperation &tx) {
		ERROR("Caught exception: " + tx.msg);
		throw tx;
	} catch(const TException &tx) {
		ERROR("Caught exception: " + string(tx.what()));
		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Caught exception: " + string(tx.what());
		throw e;
	} catch(...) {
		ERROR("Caught unknown exception");
		InvalidOperation e;
		e.ec = Error::BHT_EC_CONNECT_BACKEND;
		e.msg = "Caught unknown exception";
		throw e;
	}
}

BHT_CODE_END

