#include <algorithm>
#include <stdexcept>
#include <boost/cast.hpp>
#include <thrift/concurrency/Mutex.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "gen-cpp/BHT.h"
#include "BHTSocket.h"
#include "ClientImpl.h"
#include "Util.h"
#include "Validator.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift;
using namespace ::apache::thrift::concurrency;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

ClientImpl::ClientImpl(const string &domain, const string &host, int port,
		int conn_timeout, int send_timeout, int recv_timeout)
	:_host(host), _port(port),
	_conn_timeout(conn_timeout),
	_send_timeout(send_timeout),
	_recv_timeout(recv_timeout)
{
	if(!Validator::isValidDomain(domain)) {
		// 给定的 domain 字符串不合法，抛出异常
		throw runtime_error("BHT Client: invalid domain");
	}
	_domain = domain;
}

ClientImpl::~ClientImpl()
{
	// 析构函数不能抛出任何异常！
	try {

		close();

	} catch(...) {
	}
}

void ClientImpl::open()
{
	Guard guard(_mtx);

	if(!(_domain.size())) {
		throw runtime_error("BHT Client: domain not given");
	}

	try {

		shared_ptr<TTransport> socket(new BHTSocket(_host, _port,
					_conn_timeout, _send_timeout, _recv_timeout));
		_transport = shared_ptr<TTransport>(new TFramedTransport(socket));

		shared_ptr<TProtocol> protocol(new TBinaryProtocol(_transport));
		_client = shared_ptr<BHTClient>(new BHTClient(protocol));

		_transport->open();

	} catch(const InvalidOperation &tx) {
		throw runtime_error("BHT Client: invalid operation: " + tx.msg);
	} catch(const TException &tx) {
		throw runtime_error("BHT Client: thrift exception: " + string(tx.what()));
	} catch(...) {
		throw runtime_error("BHT Client: unknown error");
	}
}

void ClientImpl::close()
{
	Guard guard(_mtx);

	try {

		if(_client) {
			_client.reset((BHTClient*)0);
		}

		if(_transport) {
			_transport->close();
			_transport.reset((TTransport*)0);
		}

	} catch(const InvalidOperation &tx) {
		throw runtime_error("BHT Client: invalid operation: " + tx.msg);
	} catch(const TException &tx) {
		throw runtime_error("BHT Client: thrift exception: " + string(tx.what()));
	} catch(...) {
		throw runtime_error("BHT Client: unknown error");
	}
}

bool ClientImpl::get(const Client::key_type &k, Client::val_type *v)
{
	Guard guard(_mtx);

	if(!_client) {
		throw runtime_error("BHT Client: connection had been explicitly closed");
	}

	if(!ClientValidator<Client::key_type>::is_valid(k)) {
		throw runtime_error("BHT Client: invalid key");
	}

	Key nk = Converter<Client::key_type, Key>::convert(k);
	Val nv;

	try {
		ping();	// 修复可能的连接中断问题

		_client->Get(nv, _domain, nk);
	} catch(const InvalidOperation &tx) {
		throw runtime_error("BHT Client: invalid operation: " + tx.msg);
	} catch(const TException &tx) {
		throw runtime_error("BHT Client: thrift exception: " + string(tx.what()));
	} catch(...) {
		throw runtime_error("BHT Client: unknown error");
	}

	if(nv.__isset.value) {
		*v = Converter<Val, Client::val_type>::convert(nv);
		return true;
	}

	return false;
}

void ClientImpl::set(const Client::key_type &k, const Client::val_type &v)
{
	Guard guard(_mtx);

	if(!_client) {
		throw runtime_error("BHT Client: connection had been explicitly closed");
	}

	if(!ClientValidator<Client::key_type>::is_valid(k)) {
		throw runtime_error("BHT Client: invalid key");
	}

	Key nk = Converter<Client::key_type, Key>::convert(k);
	Val nv = Converter<Client::val_type, Val>::convert(v);

	try {
		ping();	// 修复可能的连接中断问题

		_client->Set(_domain, nk, nv, true/*overwrite*/, true/*relay*/);
	} catch(const InvalidOperation &tx) {
		throw runtime_error("BHT Client: invalid operation: " + tx.msg);
	} catch(const TException &tx) {
		throw runtime_error("BHT Client: thrift exception: " + string(tx.what()));
	} catch(...) {
		throw runtime_error("BHT Client: unknown error");
	}
}

void ClientImpl::del(const Client::key_type &k)
{
	Guard guard(_mtx);

	if(!_client) {
		throw runtime_error("BHT Client: connection had been explicitly closed");
	}

	if(!ClientValidator<Client::key_type>::is_valid(k)) {
		throw runtime_error("BHT Client: invalid key");
	}

	Key nk = Converter<Client::key_type, Key>::convert(k);

	try {
		ping();	// 修复可能的连接中断问题

		_client->Del(_domain, nk, true/*overwrite*/, true/*relay*/);
	} catch(const InvalidOperation &tx) {
		throw runtime_error("BHT Client: invalid operation: " + tx.msg);
	} catch(const TException &tx) {
		throw runtime_error("BHT Client: thrift exception: " + string(tx.what()));
	} catch(...) {
		throw runtime_error("BHT Client: unknown error");
	}
}

void ClientImpl::mget(const vector<Client::key_type> &ks, map<Client::key_type, Client::val_type> *ret)
{
	Guard guard(_mtx);

	if(!_client) {
		throw runtime_error("BHT Client: connection had been explicitly closed");
	}

	if(find_if(ks.begin(), ks.end(), not1(ClientValidator<Client::key_type>())) != ks.end()) {
		throw runtime_error("BHT Client: invalid key");
	}

	vector<Key> nks;
	map<Key, Val> nkvs;

	transform(ks.begin(), ks.end(), back_inserter(nks), Converter<Client::key_type, Key>());

	try {
		ping();	// 修复可能的连接中断问题

		_client->MGet(nkvs, _domain, nks);
	} catch(const InvalidOperation &tx) {
		throw runtime_error("BHT Client: invalid operation: " + tx.msg);
	} catch(const TException &tx) {
		throw runtime_error("BHT Client: thrift exception: " + string(tx.what()));
	} catch(...) {
		throw runtime_error("BHT Client: unknown error");
	}

	ret->clear();

	transform(nkvs.begin(), nkvs.end(), inserter(*ret, ret->begin()),
			Converter< pair<Key, Val>, pair<Client::key_type, Client::val_type> >());
}

void ClientImpl::mset(const map<Client::key_type, Client::val_type> &kvs)
{
	Guard guard(_mtx);

	if(!_client) {
		throw runtime_error("BHT Client: connection had been explicitly closed");
	}

	if(find_if(kvs.begin(), kvs.end(),
		not1(ClientValidator< pair<Client::key_type, Client::val_type> >())) != kvs.end()) {
		throw runtime_error("BHT Client: invalid key");
	}

	map<Key, Val> nkvs;

	transform(kvs.begin(), kvs.end(), inserter(nkvs, nkvs.begin()),
			Converter< pair<Client::key_type, Client::val_type>, pair<Key, Val> >());

	try {
		ping();	// 修复可能的连接中断问题

		_client->MSet(_domain, nkvs, true/*overwrite*/, true/*relay*/);
	} catch(const InvalidOperation &tx) {
		throw runtime_error("BHT Client: invalid operation: " + tx.msg);
	} catch(const TException &tx) {
		throw runtime_error("BHT Client: thrift exception: " + string(tx.what()));
	} catch(...) {
		throw runtime_error("BHT Client: unknown error");
	}
}

void ClientImpl::mdel(const vector<Client::key_type> &ks)
{
	Guard guard(_mtx);

	if(!_client) {
		throw runtime_error("BHT Client: connection had been explicitly closed");
	}

	if(find_if(ks.begin(), ks.end(), not1(ClientValidator<Client::key_type>())) != ks.end()) {
		throw runtime_error("BHT Client: invalid key");
	}

	vector<Key> nks;

	transform(ks.begin(), ks.end(), back_inserter(nks), Converter<Client::key_type, Key>());

	try {
		ping();	// 修复可能的连接中断问题

		_client->MDel(_domain, nks, true/*overwrite*/, true/*relay*/);
	} catch(const InvalidOperation &tx) {
		throw runtime_error("BHT Client: invalid operation: " + tx.msg);
	} catch(const TException &tx) {
		throw runtime_error("BHT Client: thrift exception: " + string(tx.what()));
	} catch(...) {
		throw runtime_error("BHT Client: unknown error");
	}
}

void ClientImpl::ping()
{
	if(!_client) {
		throw runtime_error("BHT Client: connection had been explicitly closed");
	}

	TFramedTransport *ft = polymorphic_cast<TFramedTransport*>(_transport.get());
	BHTSocket *sock = polymorphic_cast<BHTSocket*>(ft->getUnderlyingTransport().get());

	if(!sock->ping()) {
		// 连接意外中断，尝试重连
		_transport->close();
		_transport->open();	// 这里允许其抛出任何错误异常
	}
}

BHT_CODE_END

