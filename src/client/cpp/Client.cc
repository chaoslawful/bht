#include <boost/lexical_cast.hpp>
#include "Client.h"
#include "ClientImpl.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;

Client::Client(const string &domain, const string &host, int port,
		int conn_timeout, int send_timeout, int recv_timeout)
	:_impl(new ClientImpl(domain, host, port, conn_timeout, send_timeout, recv_timeout))
{
}

Client::Client(const Client &src)
	:_impl(src._impl)
{
}

const Client& Client::operator=(const Client &src)
{
	_impl = src._impl;
	return *this;
}

Client::~Client()
{
}

const string& Client::domain() const
{
	return _impl->domain();
}

const string& Client::host() const
{
	return _impl->host();
}

int Client::port() const
{
	return _impl->port();
}

void Client::open() const
{
	_impl->open();
}

void Client::close() const
{
	_impl->close();
}

bool Client::get(const key_type &k, val_type *v) const
{
	return _impl->get(k, v);
}

void Client::set(const key_type &k, const val_type &v) const
{
	_impl->set(k, v);
}

void Client::del(const key_type &k) const
{
	_impl->del(k);
}

void Client::mget(const vector<key_type> &ks, map<key_type, val_type> *ret) const
{
	_impl->mget(ks, ret);
}

void Client::mset(const map<key_type, val_type> &kvs) const
{
	_impl->mset(kvs);
}

void Client::mdel(const vector<key_type> &ks) const
{
	_impl->mdel(ks);
}

BHT_CODE_END

