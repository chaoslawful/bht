/*
 * =====================================================================================
 *
 *       Filename:  Target.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/27/2009 04:37:57 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Target.h"
#include "Logger.h"
#include "Operation.h"
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

BHT_RELAY_BEGIN

DECLARE_LOGGER("BHT.Relay.Target");

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

bool RemoteTarget::Open()
{
	ScopedLoggingCtx lc("<RemoteTarget::Open>");
	if (IsOpen()) {
		return true;
	}
	shared_ptr<TTransport> sock(new TSocket(host_, port_));
	shared_ptr<TTransport> transport(new TFramedTransport(sock));
	try {
		transport->open();
	} catch(TTransportException& e) {
		ERROR("Open Remote socket failed - host:" << host_ << " port:" << port_);
		return false;
	}
	shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	remote_.reset(new RemoteClient(protocol));
	return true;
}

void RemoteTarget::Close()
{
	if (remote_) {
		remote_.reset();
	}
}

bool RemoteTarget::IsOpen() const
{
	return remote_.get() != 0;
}

bool RemoteTarget::Relay(boost::shared_ptr<const Operation> op)
{
	ScopedLoggingCtx lc("<RemoteTarget::Relay>");
	if (!IsOpen() && !Open()) {
		ERROR("RemoteTarget::Relay - open failed");
		return false;
	}
	return op->RelayToRemote(shared_from_this());
}

bool RemoteTarget::DoRelay(const std::string& key, const std::string& val)
{
	ScopedLoggingCtx lc("<RemoteTarget::DoRelay>");
	try {
		remote_->Relay(key, val);
		return true;
	} catch (apache::thrift::TException& e) {
		ERROR("Relay to remote failed. mesage: " << e.what());
		return false;
	}
}

bool BHTTarget::Open()
{
	ScopedLoggingCtx lc("<BHTTarget::Open>");
	if (IsOpen()) {
		return true;
	}
	shared_ptr<TTransport> sock(new TSocket(host_, port_));
	shared_ptr<TTransport> transport(new TFramedTransport(sock));
	try {
		transport->open();
		INFO("BHTTarget::Open() - connect bht interface ok");
	} catch(TTransportException& e) {
		ERROR("Open Remote socket failed - host:" << host_ << " port:" << port_);
		return false;
	}
	shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	bht_.reset(new BHT::BHTClient(protocol));
	return true;
}

void BHTTarget::Close()
{
	if (bht_) {
		bht_.reset();
	}
}

bool BHTTarget::IsOpen() const
{
	return bht_.get() != 0;
}

bool BHTTarget::Relay(boost::shared_ptr<const Operation> op)
{
	ScopedLoggingCtx lc("<BHTTarget::Relay>");
	if (!IsOpen() && !Open()) {
		ERROR("BHTTarget::Relay - open failed");
		return false;
	}
	return op->RelayToBHT(shared_from_this());
}

bool BHTTarget::RelaySet(const std::string& domain, const BHT::Key& k, const BHT::Val& v)
{
	ScopedLoggingCtx lc("<BHTTarget::RelaySet>");
	try {
		bht_->Set(domain, k, v, true/*overwrite*/, false/*relay*/);
		TRACE("BHTTarget::RelaySet() - relay ok");
		return true;
	} catch (apache::thrift::TException& e) {
		ERROR("Relay set op to bht failed. mesage: " << e.what());
		return false;
	}
}
bool BHTTarget::RelayDel(const std::string& domain, const BHT::Key& k)
{
	ScopedLoggingCtx lc("<BHTTarget::RelayDel>");
	try {
		bht_->Del(domain, k, true/*overwrite*/, false/*relay*/);
		TRACE("BHTTarget::RelayDel() - relay ok");
		return true;
	} catch (apache::thrift::TException& e) {
		ERROR("Relay del op to bht failed. message: "<<e.what());
		return false;
	}
}
bool BHTTarget::RelayMSet(const std::string& domain, const std::map<BHT::Key, BHT::Val>& kvs)
{
	ScopedLoggingCtx lc("<BHTTarget::RelayMSet>");
	try {
		bht_->MSet(domain, kvs, true/*overwrite*/, false/*relay*/);
		TRACE("BHTTarget::RelayMSet() - relay ok");
		return true;
	} catch (apache::thrift::TException& e) {
		ERROR("Relay mset op to bht failed. message: " <<e.what());
		return false;
	}
}
bool BHTTarget::RelayMDel(const std::string& domain, const std::vector<BHT::Key>& ks) 
{
	ScopedLoggingCtx lc("<BHTTarget::RelayMDel>");
	try {
		bht_->MDel(domain, ks, true/*overwrite*/, false/*relay*/);
		TRACE("BHTTarget::RelayMDel() - relay ok");
		return true;
	} catch (apache::thrift::TException& e) {
		ERROR("Relay mdel op to bht failed. message: " <<e.what());
		return false;
	}
}

BHT_RELAY_END

