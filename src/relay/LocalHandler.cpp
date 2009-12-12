/*
 * =====================================================================================
 *
 *       Filename:  LocalHandler.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/14/2009 09:01:08 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "LocalHandler.h"
#include "Logger.h"
#include "ErrorCode.h"
#include "InternalKey.h"
#include "Singleton.hpp"
#include "Config.h"
#include "Storage.h"
#include "StoragePool.h"

BHT_RELAY_BEGIN

using namespace std;

DECLARE_LOGGER("BHT.Relay.LocalHandler");

void LocalHandler::OnSet(const string& domain, const Key& k, const Val& v)
{
	ScopedLoggingCtx lc("<LocalHandler::OnSet>");
	TRACE("LocalHandler::OnSet() - called");
	if (domain.empty()) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_DOMAIN;
		e.msg = "Empty domain for OnSet()";
		throw e;
	}
	if (!k.__isset.key) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_KEY;
		e.msg = "Empty key for OnSet()";
		throw e;
	}
	if (!k.__isset.timestamp) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_TIMESTAMP;
		e.msg = "Empty timestamp for OnSet()";
		throw e;
	}
	if (!v.__isset.value) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_VALUE;
		e.msg = "Empty value for OnSet()";
		throw e;
	}
	Operation::Ptr op(new OpSet(k.timestamp, domain, InternalKey::ToString(k), v.value));
	StoreOp(op);
}

void LocalHandler::OnDel(const string& domain, const Key& k)
{
	ScopedLoggingCtx lc("<LocalHandler::OnDel>");
	TRACE("LocalHandler::OnDel() - called");
	if (domain.empty()) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_DOMAIN;
		e.msg = "Empty domain for OnDel()";
		throw e;
	}
	if (!k.__isset.key) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_KEY;
		e.msg = "Empty key for OnDel()";
		throw e;
	}
	if (!k.__isset.timestamp) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_TIMESTAMP;
		e.msg = "Empty timestamp for OnDel()";
		throw e;
	}
	Operation::Ptr op(new OpDel(k.timestamp, domain, InternalKey::ToString(k)));
	StoreOp(op);
}

void LocalHandler::OnMSet(const string& domain, const std::map<Key, Val>& kvs)
{
	ScopedLoggingCtx lc("<LocalHandler::OnMSet>");
	TRACE("LocalHandler::OnMSet() - called");
	if (domain.empty()) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_DOMAIN;
		e.msg = "Empty domain for OnMSet()";
		throw e;
	}
	if (kvs.empty()) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_COUNT;
		e.msg = "Empty kvs for OnMSet()";
		throw e;
	}
	uint64_t timestamp = kvs.begin()->first.timestamp;
	std::map<std::string, std::string> kvs2;
	std::map<Key, Val>::const_iterator it = kvs.begin(), end = kvs.end();
	for(; it!=end; ++it) {
		const Key& k = it->first;
		const Val& v = it->second;
		kvs2.insert(make_pair(InternalKey::ToString(k), v.value));
	}
	Operation::Ptr op(new OpMSet(timestamp, domain, kvs2));
	StoreOp(op);
}

void LocalHandler::OnMDel(const string& domain, const std::vector<Key>& ks)
{
	ScopedLoggingCtx lc("<LocalHandler::OnMDel>");
	TRACE("LocalHandler::OnMDel() - called");
	if (domain.empty()) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_DOMAIN;
		e.msg = "Empty domain for OnMDel()";
		throw e;
	}
	if (ks.empty()) {
		InvalidOperation e;
		e.ec = ErrorCode::INVALID_COUNT;
		e.msg = "Empty ks for OnMDel()";
		throw e;
	}
	uint64_t timestamp = ks.begin()->timestamp;
	std::vector<std::string> ks2;
	std::vector<Key>::const_iterator it = ks.begin(), end = ks.end();
	for(; it!=end; ++it) {
		const Key& k = *it;
		ks2.push_back(InternalKey::ToString(k));
	}
	Operation::Ptr op(new OpMDel(timestamp, domain, ks2));
	StoreOp(op);
}

void LocalHandler::StoreOp(Operation::Ptr op)
{
	ScopedLoggingCtx lc("<LocalHandler::StoreOp>");
	std::string skey;
	std::stringstream sval;
	op->Serialize(skey, sval);

	Storage::Ptr storage = Singleton<StoragePool>::Get()->Alloc();
	storage->Open();
	if (!storage->Put(skey, sval.str())) {
		ERROR("Store operation failed: ");
		return;
	}
	Singleton<StoragePool>::Get()->Free(storage);
}

BHT_RELAY_END

