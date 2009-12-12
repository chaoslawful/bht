/*
 * =====================================================================================
 *
 *       Filename:  Operation.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/08/2009 01:19:19 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Operation.h"
#include "Framework.h"
#include "Logger.h"
#include "InternalKey.h"
#include <boost/scoped_array.hpp>
#include <boost/range/iterator_range.hpp>

BHT_RELAY_BEGIN

DECLARE_LOGGER("BHT.Relay.Operation");

using namespace std;
using namespace boost;

std::map<Operation::Type, Operation::OpFactory::Ptr> Operation::factories_;

Operation::Operation(uint64_t timestamp, const string& domain)
	: timestamp_(timestamp)
	, domain_(domain)
{
}

void Operation::Serialize(std::string& skey, std::ostream& os) const
{
	SerializeSKey(skey);
	SerializeSVal(os);
}

void Operation::SerializeSKey(std::string& skey) const
{
	skey.assign(reinterpret_cast<const char*>(&timestamp_), sizeof(uint64_t));
	skey.append(domain_);
}

bool Operation::UnserializeSKey(const std::string& skey, uint64_t& ts, string& domain)
{
	if (skey.size() <= sizeof(uint64_t)) {
		return false;
	}
	ts = *reinterpret_cast<const uint64_t*>(skey.data());
	domain.assign(skey.substr(sizeof(uint64_t)));
	return true;
}

bool Operation::Relay(Target::Ptr target) const
{
	return target->Relay(shared_from_this());
}

bool Operation::RelayToRemote(RemoteTarget::Ptr remote) const
{
	string key;
	stringstream val_stream;
	this->Serialize(key, val_stream);
	string val = val_stream.str();
	boost::scoped_array<char> encrypted_key(new char[key.size()]);
	boost::scoped_array<char> encrypted_val(new char[val.size()]);
	Framework::getCryptor()->Encrypt(key.data(), encrypted_key.get(), key.size());
	Framework::getCryptor()->Encrypt(val.data(), encrypted_val.get(), val.size());
	return remote->DoRelay(string(encrypted_key.get(), key.size()), string(encrypted_val.get(), val.size()));
}

OpSet::OpSet(uint64_t timestamp, const std::string& domain, const std::string& k, const std::string& v)
	: Operation(timestamp, domain)
	, k_(k)
	, v_(v)
{
}

OpSet::OpSet(const uint64_t& ts, const std::string& domain, const char* data, size_t length)
	: Operation(ts, domain)
{
	const char * val = data;
	//BOOST_ASSERT(static_cast<Type>(*val) == type);
	val += sizeof(uint8_t);
	uint16_t ksiz = ntohs(*reinterpret_cast<const uint16_t*>(val));
	val += sizeof(uint16_t);
	k_.assign(val, ksiz);
	val += ksiz;
	uint32_t vsiz = htonl(*reinterpret_cast<const uint32_t*>(val));
	val += sizeof(uint32_t);
	v_.assign(val, vsiz);
	//BOOST_ASSERT((val+vsiz) == (data + length));
}

void OpSet::SerializeSVal(ostream& os) const
{
	os	<< SerializeInt<uint8_t>(type)
		<< SerializeInt<uint16_t>(htons(k_.size()))
		<< k_ 
		<< SerializeInt<uint32_t>(htonl(v_.size()))
		<< v_
	;
}

bool OpSet::RelayToBHT(BHTTarget::Ptr bht) const
{
	BHT::Key k = InternalKey::FromString(k_);
	BHT::Val v;
	v.value = v_;
	v.__isset.value = true;
	return bht->RelaySet(domain_, k, v);
}

OpDel::OpDel(uint64_t timestamp, const std::string& domain, const std::string& k)
	: Operation(timestamp, domain)
	, k_(k)
{
}

OpDel::OpDel(const uint64_t& ts, const std::string& domain, const char* data, size_t length)
	: Operation(ts, domain)
{
	const char* val = data;
	//BOOST_ASSERT(static_cast<Type>(*val) == type);
	val += sizeof(uint8_t);
	uint16_t ksiz = ntohs(*reinterpret_cast<const uint16_t*>(val));
	val += sizeof(uint16_t);
	k_.assign(val, ksiz);
	//BOOST_ASSERT((val+ksiz) == (data + length));
}

void OpDel::SerializeSVal(ostream& os) const
{
	os	<< SerializeInt<uint8_t>(type)
		<< SerializeInt<uint16_t>(htons(k_.size()))
		<< k_
	;
}

bool OpDel::RelayToBHT(BHTTarget::Ptr bht) const
{
	BHT::Key k = InternalKey::FromString(k_);
	return bht->RelayDel(domain_, k);
}

OpMSet::OpMSet(uint64_t timestamp, const std::string& domain, const std::map<std::string, std::string>& kvs)
	: Operation(timestamp, domain)
	, kvs_(kvs)
{
}

OpMSet::OpMSet(const uint64_t& ts, const std::string& domain, const char* data, size_t length)
	: Operation(ts, domain)
{
	const char* val = data;
	//BOOST_ASSERT(static_cast<Type>(*val) == type);
	val += sizeof(uint8_t);
	uint32_t count = ntohl(*reinterpret_cast<const uint32_t*>(val));
	val += sizeof(uint32_t);
	//BOOST_ASSERT(count > 0);
	for (uint32_t i=0; i<count; ++i) {
		uint16_t ksiz = ntohs(*reinterpret_cast<const uint16_t*>(val));
		val += sizeof(uint16_t);
		boost::iterator_range<const char*> krange(val, val+ksiz);
		val += ksiz;
		uint32_t vsiz = ntohl(*reinterpret_cast<const uint32_t*>(val));
		val += sizeof(uint32_t);
		boost::iterator_range<const char*> vrange(val, val+vsiz);
		val += vsiz;
		kvs_.insert(
			std::make_pair(
				boost::copy_range<std::string>(krange),
				boost::copy_range<std::string>(vrange)
			)
		);
	}
	//BOOST_ASSERT(val == (data + length));
}

void OpMSet::SerializeSVal(ostream& os) const
{
	os	<< SerializeInt<uint8_t>(type)
		<< SerializeInt<uint32_t>(htonl(kvs_.size()))
	;
	std::map<std::string, std::string>::const_iterator it = kvs_.begin(), end = kvs_.end();
	for (; it!=end; ++it) {
		os	<< SerializeInt<uint16_t>(htons(it->first.size()))
			<< it->first
			<< SerializeInt<uint32_t>(htonl(it->second.size()))
			<< it->second
		;
	}
}

bool OpMSet::RelayToBHT(BHTTarget::Ptr bht) const
{
	map<BHT::Key, BHT::Val> kvs;
	map<string, string>::const_iterator it = kvs_.begin(), end = kvs_.end();
	for (; it!=end; ++it) {
		BHT::Key k = InternalKey::FromString(it->first);
		BHT::Val v;
		v.value = it->second;
		v.__isset.value = true;
		kvs.insert(make_pair(k, v));
	}
	return bht->RelayMSet(domain_, kvs);
}

OpMDel::OpMDel(uint64_t timestamp, const std::string& domain, const std::vector<std::string>& ks)
	: Operation(timestamp, domain)
	, ks_(ks)
{
}

OpMDel::OpMDel(const uint64_t& ts, const std::string& domain, const char* data, size_t length)
	: Operation(ts, domain)
{
	const char* val = data;
	//BOOST_ASSERT(static_cast<Type>(*val) == type);
	val += sizeof(uint8_t);
	uint32_t count = ntohl(*reinterpret_cast<const uint32_t*>(val));
	val += sizeof(uint32_t);
	//BOOST_ASSERT(count > 0);
	for (uint32_t i=0; i<count; ++i) {
		uint16_t ksiz = ntohs(*reinterpret_cast<const uint16_t*>(val));
		val += sizeof(uint16_t);
		boost::iterator_range<const char*> krange(val, val+ksiz);
		val += ksiz;
		ks_.push_back(boost::copy_range<std::string>(krange));
	}
	//BOOST_ASSERT(val == (data + length));
}

void OpMDel::SerializeSVal(ostream& os) const
{
	os	<< SerializeInt<uint8_t>(type)
		<< SerializeInt<uint32_t>(htonl(ks_.size()))
	;
	std::vector<std::string>::const_iterator it = ks_.begin(), end = ks_.end();
	for (; it!=end; ++it) {
		os	<< SerializeInt<uint16_t>(htons(it->size()))
			<< *it
		;
	}
}

bool OpMDel::RelayToBHT(BHTTarget::Ptr bht) const
{
	vector<BHT::Key> ks;
	vector<string>::const_iterator  it = ks_.begin(), end = ks_.end();
	for (; it!=end; ++it) {
		BHT::Key k = InternalKey::FromString(*it);
		ks.push_back(k);
	}
	return bht->RelayMDel(domain_, ks);
}

BHT_RELAY_END

