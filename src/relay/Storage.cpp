/*
 * =====================================================================================
 *
 *       Filename:  Storage.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/09/2009 12:18:44 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Storage.h"
#include "Logger.h"
#include <stdexcept>

BHT_RELAY_BEGIN

DECLARE_LOGGER("BHT.Relay.Storage");

Storage::iterator::iterator()
	: rdb_(0)
	, ok_(false)
{
}

Storage::iterator::iterator(TCRDB* rdb)
	: rdb_(rdb)
	, ok_(true)
{
	ScopedLoggingCtx lc("<Storage::iterator::iterator>");
	if (!tcrdbiterinit(rdb_)) {
		int ecode = tcrdbecode(rdb_);
		ERROR("Storage::iterator::iterator() - tcrdb iter init failed: "<< tcrdberrmsg(ecode));
		ok_ = false;
		return;
	}
	operator++();
}

const Storage::iterator::value_type& Storage::iterator::operator*() const
{
	if (!ok_) {
		throw std::logic_error("invalid storage iterator");
	}
	return val_;
}

Storage::iterator& Storage::iterator::operator++() {
	if (!ok_) {
		throw std::logic_error("invalid storage iterator");
	}
	int sp = 0;
	void* p = tcrdbiternext(rdb_, &sp);
	if (!p) {
		ok_ = false;
	} else {
		val_.assign(static_cast<const char*>(p), static_cast<size_t>(sp));
		free(p);
	}
	return *this;
}

Storage::Storage(const std::string& host, uint32_t port)
	: host_(host)
	, port_(port)
	, rdb_(NULL)
{
}

Storage::~Storage()
{
	Close();
}

bool Storage::Open()
{
	ScopedLoggingCtx lc("<Storage::Open>");
	if (rdb_) {
		return true;
	}
	rdb_ = tcrdbnew();
	if (tcrdbopen(rdb_, host_.c_str(), static_cast<int>(port_))) {
		DEBUG("Storage::Open() - tcrdb connected");
		return true;
	} else {
		int ecode = tcrdbecode(rdb_);
		ERROR("Storage::Open() - tcrdb connect failed: " << tcrdberrmsg(ecode));
		tcrdbdel(rdb_);
		rdb_ = NULL;
		return false;
	}
}

void Storage::Close()
{
	if (rdb_) {
		tcrdbclose(rdb_);
		tcrdbdel(rdb_);
		rdb_ = NULL;
	}
}

boost::shared_ptr<char> Storage::Get(const std::string& skey, size_t& vsiz)
{
	ScopedLoggingCtx lc("<Storage::Get>");
	BOOST_ASSERT(rdb_);
	int sp=0;
	char* p = (char*)tcrdbget(rdb_, skey.data(), skey.size(), &sp);
	if (p) {
		vsiz = static_cast<size_t>(sp);
		return boost::shared_ptr<char>(p, reinterpret_cast<void (*)(void*) throw()>(&free));
	} else {
		int ecode = tcrdbecode(rdb_);
		ERROR("Storage::Get() - tcrdbget failed: " << tcrdberrmsg(ecode));
		return boost::shared_ptr<char>();
	}
}


BHT_RELAY_END

