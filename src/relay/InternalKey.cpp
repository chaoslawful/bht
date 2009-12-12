/*
 * =====================================================================================
 *
 *       Filename:  InternalKey.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/08/2009 01:33:55 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "InternalKey.h"
#include "Logger.h"
#include <vector>
#include <boost/algorithm/string.hpp>

BHT_RELAY_BEGIN

DECLARE_LOGGER("BHT.Relay.InternalKey");

using namespace std;

#define SWAB64(num) \
	  ( \
		   ((num & 0x00000000000000ffULL) << 56) | \
		   ((num & 0x000000000000ff00ULL) << 40) | \
		   ((num & 0x0000000000ff0000ULL) << 24) | \
		   ((num & 0x00000000ff000000ULL) << 8) | \
		   ((num & 0x000000ff00000000ULL) >> 8) | \
		   ((num & 0x0000ff0000000000ULL) >> 24) | \
		   ((num & 0x00ff000000000000ULL) >> 40) | \
		   ((num & 0xff00000000000000ULL) >> 56) \
	  )

std::string InternalKey::ToString(const BHT::Key& k)
{
	ScopedLoggingCtx lc("<InternalKey::ToString>");
	string key;
	key.append(k.key);
	key.append(1, '\001');
	key.append(k.subkey);
	key.append(1, '\001');
	uint64_t ts = SWAB64(k.timestamp);
	key.append((const char*)&ts, sizeof(ts));
	DEBUG("InternalKey::ToString() - key: " << k.key << " subkey:" << k.subkey << " ts:" << k.timestamp);
	return key;
}

BHT::Key InternalKey::FromString(const std::string& k)
{
	ScopedLoggingCtx lc("<InternalKey::FromString>");
	std::vector<std::string> fields;
	boost::split(fields, k, boost::is_any_of("\001"));
	if (fields.size() != 3){
	}
	BHT::Key key;
	key.key.assign(fields[0]);
	key.subkey.assign(fields[1]);
	uint64_t ts = *reinterpret_cast<const uint64_t*>(fields[2].data());
	key.timestamp = SWAB64(ts);
	key.__isset.key = true;
	key.__isset.subkey = true;
	key.__isset.timestamp = true;
	DEBUG("InternalKey::FromString() - key: " << key.key << " subkey:" << key.subkey << " ts:" << key.timestamp);
	return key;
}

BHT_RELAY_END

