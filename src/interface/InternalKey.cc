#include "Logging.h"
#include "InternalKey.h"

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

BHT_CODE_BEGIN

using namespace ::std;

BHT_INIT_LOGGING("BHT.Interface.InternalKey")

PartitionKey::PartitionKey(const string &domain, const string &key)
{
	_serialized = "p\001";
	_serialized.append(domain);
	_serialized.append("\001");
	_serialized.append(key);
}

StorageKey::StorageKey(const string &domain, const string &key, const string &subkey)
{
	_serialized = "s\001";
	_serialized.append(domain);
	_serialized.append("\001");
	_serialized.append(key);
	_serialized.append("\001");
	_serialized.append(subkey);
	_serialized.append("\001");
}

StorageKey::StorageKey(const string &domain, const string &key, const string &subkey, const int64_t &timestamp)
{
	uint64_t ts = (uint64_t)(-timestamp);
	ts = SWAB64(ts); 	// 将 64-bit 时戳转换为 big-endian 形式
	_serialized = "s\001";
	_serialized.append(domain);
	_serialized.append("\001");
	_serialized.append(key);
	_serialized.append("\001");
	_serialized.append(subkey);
	_serialized.append("\001");
	_serialized.append((const char*)&ts, sizeof(ts));
}

BHT_CODE_END

