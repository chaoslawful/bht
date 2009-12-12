#ifndef BHT_INTERNAL_KEY__
#define BHT_INTERNAL_KEY__

#include <string>

#include "Common.h"

BHT_CODE_BEGIN

/**
 * 内部 key 抽象结构，要求继承类实现序列化接口
 * */
class InternalKey {
public:
	virtual ~InternalKey() {}
	virtual const ::std::string& toString() const = 0;
};

/**
 * L-node 上标识记录的 key 结构，包括 <domain, key>
 * */
class PartitionKey:virtual public InternalKey {
public:
	PartitionKey(const ::std::string &domain, const ::std::string &key);

	~PartitionKey() {}

	const ::std::string& toString() const
	{
		return _serialized;
	}

private:
	::std::string _serialized;
};

/**
 * S-node 上标识记录的 key 结构，包括 <domain, key, subkey, [timestamp]>，其中 timestamp
 * 为可选字段。
 * */
class StorageKey:virtual public InternalKey {
public:
	StorageKey(const ::std::string &domain, const ::std::string &key, const ::std::string &subkey);
	StorageKey(const ::std::string &domain, const ::std::string &key, const ::std::string &subkey, const int64_t &timestamp);

	~StorageKey() {}

	const ::std::string& toString() const
	{
		return _serialized;
	}

private:
	::std::string _serialized;
};

BHT_CODE_END

#endif

