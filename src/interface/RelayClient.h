/**
 * 使用 Thrift 的 TNonblockingServer 作为主体服务框架时，由于多个 worker 以 thread 方式存在，
 * RelayClient 作为公用资源应使用 rwlock 进行互斥保护，防止出现竞态条件。
 * */
#ifndef BHT_RELAYCLIENT_H__
#define BHT_RELAYCLIENT_H__

#include "Common.h"
#include "InternalKey.h"

BHT_CODE_BEGIN

class RelayClient {
public:
	static RelayClient& getInstance();
	static void destroyInstance();

	void onSet(const ::std::string &domain, const Key &k, const Val &v);
	void onDel(const ::std::string &domain, const Key &k);
	void onMSet(const ::std::string &domain, const ::std::map<Key, Val> &kvs);
	void onMDel(const ::std::string &domain, const ::std::vector<Key> &ks);

	~RelayClient() {}

private:
	RelayClient() {}

	static RelayClient *_instance;
};

BHT_CODE_END

#endif

