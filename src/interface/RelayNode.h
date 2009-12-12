#ifndef BHT_RELAYNODE_H__
#define BHT_RELAYNODE_H__

#include "Node.h"

BHT_CODE_BEGIN

class RelayNode:virtual public Node {
public:
	RelayNode(::boost::shared_ptr<BackendPool> pool, uint32_t id, ::std::vector< ::std::pair< ::std::string/* host */, int/* port*/> > &hosts);
	~RelayNode();

	// Relay 服务交互方法
	void onSet(const ::std::string &domain, const Key &k, const Val &v);
	void onDel(const ::std::string &domain, const Key &k);
	void onMSet(const ::std::string &domain, const ::std::map<Key, Val> &kvs);
	void onMDel(const ::std::string &domain, const ::std::vector<Key> &ks);
};

BHT_CODE_END

#endif

