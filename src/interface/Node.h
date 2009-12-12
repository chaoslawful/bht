#ifndef BHT_NODE_H__
#define BHT_NODE_H__

#include "Common.h"
#include "InternalKey.h"
#include "BackendPool.h"

BHT_CODE_BEGIN

class Node {
public:
	Node(::boost::shared_ptr<BackendPool> pool, uint32_t id, ::std::vector< ::std::pair< ::std::string/* host*/, int/* port*/> > &hosts);
	virtual ~Node();

	/**
	 * 从 Node 对应的后端 TokyoTyrant 服务获取指定 key 对应的 val。这里的 val 是去除值的首字节后剩余
	 * 的数据，首字节在 BHT 中作为记录删除与否的标志，为 0 表示记录已被删除，为 1 表示记录存在。
	 * @param key 待获取记录对应的 key 对象
	 * @param val 保存获取到 val 的 string 对象指针
	 * @retval 获取成功时返回 true；记录不存在时返回 false；获取出错时尝试其他备用主机，尝试完毕时抛出最终的 InvalidOperation 异常
	 * */
	bool get(const InternalKey &key, ::std::string *val);

	/**
	 * 从 Node 对应的后端存储服务中获取指定 key 对应的 val。
	 * @param kvs 作为输入时含有待获取记录对应的 key 对象，获取成功的 key 对应的标志位为 true 且含有
	 * 对应 val；获取失败的 key 对应的标志位为 false。
	 * @retval 获取出错时尝试其他备用主机，尝试完毕时抛出最终的 InvalidOperation 异常。
	 **/
	void mget(::std::map<InternalKey*, ::std::pair<bool, ::std::string> > *kvs);

	/**
	 * 将指定的 key/val 对设置到 Node 对应的后端 TokyoTyrant 服务中。实际保存的 val 要在给定值前加入
	 * 标志字节 1，表示该记录存在。
	 * @param key 待保存记录对应的 key 对象
	 * @param val 待保存记录对应值的 string 对象
	 * @param overwrite 若已存在相同 key 的记录是否覆盖，默认为 true，即覆盖已有记录
	 * @param timeout 记录超时时间(s)
	 * @retval 设置出错时尝试其他备用主机，尝试完毕时抛出最终的 InvalidOperation 异常。
	 * */
	void set(const InternalKey &key, const ::std::string &val, bool overwrite = true, int timeout = 0);

	/**
	 * 从 Node 对应的后端 TokyoTyrant 服务中逻辑删除指定 key 对应记录（即插入一条 value
	 * 首字节为 0 的记录）。
	 * @param key 待逻辑删除记录对应的 key 对象
	 * @param overwrite 逻辑删除记录同已有记录 key 重复时是否覆盖已有记录
	 * @retval 删除出错时尝试其他备用主机，尝试完毕时抛出最终的 InvalidOperation 异常。
	 * */
	void out(const InternalKey &key, bool overwrite = true);

	/**
	 * 从 Node 对应的后端 TokyoTyrant 服务中物理删除指定 key 对应记录
	 * @param key 待删除记录对应的 key 对象
	 * @retval 删除出错时尝试其他备用主机，尝试完毕时抛出最终的 InvalidOperation 异常。
	 * */
	void del(const InternalKey &key);

	/**
	 * 获取 Node 对应的 ID
	 * */
	uint32_t id() const { return _id; }

	bool operator<(const Node& rhs) const;
	bool operator==(const Node& rhs) const;

protected:
	uint32_t _id;

	::std::vector< ::std::pair< ::std::string, int > > _node_hosts;
	::boost::shared_ptr<BackendPool> _pool;
};

BHT_CODE_END

#endif

