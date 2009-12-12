/*
 * =====================================================================================
 *
 *       Filename:  Operation.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/08/2009 12:23:33 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */
#ifndef BHT_RELAY_OPERATION_H
#define BHT_RELAY_OPERATION_H

#include <string>
#include <vector>
#include <map>
#include <boost/assert.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include "Common.h"
#include "Target.h"

BHT_RELAY_BEGIN

/** 
 * @brief Operation代表一个需要进行同步的操作
 */
class Operation : public boost::enable_shared_from_this<Operation>
{
public:
	Operation(uint64_t timestamp, const std::string& domain);
	virtual ~Operation() {}

	typedef boost::shared_ptr<Operation> Ptr;

	typedef enum {
		OP_SET  = 1,
		OP_DEL  = 2,
		OP_MSET = 3,
		OP_MDEL = 4,
	} Type;
public:
	/** 
	 * @brief 将操作序列化为tt中存储的序列
	 * 
	 * @param os 输出流
	 */
	void Serialize(std::string& skey, std::ostream& os) const;
	/** 
	 * @brief 将操作同步到目标主机(远端relay 或 本地bht集群)
	 * 
	 * @param target RemoteTarget or BHTTarget 对象
	 * 
	 * @return 操作结果
	 */
	bool Relay(Target::Ptr target) const ;
	/** 
	 * @brief 将tt中存储的串反序列化为Operation对象
	 * 
	 * @param timestamp 时戳
	 * @param data 序列串
	 * @param length 序列长度
	 * 
	 * @return Operation对象
	 */
	static Ptr Unserialize(const std::string& skey, const char* data, size_t length) {
		BOOST_ASSERT(data);
		BOOST_ASSERT(length);
		uint64_t ts;
		std::string domain;
		if (!UnserializeSKey(skey, ts, domain)) {
			return Ptr();
		}
		Type optype = (Type)(*data);
		Ptr op = Get(ts, domain, optype, data, length);
		return op;
	}

	bool RelayToRemote(RemoteTarget::Ptr remote) const;
	virtual bool RelayToBHT(BHTTarget::Ptr bht) const = 0;

	const uint64_t& getTimestamp() const { return timestamp_; }
	const std::string& getDomain() const { return domain_; }
protected:
	struct OpFactory{
		typedef boost::shared_ptr<OpFactory> Ptr;
		virtual ~OpFactory() {}
		virtual Operation::Ptr Get(const uint64_t&, const std::string&, const char*, size_t) = 0;
	};
	template <class Op>
	struct OpFactoryImpl: virtual public OpFactory {
		Operation::Ptr Get(const uint64_t& ts, const std::string& domain, const char* data, size_t length) { return Operation::Ptr(new Op(ts, domain, data, length)); }
	};
	static Ptr Get(const uint64_t& ts, const std::string& domain, Type optype, const char* data, size_t length) {
		std::map<Type, OpFactory::Ptr>::iterator pos = factories_.find(optype);
		if (pos != factories_.end()) {
			Ptr op = pos->second->Get(ts, domain, data, length);
			return op;
		} else {
			return Ptr();
		}
	}
public:
	template <class Op>
	static void AddFactory() {
		OpFactory::Ptr factory(new OpFactoryImpl<Op>);
		factories_.insert(std::make_pair(Op::type, factory));
	}
private:
	/** 
	 * @brief 序列化、反序列话tt中存储时使用的key
	 * 
	 * @param skey 
	 */
	void SerializeSKey(std::string& skey) const;
	static bool UnserializeSKey(const std::string& skey, uint64_t& ts, std::string& domain);
protected:
	/** 
	 * @brief 序列化tt中存储的value
	 * 
	 * @param os 
	 */
	virtual void SerializeSVal(std::ostream& os) const = 0;

	/** 
	 * @brief 将integer序列化
	 */
	template <typename Int>
	static inline std::string SerializeInt(Int i) {
		return std::string(reinterpret_cast<char*>(&i), sizeof(Int));
	}
protected:
	uint64_t timestamp_;
	std::string domain_;
private:
	static std::map<Type, OpFactory::Ptr> factories_;
};

class OpSet: public Operation
{
	friend class OpFactoryImpl<OpSet>;
public:
	static const Type type = OP_SET;
	OpSet(uint64_t timestamp, const std::string& domain, const std::string& k, const std::string& v);
private:
	OpSet(const uint64_t& ts, const std::string& domain, const char* data, size_t length);
	void SerializeSVal(std::ostream& os) const;

	bool RelayToBHT(BHTTarget::Ptr bht) const;
private:
	std::string k_;
	std::string v_;
};

class OpDel: public Operation
{
	friend class OpFactoryImpl<OpDel>;
public:
	static const Type type = OP_DEL;
	OpDel(uint64_t timestamp, const std::string& domain, const std::string& k);
private:
	OpDel(const uint64_t& ts, const std::string& domain, const char* data, size_t length);
	void SerializeSVal(std::ostream& os) const;

	bool RelayToBHT(BHTTarget::Ptr bht) const;
private:
	std::string k_;
};

class OpMSet: public Operation
{
	friend class OpFactoryImpl<OpMSet>;
public:
	static const Type type = OP_MSET;
	OpMSet(uint64_t timestamp, const std::string& domain, const std::map<std::string, std::string>& kvs);
private:
	OpMSet(const uint64_t& ts, const std::string& domain, const char* data, size_t length);
	void SerializeSVal(std::ostream& os) const;

	bool RelayToBHT(BHTTarget::Ptr bht) const;
private:
	std::map<std::string, std::string> kvs_;
};

class OpMDel: public Operation
{
	friend class OpFactoryImpl<OpMDel>;
public:
	static const Type type = OP_MDEL;
	OpMDel(uint64_t timestamp, const std::string& domain, const std::vector<std::string>& ks);
private:
	OpMDel(const uint64_t& ts, const std::string& domain, const char* data, size_t length);
	void SerializeSVal(std::ostream& os) const;

	bool RelayToBHT(BHTTarget::Ptr bht) const;
private:
	std::vector<std::string> ks_;
};

BHT_RELAY_END

#endif // BHT_RELAY_OPERATION_H

