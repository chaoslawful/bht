/*
 * =====================================================================================
 *
 *       Filename:  Target.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/27/2009 04:29:13 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_TARGET_H
#define BHT_RELAY_TARGET_H

#include "Common.h"
#include "gen-cpp/Remote.h"
#include "gen-cpp/BHT.h"
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

BHT_RELAY_BEGIN

class Operation;

class Target
{
public:
	typedef boost::shared_ptr<Target> Ptr;
	Target(const std::string& host, uint32_t port): host_(host), port_(port) {}
	virtual ~Target() {}

	virtual bool Open() = 0;
	virtual void Close() = 0;
	virtual bool IsOpen() const = 0;
	virtual bool Relay(boost::shared_ptr<const Operation> op) = 0;

	const std::string& host() const { return host_; }
	const uint32_t& port() const { return port_; }
protected:
	const std::string host_;
	const uint32_t port_;
};

class RemoteTarget : public Target, public boost::enable_shared_from_this<RemoteTarget>
{
public:
	typedef boost::shared_ptr<RemoteTarget> Ptr;
	RemoteTarget(const std::string& host, uint32_t port): Target(host, port) {}

	bool Open();
	void Close();
	bool IsOpen() const;
	bool Relay(boost::shared_ptr<const Operation> op);

	bool DoRelay(const std::string& key, const std::string& val);
private:
	boost::shared_ptr<RemoteClient> remote_;
};

class BHTTarget : public Target, public boost::enable_shared_from_this<BHTTarget>
{
public:
	typedef boost::shared_ptr<BHTTarget> Ptr;
	BHTTarget(const std::string& host, uint32_t port): Target(host, port) {}
	
	bool Open();
	void Close();
	bool IsOpen() const;
	bool Relay(boost::shared_ptr<const Operation> op);

	bool RelaySet(const std::string& domain, const BHT::Key& k, const BHT::Val& v);
	bool RelayDel(const std::string& domain, const BHT::Key& k);
	bool RelayMSet(const std::string& domain, const std::map<BHT::Key, BHT::Val>& kvs);
	bool RelayMDel(const std::string& domain, const std::vector<BHT::Key>& ks);
private:
	boost::shared_ptr<BHT::BHTClient> bht_;
};

BHT_RELAY_END

#endif // BHT_RELAY_TARGET_H

