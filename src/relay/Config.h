/*
 * =====================================================================================
 *
 *       Filename:  Config.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/15/2009 06:07:04 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_CONFIG_H
#define BHT_RELAY_CONFIG_H

#include "Common.h"
#include "Singleton.hpp"
#include "Mode.h"
#include <string>
#include <vector>

BHT_RELAY_BEGIN

class Config
{
	friend class Singleton<Config>;
	explicit Config(const std::string& config_file);
public:
	typedef boost::shared_ptr<Config> Ptr;
	typedef std::pair<std::string, uint32_t> AddrType;

	void Configure();
public:
	const std::string& LoggerConfig() const { return logger_config_; }
	const AddrType& StorageAddr() const { return storage_addr_; }
	const std::vector<AddrType>& TargetAddrs() const { return target_addrs_; }
	const std::string& SecretKey() const { return secret_key_; }
	uint32_t ServicePort() const { return service_port_; }
	size_t ServiceThreads() const { return service_threads_; }
	size_t WorkerThreads() const { return worker_threads_; }
	size_t WorkerSleeps() const { return worker_sleeps_; }
private:
	std::string config_file_;
	// logger conf
	std::string logger_config_;
	// storage conf
	AddrType storage_addr_;
	// target conf
	std::vector<AddrType> target_addrs_;
	// cryptor conf
	std::string secret_key_;
	// service conf
	uint32_t service_port_;
	size_t service_threads_;
	// worker conf
	size_t worker_threads_;
	uint32_t worker_sleeps_;
};

BHT_RELAY_END

#endif // BHT_RELAY_CONFIG_H

