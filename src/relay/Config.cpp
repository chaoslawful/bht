/*
 * =====================================================================================
 *
 *       Filename:  Config.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/15/2009 06:12:19 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Config.h"
#include <libgen.h>
#include <fstream>
#include <iostream>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/detail/convert.hpp>
#include <boost/program_options/detail/config_file.hpp>

BHT_RELAY_BEGIN

using namespace std;
using namespace boost;

Config::Config(const std::string& config_file)
	: config_file_(config_file)
	, service_port_(0)
	, service_threads_(1)
	, worker_threads_(1)
	, worker_sleeps_(1)
{
}

void Config::Configure()
{
	std::string storage_host;
	uint32_t storage_port = 0;
	std::vector<std::string> target_hosts;
	std::vector<uint32_t> target_ports;

	set<std::string> allowed_options;
	allowed_options.insert("*");
	ifstream is(config_file_.c_str());
	if (!is) {
		cerr << "Cannot open config file : " << config_file_ <<endl;
		throw "Cannot open config file.";
	}
	program_options::detail::config_file_iterator it(is, allowed_options), end;
	for (; it!=end; ++it) {
		const std::string& key = it->string_key;
		//const vector<string>& values = it->value;
		const std::string& value = it->value[0];
		if (key == "logger.config") {
			if (value.empty()) {
				continue;
			}
			if (value[0] == '/') {
				logger_config_ = value;
			} else {
				logger_config_ = BHT_RELAY_ROOT_PATH "/" BHT_RELAY_CONF_PATH "/" + value;
			}
		} else if (key == "storage.host") {
			storage_host.assign(value);
		} else if (key == "storage.port") {
			uint32_t port = 0;
			try {
				port = boost::lexical_cast<uint32_t>(value);
			} catch(std::exception& e) {
				continue;
			}
			storage_port = port;
		} else if (key == "target.host") {
			target_hosts.push_back(value);
		} else if (key == "target.port") {
			uint32_t port = 0;
			try {
				port = boost::lexical_cast<uint32_t>(value);
				target_ports.push_back(port);
			} catch(std::exception& e) {
				continue;
			}
		} else if (key == "secret.key") {
			secret_key_.assign(value);
		} else if (key == "service.port") {
			uint32_t port = 0;
			try {
				port = boost::lexical_cast<uint32_t>(value);
			} catch(std::exception& e) {
				continue;
			}
			service_port_ = port;
		} else if (key == "service.threads") {
			size_t n = 0;
			try {
				n = boost::lexical_cast<size_t>(value);
			} catch(std::exception& e) {
				continue;
			}
			service_threads_ = n ? n : 1;
		} else if (key == "worker.threads") {
			size_t n = 0;
			try {
				n = boost::lexical_cast<size_t>(value);
			} catch(std::exception& e) {
				continue;
			}
			worker_threads_ = n ? n : 1;
		} else if (key == "worker.sleeps") {
			uint32_t n = 0;
			try {
				n = boost::lexical_cast<uint32_t>(value);
			} catch(std::exception& e) {
				continue;
			}
			worker_sleeps_ = n ? n : 1;
		} else {
			cerr << "Unkonwn config " << key << " -> "<< value << endl;
		}
	}

	// 检查配置
	if (logger_config_.empty()) {
		logger_config_ = BHT_RELAY_ROOT_PATH "/" BHT_RELAY_CONF_PATH "/" BHT_RELAY_LOGGER_CONF_FILE;
	}

	if (storage_host.empty()) {
		cerr << "Empty storage host." <<endl;
		throw "Empty storage host.";
	}
	if (storage_port == 0) {
		cerr << "Invalid storage port." << endl;
		throw "Invalid storage port.";
	}
	storage_addr_.first = storage_host;
	storage_addr_.second = storage_port;

	if (target_hosts.empty()) {
		cerr << "Empty target host." <<endl;
		throw "Empty target host.";
	}
	if (target_ports.empty()) {
		cerr << "Empty target port." <<endl;
		throw "Empty target port.";
	}
	if (target_hosts.size() != target_ports.size()) {
		cerr << "Invalid target host, port pair numbers." << endl;
		throw "Invalid target host, port pair numbers.";
	}
	for(size_t i=0; i<target_hosts.size(); ++i) {
		target_addrs_.push_back(make_pair(target_hosts[i], target_ports[i]));
	}

	if (secret_key_.empty()) {
		cerr << "Empty secret key." <<endl;
		throw "Empty secret key.";
	}

	if (service_port_ == 0) {
		cerr << "Empty service port." <<endl;
		throw "Empty service port.";
	}
}

BHT_RELAY_END


