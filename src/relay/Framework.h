/*
 * =====================================================================================
 *
 *       Filename:  Framework.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/09/2009 06:18:46 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_FRAMEWORK_H
#define BHT_RELAY_FRAMEWORK_H

#include "Common.h"
#include "WorkerMgr.h"
#include "Service.h"
#include "Cryptor.h"
#include "TargetPoolMgr.h"
#include "Mode.h"

BHT_RELAY_BEGIN

class Framework
{
public:
	static bool Init(int argc, char** argv);
	static void Cleanup();

	static bool Start();
	static void Stop();

	static Service::Ptr getService() { return service_; }
	static Cryptor::Ptr getCryptor() { return cryptor_; }
	static TargetPoolMgr::Ptr getTargetPoolMgr() { return target_pool_mgr_; }
private:
	static void ParseOpt(int argc, char** argv);
	static void Usage(const std::string& prog, const std::string& err = std::string());
	static void Daemonize();
private:
	static Cryptor::Ptr cryptor_;
	static Service::Ptr service_;
	static TargetPoolMgr::Ptr target_pool_mgr_;
private:
	static Mode::Type mode_;
	static bool daemon_;
	static std::string config_file_;
};

BHT_RELAY_END

#endif // BHT_RELAY_FRAMEWORK_H

