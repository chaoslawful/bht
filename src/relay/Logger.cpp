/*
 * =====================================================================================
 *
 *       Filename:  Logger.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/15/2009 05:58:59 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Logger.h"
#include <iostream>                                                                                                                                                                                              
#include <cassert>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/xml/domconfigurator.h>
#include <log4cxx/ndc.h>
#include <sys/stat.h>

BHT_RELAY_BEGIN

using namespace std;
using namespace log4cxx;

void Logger::Init(const std::string& config_file)
{
	// 日志系统尚未初始化
	struct stat st;
	string confpath = config_file;
	if(confpath.size() == 0) {
		assert(0);
	}

	cerr << confpath << endl;
	// 若指定的日志配置文件不存在，或者存在但不是普通文件或符号链接，则抛出异常终止程序
	if(stat(confpath.c_str(), &st) || !(S_ISREG(st.st_mode) || S_ISLNK(st.st_mode))) {
		cerr << "Invalid logging confiugration file: "<<confpath<<endl;
		throw "Invalid logging configuration file";
	}

	PropertyConfigurator::configureAndWatch(confpath);
	log4cxx::Logger::getLogger("root")->info("Logging system initialized with config file: "+confpath);
}

ScopedLoggingCtx::ScopedLoggingCtx(const string &ctx)
{
	NDC::push(ctx);
}

ScopedLoggingCtx::~ScopedLoggingCtx()
{
	NDC::pop();
}

BHT_RELAY_END

