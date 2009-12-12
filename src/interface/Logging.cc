#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/ndc.h>
#include <cstdlib>	// for abort()
#include <iostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Logging.h"

#define DEFAULT_LOGCONF_PATH "/home/z/conf/bht/bht_log.conf"
#define TEST_LOGCONF_PATH "./testlog.conf"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::log4cxx;

// Logging 类静态成员初始化
bool Logging::_inited = false;

Logging Logging::getInstance(const string &name, const string &conf)
{
	if(!_inited) {
		// 日志系统尚未初始化
		struct stat st;
		string confpath = conf;
		if(confpath.size() == 0) {
			confpath = DEFAULT_LOGCONF_PATH;
		}

		// 若指定的日志配置文件不存在，或者存在但不是普通文件或符号链接，则抛出异常终止程序
		if(stat(confpath.c_str(), &st) || !(S_ISREG(st.st_mode) || S_ISLNK(st.st_mode))) {
			confpath = TEST_LOGCONF_PATH;

			if(stat(confpath.c_str(), &st) || !(S_ISREG(st.st_mode) || S_ISLNK(st.st_mode))) {
				cerr << "Invalid logging configuration file: "<<confpath<<endl;
				throw "Invalid logging configuration file";
			}
		}

		PropertyConfigurator::configureAndWatch(confpath, 5000);	// 每隔 5s 检查一次日志配置文件是否变化
//		PropertyConfigurator::configure(confpath);
		Logger::getLogger("root")->info("Logging system initialized with config file: "+confpath);

		_inited = true;
	}

	// 获取一个新的 log4cxx 日志对象包装后返回
	return Logging(Logger::getLogger(name));
}

void Logging::debug(const string &msg, const char *file, int line)
{
#if defined(LOG4CXX_LOCATION)
	_logger->debug(msg, ::log4cxx::spi::LocationInfo(file, __LOG4CXX_FUNC__, line));
#else
	_logger->debug(msg, file, line);
#endif
}

void Logging::info(const string &msg, const char *file, int line)
{
#if defined(LOG4CXX_LOCATION)
	_logger->info(msg, ::log4cxx::spi::LocationInfo(file, __LOG4CXX_FUNC__, line));
#else
	_logger->info(msg, file, line);
#endif
}

void Logging::warn(const string &msg, const char *file, int line)
{
#if defined(LOG4CXX_LOCATION)
	_logger->warn(msg, ::log4cxx::spi::LocationInfo(file, __LOG4CXX_FUNC__, line));
#else
	_logger->warn(msg, file, line);
#endif
}

void Logging::error(const string &msg, const char *file, int line)
{
#if defined(LOG4CXX_LOCATION)
	_logger->error(msg, ::log4cxx::spi::LocationInfo(file, __LOG4CXX_FUNC__, line));
#else
	_logger->error(msg, file, line);
#endif
}

void Logging::fatal(const string &msg, const char *file, int line)
{
#if defined(LOG4CXX_LOCATION)
	_logger->fatal(msg, ::log4cxx::spi::LocationInfo(file, __LOG4CXX_FUNC__, line));
#else
	_logger->fatal(msg, file, line);
#endif
}

void Logging::push_ctx(const string &msg)
{
	NDC::push(msg);
}

void Logging::pop_ctx()
{
	NDC::pop();
}

ScopedLoggingCtx::ScopedLoggingCtx(const string &ctx)
{
	NDC::push(ctx);
}

ScopedLoggingCtx::~ScopedLoggingCtx()
{
	NDC::pop();
}

BHT_CODE_END

