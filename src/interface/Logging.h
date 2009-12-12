#ifndef BHT_LOGGING_H__
#define BHT_LOGGING_H__

#include <string>
#include <log4cxx/logger.h>
#include "Common.h"

#define BHT_INIT_LOGGING(name) static ::BHT::Logging _bht_logger = ::BHT::Logging::getInstance(name);
#define BHT_INIT_LOGGING2(name,conf) static ::BHT::Logging _bht_logger = ::BHT::Logging::getInstance(name,conf);
#define DEBUG(msg) _bht_logger.debug(msg,__FILE__,__LINE__)
#define INFO(msg) _bht_logger.info(msg,__FILE__,__LINE__)
#define WARN(msg) _bht_logger.warn(msg,__FILE__,__LINE__)
#define ERROR(msg) _bht_logger.error(msg,__FILE__,__LINE__)
#define FATAL(msg) _bht_logger.fatal(msg,__FILE__,__LINE__)
#define PUSH_CTX(msg) _bht_logger.push_ctx(msg)
#define POP_CTX() _bht_logger.pop_ctx()

BHT_CODE_BEGIN

class ScopedLoggingCtx {
public:
	ScopedLoggingCtx(const ::std::string &ctx);
	~ScopedLoggingCtx();
};

class Logging {
	static bool _inited;

	::log4cxx::LoggerPtr _logger;

	Logging(::log4cxx::LoggerPtr logger)
		:_logger(logger)
	{
	}

public:
	/**
	 * 获取日志对象实例。
	 * @param name 日志对象名称，可在配置文件中控制其信息是否输出。
	 * @param conf 指定日志配置文件路径。
	 * @note 只允许在全局上下文中调用该方法初始化静态日志对象！
	 * @retval 日志对象实例。
	 * */
	static Logging getInstance(const ::std::string &name, const ::std::string &conf = ::std::string());

	~Logging() {}
	void debug(const ::std::string &msg, const char *file=0, int line=-1);
	void info(const ::std::string &msg, const char *file=0, int line=-1);
	void warn(const ::std::string &msg, const char *file=0, int line=-1);
	void error(const ::std::string &msg, const char *file=0, int line=-1);
	void fatal(const ::std::string &msg, const char *file=0, int line=-1);
	void push_ctx(const ::std::string &msg);
	void pop_ctx();
};

BHT_CODE_END

#endif

