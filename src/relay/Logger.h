/*
 * =====================================================================================
 *
 *       Filename:  Logger.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/06/2009 03:20:41 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_LOGGER_H
#define BHT_RELAY_LOGGER_H

#include "Common.h"
#include <log4cxx/logger.h>
#include <log4cxx/helpers/exception.h>
//#include <log4cxx/ndc.h>
#include <string>

BHT_RELAY_BEGIN

class ScopedLoggingCtx {
public:
	ScopedLoggingCtx(const ::std::string &ctx);
	~ScopedLoggingCtx();
};

class Logger
{
public:
	static void Init(const std::string& config_file);
};

#define DECLARE_LOGGER(name)	static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger(name))
#define DEBUG(message)	LOG4CXX_DEBUG(logger, message)
#define TRACE(message)	LOG4CXX_TRACE(logger, message)
#define INFO(message)	LOG4CXX_INFO(logger, message)
#define WARN(message)	LOG4CXX_WARN(logger, message)
#define ERROR(message)	LOG4CXX_ERROR(logger, message)
#define FATAL(message)	LOG4CXX_FATAL(logger, message)

BHT_RELAY_END

#endif // BHT_RELAY_LOGGER_H

