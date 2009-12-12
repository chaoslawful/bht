/*
 * =====================================================================================
 *
 *       Filename:  Service.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/15/2009 05:07:41 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_SERVICE_H
#define BHT_RELAY_SERVICE_H

#include "Common.h"
#include "Singleton.hpp"
#include "gen-cpp/Local.h"
#include <thrift/TProcessor.h>
#include <boost/shared_ptr.hpp>

BHT_RELAY_BEGIN

class Service 
{
public:
	typedef boost::shared_ptr<Service> Ptr;
	explicit Service(uint32_t port, size_t threads) : port_(port), threads_(threads) {}
	virtual ~Service() {}

	bool Start();
protected:
	virtual boost::shared_ptr<apache::thrift::TProcessor> CreateProcessor() const = 0;
protected:
	uint32_t port_;
	size_t threads_;
};

class LocalService : public Service
{
	friend class Singleton<LocalService>;
public:
	LocalService(uint32_t port, size_t threads): Service(port, threads) {}
private:
	boost::shared_ptr<apache::thrift::TProcessor> CreateProcessor() const;
};

class RemoteService: public Service
{
	friend class Singleton<RemoteService>;
public:
	RemoteService(uint32_t port, size_t threads): Service(port, threads) {}
private:
	boost::shared_ptr<apache::thrift::TProcessor> CreateProcessor() const;
};

BHT_RELAY_END

#endif // BHT_RELAY_SERVICE_H

