/*
 * =====================================================================================
 *
 *       Filename:  Worker.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/09/2009 03:22:12 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_WORKER_H
#define BHT_RELAY_WORKER_H

#include "Common.h"
#include "Operation.h"
#include "Storage.h"
#include <string>
#include <boost/shared_ptr.hpp>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/Monitor.h>

BHT_RELAY_BEGIN

class Worker
{
public:
	typedef boost::shared_ptr<Worker> Ptr;
	Worker(uint32_t count, uint32_t slice, uint32_t sleep=1);
	~Worker();

	bool Start();
	void Stop();
private:
	void ThreadProc();
	bool ProcessOp(const std::string& key, Operation::Ptr op);
private:
	boost::shared_ptr<apache::thrift::concurrency::Thread> thread_;
	apache::thrift::concurrency::Monitor monitor_;
	Storage::Ptr storage_;
	bool running_;
	uint32_t count_;
	uint32_t slice_;
	uint32_t sleep_;
};

BHT_RELAY_END

#endif // BHT_RELAY_WORKER_H

