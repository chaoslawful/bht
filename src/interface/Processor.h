/*
 * =====================================================================================
 *
 *       Filename:  Processor.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/10/2009 10:43:35 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_PROCESSOR_H
#define BHT_PROCESSOR_H

#include "Common.h"
#include "Task.h"
#include "TaskSynchronizer.h"
#include <thrift/concurrency/ThreadManager.h>

BHT_CODE_BEGIN

class Processor
{
public:
	Processor(int32_t worker_num);
	~Processor() {}

	static ::boost::shared_ptr<Processor> getInstance();

	void Start();
	void Stop();

	void Process(::boost::shared_ptr<Task> task, TaskSynchronizer& sync);
private:
	void DoProcess(::boost::shared_ptr<Task> task, TaskSynchronizer& sync);
private:
	::boost::shared_ptr< ::apache::thrift::concurrency::ThreadManager> _threads;

	static ::boost::shared_ptr<Processor> _instance;
};

BHT_CODE_END

#endif // BHT_PROCESSOR_H

