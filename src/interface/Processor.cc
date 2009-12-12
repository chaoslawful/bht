/*
 * =====================================================================================
 *
 *       Filename:  Processor.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/13/2009 03:16:21 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Processor.h"
#include "Config.h"
#include <thrift/concurrency/FunctionRunner.h>
#include <boost/bind.hpp>

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift::concurrency;

::boost::shared_ptr<Processor> Processor::_instance;

::boost::shared_ptr<Processor> Processor::getInstance()
{
	if (!_instance) {
		Config& conf = Config::getInstance();
		int32_t worker_num = conf.getHandlerWorkerNumber();
		if (worker_num <= 0) {
			worker_num = 10;
		}
		_instance = boost::shared_ptr<Processor>(new Processor(worker_num));
		_instance->Start();
	}
	return _instance;
}

Processor::Processor(int32_t worker_num)
	: _threads(ThreadManager::newSimpleThreadManager(worker_num))
{
}

void Processor::Start()
{
	_threads->start();
}

void Processor::Stop()
{
	_threads->join();
}

void Processor::Process(::boost::shared_ptr<Task> task, TaskSynchronizer& sync)
{
	shared_ptr<Runnable> runner(new FunctionRunner(boost::bind(&Processor::DoProcess, this, task, sync)));
	_threads->add(runner);
}

void Processor::DoProcess(::boost::shared_ptr<Task> task, TaskSynchronizer& sync)
{
	task->Run();
	sync.Notify();
}

BHT_CODE_END

