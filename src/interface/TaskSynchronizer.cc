/*
 * =====================================================================================
 *
 *       Filename:  TaskSynchronizer.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/13/2009 03:51:04 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "TaskSynchronizer.h"

BHT_CODE_BEGIN

using namespace std;
using namespace boost;
using namespace apache::thrift::concurrency;


void TaskSynchronizer::Notify()
{
	Synchronized lock(_monitor);
	if (_count == 0 || --_count == 0) {
		_monitor.notifyAll();
	}
}

void TaskSynchronizer::Wait()
{
	Synchronized lock(_monitor);
	if (_count > 0) {
		_monitor.wait();
	}
}


BHT_CODE_END

