/*
 * =====================================================================================
 *
 *       Filename:  TaskSynchronizer.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/13/2009 03:49:05 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_TASK_SYNCHRONIZER_H
#define BHT_TASK_SYNCHRONIZER_H

#include "Common.h"
#include "thrift/concurrency/Mutex.h"
#include "thrift/concurrency/Monitor.h"

BHT_CODE_BEGIN

class TaskSynchronizer
{
public:
	TaskSynchronizer(uint32_t count): _count(count) { BOOST_ASSERT(count > 0);}
	~TaskSynchronizer() {}

	void Notify();
	void Wait();
private:
	apache::thrift::concurrency::Monitor _monitor;
	uint32_t _count;
};

BHT_CODE_END

#endif // BHT_TASK_SYNCHRONIZER_H

