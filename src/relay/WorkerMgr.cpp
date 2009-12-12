/*
 * =====================================================================================
 *
 *       Filename:  WorkerMgr.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/09/2009 06:01:59 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "WorkerMgr.h"
#include "Logger.h"

BHT_RELAY_BEGIN

DECLARE_LOGGER("BHT.Relay.WorkerMgr");

using namespace std;
using namespace boost;

WorkerMgr::WorkerMgr(uint32_t count, uint32_t sleep)
	: count_(count)
	, sleep_(sleep)
{
}


bool WorkerMgr::Start()
{
	for (uint32_t i=0; i<count_; ++i) {
		Worker::Ptr worker(new Worker(count_, i, sleep_));
		worker->Start();
		workers_.push_back(worker);
	}
	return true;
}

void WorkerMgr::Stop()
{
	vector<Worker::Ptr>::iterator it = workers_.begin(), end = workers_.end();
	for (; it!=end; ++it) {
		Worker::Ptr worker = *it;
		worker->Stop();
		worker.reset();
	}
	workers_.clear();
}

BHT_RELAY_END

