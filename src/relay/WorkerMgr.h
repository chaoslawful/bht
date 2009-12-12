/*
 * =====================================================================================
 *
 *       Filename:  WorkerMgr.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/09/2009 05:20:31 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_WORKER_MGR_H
#define BHT_RELAY_WORKER_MGR_H

#include "Common.h"
#include "Singleton.hpp"
#include "Worker.h"

#include <vector>
#include <boost/shared_ptr.hpp>

BHT_RELAY_BEGIN

class WorkerMgr
{
	friend class Singleton<WorkerMgr>;
public:
	typedef boost::shared_ptr<WorkerMgr> Ptr;

	explicit WorkerMgr(uint32_t count, uint32_t sleep = 1);
	~WorkerMgr() {}

	bool Start();
	void Stop();
private:
	uint32_t count_;
	uint32_t sleep_;
	std::vector<Worker::Ptr> workers_;
};

BHT_RELAY_END

#endif // BHT_RELAY_WORKER_MGR_H

