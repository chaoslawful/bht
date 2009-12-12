/*
 * =====================================================================================
 *
 *       Filename:  Worker.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/09/2009 03:25:56 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Worker.h"
#include "Logger.h"
#include "StoragePool.h"
#include "Singleton.hpp"
#include "Framework.h"
#include "Config.h"
#include <boost/bind.hpp>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/concurrency/FunctionRunner.h>

BHT_RELAY_BEGIN

using namespace std;
using namespace boost;
using namespace BHT::Relay;
using namespace apache::thrift::concurrency;

DECLARE_LOGGER("BHT.Relay.Worker");

Worker::Worker(uint32_t count, uint32_t slice, uint32_t sleep)
	: running_(false)
	, count_(count)
	, slice_(slice)
	, sleep_(sleep)
{
	BOOST_ASSERT(slice < count);
	BOOST_ASSERT(sleep);
}

Worker::~Worker()
{
}

bool Worker::Start()
{
	ScopedLoggingCtx lc("<Worker::Start>");
	BOOST_ASSERT(!thread_);
	storage_ = Singleton<StoragePool>::Get()->Alloc();
	if (!storage_->Open()) {
		ERROR("storage open failed");
		return false;
	}
	running_ = true;
	shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
	shared_ptr<Runnable> runnable(new FunctionRunner(bind(&Worker::ThreadProc, this)));
	thread_ = threadFactory->newThread(runnable);
	thread_->start();
	return true;
}

void Worker::Stop()
{
	if (thread_) {
		running_ = false;
		monitor_.notify();
		thread_->join();
	}
}

void Worker::ThreadProc()
{
	ScopedLoggingCtx lc("<Worker::ThreadProc>");
	while (running_) {
		Storage::iterator it=storage_->begin(), end=storage_->end();
		if (it==end) {
			try{
				monitor_.wait(static_cast<int64_t>(sleep_)*1000);
			}catch(...){
			}
		} else {
			do {
				const std::string& key = *it;
				size_t vsiz = 0;
				shared_ptr<const char> v = storage_->Get(key, vsiz);
				// 此key可能已被其他worker处理后删除，因此会获取失败
				if (!v) {
					continue;
				}
				Operation::Ptr op = Operation::Unserialize(key, v.get(), vsiz);
				if (!op) {
					ERROR("Worker::ThreadProc() : Operation unserialize failed. key - " << key << ". val - " << v);
					storage_ = Singleton<StoragePool>::Get()->Alloc();
					continue;
				}
				if ((op->getTimestamp() % count_) == slice_) {
					if (!ProcessOp(key, op)) {
						storage_ = Singleton<StoragePool>::Get()->Alloc();
						storage_->Open();
					}
				}
				if (!running_) {
					return;
				}
			} while (++it != end);
		}
	}
}

bool Worker::ProcessOp(const std::string& key, Operation::Ptr op)
{
	ScopedLoggingCtx lc("<Worker::ProcessOp>");
	const std::vector<Config::AddrType>& target_addrs = Singleton<Config>::Get()->TargetAddrs();
	std::vector<Config::AddrType>::const_iterator it = target_addrs.begin(), end = target_addrs.end();
	for (; it!=end; ++it) {
		Target::Ptr target = Framework::getTargetPoolMgr()->Alloc(it->first, it->second);
		if (op->Relay(target)) {
			TRACE("Worker::ProcessOp() - Relay to remote ok");
			if (!storage_->Del(key)) {
				ERROR("Worker::ProcessOp() - Storage delete failed. key - " << key);
			} else {
				TRACE("Worker::ProcessOp() - Storage delete ok" << key);
				Framework::getTargetPoolMgr()->Free(target, target->host(), target->port());
			}
			return true;
		} 
		TRACE("Worker::ProcessOp() - Relay to remote failed");
	}
	return false;
}

BHT_RELAY_END

