#include <assert.h>
#include <pthread.h>
#include <iostream>
#include <boost/weak_ptr.hpp>

#include <tbb/task_scheduler_init.h>

#include <thrift/concurrency/Exception.h>
#include "TBBThreadFactory.h"
#include "Config.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::boost;
using namespace ::tbb;
using namespace ::apache::thrift;
using namespace ::apache::thrift::concurrency;

class TBBPthreadThread: public Thread {
public:
	enum STATE {
		uninitialized,
		starting,
		started,
		stopping,
		stopped
	};
	static const int MB = 1024 * 1024;
	static void* threadMain(void* arg);

private:
	pthread_t pthread_;
	STATE state_;
	int policy_;
	int priority_;
	int stackSize_;
	weak_ptr<TBBPthreadThread> self_;
	bool detached_;

public:
	TBBPthreadThread(int policy, int priority, int stackSize, bool detached, shared_ptr<Runnable> runnable) :
		pthread_(0),
		state_(uninitialized),
		policy_(policy),
		priority_(priority),
		stackSize_(stackSize),
		detached_(detached)
	{
		this->Thread::runnable(runnable);
	}

	~TBBPthreadThread()
	{
		/* Nothing references this thread, if is is not detached, do a join
		   now, otherwise the thread-id and, possibly, other resources will
		   be leaked. */
		if(!detached_) {
			try {
				join();
			} catch(...) {
				// We're really hosed.
			}
		}
	}

	void start()
	{
		if (state_ != uninitialized) {
			return;
		}

		pthread_attr_t thread_attr;
		if (pthread_attr_init(&thread_attr) != 0) {
			throw SystemResourceException("pthread_attr_init failed");
		}

		if(pthread_attr_setdetachstate(&thread_attr,
					detached_ ?
					PTHREAD_CREATE_DETACHED :
					PTHREAD_CREATE_JOINABLE) != 0) {
			throw SystemResourceException("pthread_attr_setdetachstate failed");
		}

		// Set thread stack size
		if (pthread_attr_setstacksize(&thread_attr, MB * stackSize_) != 0) {
			throw SystemResourceException("pthread_attr_setstacksize failed");
		}

		// Set thread policy
		if (pthread_attr_setschedpolicy(&thread_attr, policy_) != 0) {
			throw SystemResourceException("pthread_attr_setschedpolicy failed");
		}

		struct sched_param sched_param;
		sched_param.sched_priority = priority_;

		// Set thread priority
		if (pthread_attr_setschedparam(&thread_attr, &sched_param) != 0) {
			throw SystemResourceException("pthread_attr_setschedparam failed");
		}

		// Create reference
		shared_ptr<TBBPthreadThread>* selfRef = new shared_ptr<TBBPthreadThread>();
		*selfRef = self_.lock();

		state_ = starting;

		if (pthread_create(&pthread_, &thread_attr, threadMain, (void*)selfRef) != 0) {
			throw SystemResourceException("pthread_create failed");
		}
	}

	void join()
	{
		if (!detached_ && state_ != uninitialized) {
			void* ignore;
			/* XXX
			   If join fails it is most likely due to the fact
			   that the last reference was the thread itself and cannot
			   join.  This results in leaked threads and will eventually
			   cause the process to run out of thread resources.
			   We're beyond the point of throwing an exception.  Not clear how
			   best to handle this. */
			detached_ = pthread_join(pthread_, &ignore) == 0;
		}
	}

	Thread::id_t getId()
	{
		return (Thread::id_t)pthread_;
	}

	shared_ptr<Runnable> runnable() const { return Thread::runnable(); }

	void runnable(shared_ptr<Runnable> value) { Thread::runnable(value); }

	void weakRef(shared_ptr<TBBPthreadThread> self) {
		assert(self.get() == this);
		self_ = weak_ptr<TBBPthreadThread>(self);
	}
};

void* TBBPthreadThread::threadMain(void* arg)
{
	shared_ptr<TBBPthreadThread> thread = *(shared_ptr<TBBPthreadThread>*)arg;
	delete reinterpret_cast<shared_ptr<TBBPthreadThread>*>(arg);

	if (thread == NULL) {
		return (void*)0;
	}

	if (thread->state_ != starting) {
		return (void*)0;
	}

	Config& conf = Config::getInstance();

	// 为当前线程初始化 Intel TBB 库
	task_scheduler_init init(task_scheduler_init::deferred);
	int32_t worker_num = conf.getHandlerWorkerNumber();
	if(worker_num == 0) {
		init.initialize();
	} else {
		init.initialize(worker_num);
	}

	thread->state_ = starting;
	thread->runnable()->run();
	if (thread->state_ != stopping && thread->state_ != stopped) {
		thread->state_ = stopping;
	}

	return (void*)0;
}

class TBBThreadFactory::Impl {
private:
	POLICY policy_;
	PRIORITY priority_;
	int stackSize_;
	bool detached_;

	/**
	 * Converts generic posix thread schedule policy enums into pthread
	 * API values.
	 */
	static int toPthreadPolicy(POLICY policy)
	{
		switch (policy) {
			case OTHER:
				return SCHED_OTHER;
			case FIFO:
				return SCHED_FIFO;
			case ROUND_ROBIN:
				return SCHED_RR;
		}
		return SCHED_OTHER;
	}

	/**
	 * Converts relative thread priorities to absolute value based on posix
	 * thread scheduler policy
	 *
	 *  The idea is simply to divide up the priority range for the given policy
	 * into the correpsonding relative priority level (lowest..highest) and
	 * then pro-rate accordingly.
	 */
	static int toPthreadPriority(POLICY policy, PRIORITY priority)
	{
		int pthread_policy = toPthreadPolicy(policy);
		int min_priority = 0;
		int max_priority = 0;
		min_priority = sched_get_priority_min(pthread_policy);
		max_priority = sched_get_priority_max(pthread_policy);
		int quanta = (HIGHEST - LOWEST) + 1;
		float stepsperquanta = (max_priority - min_priority) / quanta;

		if (priority <= HIGHEST) {
			return (int)(min_priority + stepsperquanta * priority);
		} else {
			// should never get here for priority increments.
			assert(false);
			return (int)(min_priority + stepsperquanta * NORMAL);
		}
	}

public:
	Impl(POLICY policy, PRIORITY priority, int stackSize, bool detached) :
		policy_(policy),
		priority_(priority),
		stackSize_(stackSize),
		detached_(detached) {}

	shared_ptr<Thread> newThread(shared_ptr<Runnable> runnable) const
	{
		shared_ptr<TBBPthreadThread> result = shared_ptr<TBBPthreadThread>(new TBBPthreadThread(toPthreadPolicy(policy_), toPthreadPriority(policy_, priority_), stackSize_, detached_, runnable));
		result->weakRef(result);
		runnable->thread(result);
		return result;
	}

	int getStackSize() const { return stackSize_; }

	void setStackSize(int value) { stackSize_ = value; }

	PRIORITY getPriority() const { return priority_; }

	void setPriority(PRIORITY value) { priority_ = value; }

	bool isDetached() const { return detached_; }

	void setDetached(bool value) { detached_ = value; }

	Thread::id_t getCurrentThreadId() const
	{
		return (id_t)pthread_self();
	}

};

TBBThreadFactory::TBBThreadFactory(POLICY policy, PRIORITY priority, int stackSize, bool detached) :
	impl_(new TBBThreadFactory::Impl(policy, priority, stackSize, detached))
{
}

shared_ptr<Thread> TBBThreadFactory::newThread(shared_ptr<Runnable> runnable) const { return impl_->newThread(runnable); }

int TBBThreadFactory::getStackSize() const { return impl_->getStackSize(); }

void TBBThreadFactory::setStackSize(int value) { impl_->setStackSize(value); }

TBBThreadFactory::PRIORITY TBBThreadFactory::getPriority() const { return impl_->getPriority(); }

void TBBThreadFactory::setPriority(TBBThreadFactory::PRIORITY value) { impl_->setPriority(value); }

bool TBBThreadFactory::isDetached() const { return impl_->isDetached(); }

void TBBThreadFactory::setDetached(bool value) { impl_->setDetached(value); }

Thread::id_t TBBThreadFactory::getCurrentThreadId() const { return impl_->getCurrentThreadId(); }

BHT_CODE_END

