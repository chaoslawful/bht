/**
 * TBBThreadFactory 基本上照搬了 Thrift 的 PosixThreadFactory 的代码，区别是
 * TBBThreadFactory 在产生新线程后会先于指定的业务逻辑创建 Intel TBB 库的
 * task_scheduler_init 实例，以满足 TBB 的运行要求。
 *
 * 对 TBBThreadFactory 的代码有疑问时请参考 Thrift 的 PosixThreadFactory。
 * */
#ifndef BHT_TBBTHREADFACTORY_H__
#define BHT_TBBTHREADFACTORY_H__

#include "Common.h"
#include <thrift/concurrency/Thread.h>
#include <boost/shared_ptr.hpp>

BHT_CODE_BEGIN

class TBBThreadFactory : public ::apache::thrift::concurrency::ThreadFactory {
public:
	enum POLICY {
		OTHER,
		FIFO,
		ROUND_ROBIN
	};

	enum PRIORITY {
		LOWEST = 0,
		LOWER = 1,
		LOW = 2,
		NORMAL = 3,
		HIGH = 4,
		HIGHER = 5,
		HIGHEST = 6,
		INCREMENT = 7,
		DECREMENT = 8
	};

	TBBThreadFactory(POLICY policy=ROUND_ROBIN, PRIORITY priority=NORMAL, int stackSize=1, bool detached=true);
	::boost::shared_ptr< ::apache::thrift::concurrency::Thread > newThread(::boost::shared_ptr< ::apache::thrift::concurrency::Runnable > runnable) const;
	::apache::thrift::concurrency::Thread::id_t getCurrentThreadId() const;

	virtual int getStackSize() const;
	virtual void setStackSize(int value);
	virtual PRIORITY getPriority() const;
	virtual void setPriority(PRIORITY priority);
	virtual void setDetached(bool detached);
	virtual bool isDetached() const;

private:
	class Impl;
	::boost::shared_ptr<Impl> impl_;
};

BHT_CODE_END

#endif
