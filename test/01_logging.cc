#include <iostream>	// 为了输出耗时信息而包含
#include <sys/time.h>	// 为了 timeval 结构和 gettimeofday() 函数的声明而包含

#include "01_logging.h"	// 载入单元测试的声明，请根据实际文件名自行修改

#include "Logging.h"

using namespace std;
using namespace BHT;

CPPUNIT_TEST_SUITE_REGISTRATION( LoggingUnitTest/*此处写入单元测试类名*/);

void LoggingUnitTest::setUp()
{
	// 记录测试开始时刻以便输出耗时信息
	gettimeofday(&begin,0);

	// TODO: 可以在本函数中插入所有测试用例共用的资源初始化代码
}

void LoggingUnitTest::tearDown()
{
	struct timeval end;
	double elapsed;

	// 单次测试结束，根据当前时刻输出耗时信息
	gettimeofday(&end,0);
	elapsed=(end.tv_sec-begin.tv_sec)+(end.tv_usec-begin.tv_usec)/1000000.0;
	std::cerr<<" ["<<elapsed<<" s] ";

	// TODO: 可以在本函数中插入所有测试用例共用的资源释放代码
}

void LoggingUnitTest::test1()
{
	BHT_INIT_LOGGING2("unit-test", "./testlog.conf")

	CPPUNIT_ASSERT_NO_THROW(DEBUG("hello,debug"));
	CPPUNIT_ASSERT_NO_THROW(INFO("hello,info"));
	CPPUNIT_ASSERT_NO_THROW(WARN("hello,warn"));
	CPPUNIT_ASSERT_NO_THROW(ERROR("hello,error"));
	CPPUNIT_ASSERT_NO_THROW(FATAL("hello,fatal"));
	CPPUNIT_ASSERT_NO_THROW(PUSH_CTX("<called>"));
	CPPUNIT_ASSERT_NO_THROW(DEBUG("hello,ndc"));
	CPPUNIT_ASSERT_NO_THROW(POP_CTX());
}

void LoggingUnitTest::test2()
{
	BHT_INIT_LOGGING2("unit-test", "./testlog.conf")

	CPPUNIT_ASSERT_NO_THROW(ScopedLoggingCtx("unit-test"));

	{
		ScopedLoggingCtx ctx("<LoggingUnitTest::test2>");
		CPPUNIT_ASSERT_NO_THROW(DEBUG("hello, scoped logging context"));
	}

	CPPUNIT_ASSERT_NO_THROW(DEBUG("oops, out of scope"));
}

