#include <iostream>	// 为了输出耗时信息而包含
#include <sys/time.h>	// 为了 timeval 结构和 gettimeofday() 函数的声明而包含

#include "06_config.h"	// 载入单元测试的声明，请根据实际文件名自行修改

#include "InternalKey.h"
#include "Config.h"

using namespace ::boost;
using namespace ::BHT;

CPPUNIT_TEST_SUITE_REGISTRATION( ConfigUnitTest/*此处写入单元测试类名*/);

void ConfigUnitTest::setUp()
{
	// 记录测试开始时刻以便输出耗时信息
	gettimeofday(&begin,0);

	// TODO: 可以在本函数中插入所有测试用例共用的资源初始化代码
}

void ConfigUnitTest::tearDown()
{
	struct timeval end;
	double elapsed;

	// 单次测试结束，根据当前时刻输出耗时信息
	gettimeofday(&end,0);
	elapsed=(end.tv_sec-begin.tv_sec)+(end.tv_usec-begin.tv_usec)/1000000.0;
	std::cerr<<" ["<<elapsed<<" s] ";

	// TODO: 可以在本函数中插入所有测试用例共用的资源释放代码
}

void ConfigUnitTest::test()
{
	CPPUNIT_ASSERT_NO_THROW(Config::getInstance());

	Config &conf = Config::getInstance();
	CPPUNIT_ASSERT_NO_THROW(conf.updateConfig());

	CPPUNIT_ASSERT_NO_THROW(conf.getStorageRing());
	CPPUNIT_ASSERT_NO_THROW(conf.getLookupRing());
	CPPUNIT_ASSERT_NO_THROW(conf.getCacheRing());
	CPPUNIT_ASSERT_NO_THROW(conf.getRelayRing());

	StorageKey sk("hello","world","x");
	PartitionKey pk("hello","world");
	shared_ptr<Ring> r;
	
	r = conf.getStorageRing();
	CPPUNIT_ASSERT(r);
	CPPUNIT_ASSERT(r->getNode(sk));
	CPPUNIT_ASSERT_EQUAL(r->getNode(sk)->id(), (uint32_t)0x5678);

	r = conf.getLookupRing();
	CPPUNIT_ASSERT(r);
	CPPUNIT_ASSERT(r->getNode(pk));
	CPPUNIT_ASSERT_EQUAL(r->getNode(pk)->id(), (uint32_t)0x1234);

	r = conf.getCacheRing();
	CPPUNIT_ASSERT(r);
	CPPUNIT_ASSERT(r->getNode(sk));
	CPPUNIT_ASSERT_EQUAL(r->getNode(sk)->id(), (uint32_t)0xaabb);

	r = conf.getRelayRing();
	CPPUNIT_ASSERT(r);
	CPPUNIT_ASSERT(r->getNode(sk));
	CPPUNIT_ASSERT_EQUAL(r->getNode(sk)->id(), (uint32_t)0xccdd);

	CPPUNIT_ASSERT(conf.isRelayDomain("domain1"));
	CPPUNIT_ASSERT(!conf.isRelayDomain("test"));
	CPPUNIT_ASSERT(conf.isRelayDomain("domain2"));
}

