#ifndef MEMCACHED_BACKEND_UNIT_H_STUB	// 头文件保护宏，请根据实际的头文件名自行修改
#define MEMCACHED_BACKEND_UNIT_H_STUB	//

#include <cppunit/extensions/HelperMacros.h>
// 为了得到 timeval 结构和 gettimeofday() 函数的声明，以便输出每次测试的耗时
#include <sys/time.h>

class MemcachedBackendUnitTest/* 这里的单元测试类名必须在整个单元测试系统中唯一 */
	: public CPPUNIT_NS::TestFixture
{
	CPPUNIT_TEST_SUITE( MemcachedBackendUnitTest /* 这里是单元测试类名 */ );
	CPPUNIT_TEST( test_factory /* 单个测试用例函数 */ );
	CPPUNIT_TEST( test_function );
	// TODO: 这里可以用 CPPUNIT_TEST() 宏继续增加测试用例函数
	CPPUNIT_TEST_SUITE_END();

	struct timeval begin; // 为了计算测试耗时而定义

	// TODO: 这里可以增加测试用例都要使用的变量，注意变量值并不在用例之间共享
public:
	void setUp();
	void tearDown();

	/* 测试用例函数声明 */
	void test_factory();
	void test_function();
	// TODO: 这里可以继续增加测试用例函数声明，所有测试用例函数的原型相同
};

#endif

