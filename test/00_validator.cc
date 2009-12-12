#include <iostream>	// 为了输出耗时信息而包含
#include <sys/time.h>	// 为了 timeval 结构和 gettimeofday() 函数的声明而包含

#include "00_validator.h"	// 载入单元测试的声明，请根据实际文件名自行修改

#include "Validator.h"

using namespace std;
using namespace BHT;

CPPUNIT_TEST_SUITE_REGISTRATION( ValidatorUnitTest/*此处写入单元测试类名*/);

void ValidatorUnitTest::setUp()
{
	// 记录测试开始时刻以便输出耗时信息
	gettimeofday(&begin,0);

	// TODO: 可以在本函数中插入所有测试用例共用的资源初始化代码
}

void ValidatorUnitTest::tearDown()
{
	struct timeval end;
	double elapsed;

	// 单次测试结束，根据当前时刻输出耗时信息
	gettimeofday(&end,0);
	elapsed=(end.tv_sec-begin.tv_sec)+(end.tv_usec-begin.tv_usec)/1000000.0;
	std::cerr<<" ["<<elapsed<<" s] ";

	// TODO: 可以在本函数中插入所有测试用例共用的资源释放代码
}

void ValidatorUnitTest::test_domain()
{
	// domain length test
	CPPUNIT_ASSERT(!Validator::isValidDomain(string()));
	CPPUNIT_ASSERT(!Validator::isValidDomain(string(9,'a')));

	// domain valid charset test
	CPPUNIT_ASSERT(Validator::isValidDomain("heLLo1_"));
	CPPUNIT_ASSERT(!Validator::isValidDomain(" "));
	CPPUNIT_ASSERT(!Validator::isValidDomain("|.,"));
}

void ValidatorUnitTest::test_key()
{
	// key length test
	CPPUNIT_ASSERT(!Validator::isValidKey(string()));
	CPPUNIT_ASSERT(!Validator::isValidKey(string(128,'a')));

	// key valid charset test
	CPPUNIT_ASSERT(Validator::isValidKey("!12_abdAJ.|'~"));
	CPPUNIT_ASSERT(!Validator::isValidKey(" "));
    CPPUNIT_ASSERT(!Validator::isValidKey("\x7f"));
}

void ValidatorUnitTest::test_subkey()
{
	// subkey length test
	CPPUNIT_ASSERT(Validator::isValidSubkey(string()));
	CPPUNIT_ASSERT(!Validator::isValidSubkey(string(121,'a')));

	// subkey valid charset test
	CPPUNIT_ASSERT(Validator::isValidSubkey("!12_abdAJ.|'~"));
	CPPUNIT_ASSERT(!Validator::isValidSubkey(" "));
    CPPUNIT_ASSERT(!Validator::isValidSubkey("\x7f"));
}

void ValidatorUnitTest::test_value()
{
	// value length test
	CPPUNIT_ASSERT(Validator::isValidValue(string()));
	CPPUNIT_ASSERT(!Validator::isValidValue(string(65537,'a')));
}

