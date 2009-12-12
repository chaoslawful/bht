#include <iostream>	// 为了输出耗时信息而包含
#include <sys/time.h>	// 为了 timeval 结构和 gettimeofday() 函数的声明而包含
#include <time.h>
#include <stdlib.h>

#include "02_internalkey.h"	// 载入单元测试的声明，请根据实际文件名自行修改

#include <string>
#include "InternalKey.h"

using namespace ::std;
using namespace ::BHT;

CPPUNIT_TEST_SUITE_REGISTRATION( InternalKeyUnitTest/*此处写入单元测试类名*/);

void InternalKeyUnitTest::setUp()
{
	// 记录测试开始时刻以便输出耗时信息
	gettimeofday(&begin,0);

	// TODO: 可以在本函数中插入所有测试用例共用的资源初始化代码
}

void InternalKeyUnitTest::tearDown()
{
	struct timeval end;
	double elapsed;

	// 单次测试结束，根据当前时刻输出耗时信息
	gettimeofday(&end,0);
	elapsed=(end.tv_sec-begin.tv_sec)+(end.tv_usec-begin.tv_usec)/1000000.0;
	std::cerr<<" ["<<elapsed<<" s] ";

	// TODO: 可以在本函数中插入所有测试用例共用的资源释放代码
}

void InternalKeyUnitTest::test_pkey()
{
	PartitionKey pkey("hello", "world");
	CPPUNIT_ASSERT(pkey.toString() == "p\001hello\001world");
}

void InternalKeyUnitTest::test_skey()
{
	StorageKey skey("hello", "world", "a");
	CPPUNIT_ASSERT(skey.toString() == "s\001hello\001world\001a\001");

	StorageKey skeyt("hello", "world", "a", 128);
	CPPUNIT_ASSERT(skeyt.toString() == "s\001hello\001world\001a\001\xff\xff\xff\xff\xff\xff\xff\x80");

	srandom(time(NULL));
	for(int i=0;i<10000;++i) {
		long r1 = random();
		long r2 = random();
		StorageKey skey1("h", "w", "x", r1);
		StorageKey skey2("h", "w", "x", r2);
		// 大的时戳对应序列化后 key 的字符串顺序应该小，反之亦然
		if(r1>=r2) {
			CPPUNIT_ASSERT(skey1.toString() <= skey2.toString());
		} else {
			CPPUNIT_ASSERT(skey1.toString() > skey2.toString());
		}
	}
}

