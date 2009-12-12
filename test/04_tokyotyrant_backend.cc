#include <iostream>	// 为了输出耗时信息而包含
#include <sys/time.h>	// 为了 timeval 结构和 gettimeofday() 函数的声明而包含

#include "04_tokyotyrant_backend.h"	// 载入单元测试的声明，请根据实际文件名自行修改
#include <boost/shared_ptr.hpp>

#include "TokyoTyrantBackend.h"

CPPUNIT_TEST_SUITE_REGISTRATION( TokyoTyrantBackendUnitTest/*此处写入单元测试类名*/);

using namespace ::std;
using namespace ::BHT;
using namespace ::boost;

void TokyoTyrantBackendUnitTest::setUp()
{
	// 记录测试开始时刻以便输出耗时信息
	gettimeofday(&begin,0);

	// TODO: 可以在本函数中插入所有测试用例共用的资源初始化代码
}

void TokyoTyrantBackendUnitTest::tearDown()
{
	struct timeval end;
	double elapsed;

	// 单次测试结束，根据当前时刻输出耗时信息
	gettimeofday(&end,0);
	elapsed=(end.tv_sec-begin.tv_sec)+(end.tv_usec-begin.tv_usec)/1000000.0;
	std::cerr<<" ["<<elapsed<<" s] ";

	// TODO: 可以在本函数中插入所有测试用例共用的资源释放代码
}

void TokyoTyrantBackendUnitTest::test_factory()
{
	TokyoTyrantBackendFactory factory;
	CPPUNIT_ASSERT_NO_THROW(factory.getBackendInstance("localhost", 1978));
}

void TokyoTyrantBackendUnitTest::test_function()
{
	TokyoTyrantBackendFactory factory;
	shared_ptr<Backend> p;
	CPPUNIT_ASSERT_NO_THROW(p = factory.getBackendInstance("localhost",1978));

	CPPUNIT_ASSERT_NO_THROW(p->set(string("hello"), string("world"), true, 0));
	CPPUNIT_ASSERT_NO_THROW(p->set(string("hello"), string("x"), false, 0));
	CPPUNIT_ASSERT_NO_THROW(p->set(string("hello"), string("world"), true, 0));

	string v;
	CPPUNIT_ASSERT_NO_THROW(p->get(string("hello"), &v));
	CPPUNIT_ASSERT(p->get(string("hello"), &v));
	CPPUNIT_ASSERT_EQUAL(v, string("world"));

	CPPUNIT_ASSERT_NO_THROW(p->get(string("he"), &v));
	CPPUNIT_ASSERT(p->get(string("he"), &v));
	CPPUNIT_ASSERT_EQUAL(v, string("world"));

	CPPUNIT_ASSERT_NO_THROW(p->get(string("x"), &v));
	CPPUNIT_ASSERT(!(p->get(string("x"), &v)));

	CPPUNIT_ASSERT_NO_THROW(p->set(string("dummy"), string(), true, 0));
	CPPUNIT_ASSERT(p->get(string("dummy"), &v));
	CPPUNIT_ASSERT_EQUAL(v, string());

	map<string,pair<bool,string> > kvs;

	kvs["hello"] = pair<bool,string>(false,string());
	kvs["dummy"] = pair<bool,string>(false,string());
	CPPUNIT_ASSERT_NO_THROW(p->mget(&kvs));
	CPPUNIT_ASSERT(kvs.find(string("hello")) != kvs.end());
	CPPUNIT_ASSERT(kvs["hello"].first);
	CPPUNIT_ASSERT_EQUAL(kvs["hello"].second, string("world"));
	CPPUNIT_ASSERT(kvs.find(string("dummy")) != kvs.end());
	CPPUNIT_ASSERT(kvs["dummy"].first);
	CPPUNIT_ASSERT_EQUAL(kvs["dummy"].second, string());

	CPPUNIT_ASSERT_NO_THROW(p->del(string("hello")));
	CPPUNIT_ASSERT_NO_THROW(p->get(string("hello"), &v));
	CPPUNIT_ASSERT(!(p->get(string("hello"), &v)));

	CPPUNIT_ASSERT_NO_THROW(p->del(string("h")));

	kvs["hello"] = pair<bool,string>(false,string());
	kvs["dummy"] = pair<bool,string>(false,string());
	CPPUNIT_ASSERT_NO_THROW(p->mget(&kvs));
	CPPUNIT_ASSERT(kvs.find(string("hello")) != kvs.end());
	CPPUNIT_ASSERT(!kvs["hello"].first);
	CPPUNIT_ASSERT_EQUAL(kvs["hello"].second, string());
	CPPUNIT_ASSERT(kvs.find(string("dummy")) != kvs.end());
	CPPUNIT_ASSERT(kvs["dummy"].first);
	CPPUNIT_ASSERT_EQUAL(kvs["dummy"].second, string());
}

