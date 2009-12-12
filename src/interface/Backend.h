#ifndef BHT_BACKEND_H__
#define BHT_BACKEND_H__

#include "Common.h"

BHT_CODE_BEGIN

class Backend {
protected:
	virtual ~Backend() {}	//< 保护虚基类析构函数，避免从 shared_ptr 中取得指向派生类实例的虚基类裸指针被意外释放

public:
	virtual bool get(const ::std::string &key, ::std::string *val) = 0;	//< 获取给定 key 对应的 val
	virtual void mget(::std::map< ::std::string, ::std::pair<bool, ::std::string> > *kvs) = 0;	//< 同时对多个 key 执行 get
	virtual void set(const ::std::string &key, const ::std::string &val, bool overwrite, int timeout) = 0;	//< 将给定的 key-val 对保存到存储介质中
	virtual void del(const ::std::string &key) = 0;	//< 从存储介质中删除给定的 key 对应的记录
	virtual ::std::string host() const = 0;
	virtual int port() const = 0;
};

class BackendFactory {
public:
	virtual ~BackendFactory() {}
	virtual ::boost::shared_ptr<Backend> getBackendInstance(const ::std::string &host, int port) = 0;
};

BHT_CODE_END

#endif

