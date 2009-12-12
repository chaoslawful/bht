/*
 * =====================================================================================
 *
 *       Filename:  Task.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/10/2009 03:36:24 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_TASK_H
#define BHT_TASK_H

#include "Common.h"
#include "Node.h"
#include <thrift/concurrency/Monitor.h>

BHT_CODE_BEGIN

class Task
{
public:
	virtual ~Task() {}
	virtual void Run() = 0;
};

class TaskMGetFromCache : virtual public Task
{
public:
	TaskMGetFromCache(::std::map<Key, Val>& kvs, ::boost::shared_ptr<Node> node, const ::std::map<StorageKey*, const Key*>& ks, ::std::map<StorageKey*, const Key*>& all_ks, apache::thrift::concurrency::Mutex& mutex): _kvs(kvs), _node(node), _ks(ks), _all_ks(all_ks), _mutex(mutex) {}

	void Run();
private:
	::std::map<Key, Val>& _kvs;
	::boost::shared_ptr<Node> _node;
	const ::std::map<StorageKey*, const Key*>& _ks;
	::std::map<StorageKey*, const Key*>& _all_ks;
	apache::thrift::concurrency::Mutex& _mutex;
};

class TaskMGetFromSNode : virtual public Task
{
public:
	TaskMGetFromSNode(::std::map<Key, Val>& kvs, ::boost::shared_ptr<Node> node, const ::std::map<StorageKey*, const Key*>& ks, ::std::map<StorageKey*, const Key*>& all_ks, apache::thrift::concurrency::Mutex& mutex): _kvs(kvs), _node(node), _ks(ks), _all_ks(all_ks), _mutex(mutex) {}

	void Run();
private:
	::std::map<Key, Val>& _kvs;
	::boost::shared_ptr<Node> _node;
	const ::std::map<StorageKey*, const Key*>& _ks;
	::std::map<StorageKey*, const Key*>& _all_ks;
	::apache::thrift::concurrency::Mutex& _mutex;
};

class TaskLocateNode : virtual public Task
{
public:
	TaskLocateNode(::std::map< ::boost::shared_ptr<Node>, ::std::map<StorageKey*, const Key*> >& node_map,
			const ::std::string& domain, const StorageKey* sk, const Key* k,
			::apache::thrift::concurrency::Mutex& mutex): _node_map(node_map), _domain(domain), _sk(sk), _k(k), _mutex(mutex) {}

	void Run();
private:
	::std::map< ::boost::shared_ptr<Node>, ::std::map<StorageKey*, const Key*> >& _node_map;
	const ::std::string& _domain;
	const StorageKey* _sk;
	const Key* _k;
	::apache::thrift::concurrency::Mutex& _mutex;
};


class TaskSet : virtual public Task
{
public:
	TaskSet(const ::std::string& domain, const Key& k, const Val& v, bool overwrite): _domain(domain), _k(k), _v(v), _overwrite(overwrite) {}
	void Run();
private:
	const ::std::string& _domain;
	const Key& _k;
	const Val& _v;
	bool _overwrite;
};

class TaskDel : virtual public Task
{
public:
	TaskDel(const ::std::string& domain, const Key& k, bool overwrite): _domain(domain), _k(k), _overwrite(overwrite) {}
	void Run();
private:
	const ::std::string& _domain;
	const Key& _k;
	bool _overwrite;
};


BHT_CODE_END

#endif // BHT_TASK_H

