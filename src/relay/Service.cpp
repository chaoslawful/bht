/*
 * =====================================================================================
 *
 *       Filename:  Service.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/15/2009 05:14:44 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Service.h"
#include "Logger.h"
#include "LocalHandler.h"
#include "RemoteHandler.h"
#include "gen-cpp/Local.h"

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/server/TNonblockingServer.h>

using namespace apache::thrift;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using boost::shared_ptr;

BHT_RELAY_BEGIN

DECLARE_LOGGER("BHT.Relay.Service");

bool Service::Start()
{
	TRACE("Services start... port: " << port_ << " threads: " << threads_);

	shared_ptr<TProcessor> processor = CreateProcessor();
	shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

	shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
	shared_ptr<ThreadManager> threadManager(ThreadManager::newSimpleThreadManager(threads_));
	threadManager->threadFactory(threadFactory);
	threadManager->start();

	TNonblockingServer server(processor, protocolFactory, port_, threadManager);
	server.serve();
	return true;
}

shared_ptr<TProcessor> LocalService::CreateProcessor() const
{
	shared_ptr<LocalIf> handler(new LocalHandler());
	shared_ptr<TProcessor> processor(new LocalProcessor(handler));
	return processor;
}

shared_ptr<TProcessor> RemoteService::CreateProcessor() const
{
	shared_ptr<RemoteIf> handler(new RemoteHandler());
	shared_ptr<TProcessor> processor(new RemoteProcessor(handler));
	return processor;
}

BHT_RELAY_END

