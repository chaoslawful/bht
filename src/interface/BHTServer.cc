#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TNonblockingServer.h>

#include <tbb/task_scheduler_init.h>

#include <iostream>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "Config.h"
#include "TBBThreadFactory.h"
#include "Logging.h"

#define DEFAULT_SERVER_PORT 9090
#define DEFAULT_WORKER_NUM 10

using namespace ::std;
using namespace ::boost;
using namespace ::tbb;
using namespace ::apache::thrift;
using namespace ::apache::thrift::concurrency;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::BHT;

typedef enum { SIMPLE, NONBLOCKING } ServerType;

void daemonize();
void run_simple_server(int port);
void run_nonblocking_server(int port, int worker_num);
void parse_cmdline(int argc, char **argv);

BHT_INIT_LOGGING("BHT.Interface")

static int port = DEFAULT_SERVER_PORT;
static int worker_num = DEFAULT_WORKER_NUM;
static ServerType type = SIMPLE;
static bool run_as_daemon = false;

int main(int argc, char **argv)
{
	ScopedLoggingCtx logging_ctx("<main>");
	parse_cmdline(argc,argv);

	if(run_as_daemon) {
		daemonize();
	}

	switch(type) {
		case SIMPLE:
			run_simple_server(port);
			break;
		case NONBLOCKING:
			run_nonblocking_server(port, worker_num);
			break;
		default:
			ERROR("Unknown server type: " + lexical_cast<string>((int)type));
	};

	return 0;
}

void parse_cmdline(int argc, char **argv)
{
	ScopedLoggingCtx logging_ctx("<parse_cmdline>");
	int opt;
	int num;
	string t;

	while((opt = getopt(argc, argv, "?dn:p:t:")) != -1) {
		switch(opt) {
			case 'n':	// set worker number
				num = lexical_cast<int>(optarg);
				if(num <= 0) {
					ERROR("Invalid worker thread number: " + string(optarg));
					exit(-1);
				}
				worker_num = num;
				break;
			case 'p':	// set listening port
				num = lexical_cast<int>(optarg);
				if(num < 0 || num > 65535) {
					ERROR("Invalid listening port: " + string(optarg));
					exit(-1);
				}
				port = num;
				break;
			case 't':	// set server type
				t = string(optarg);
				if(t == "simple") {
					type = SIMPLE;
				} else if(t == "nonblocking") {
					type = NONBLOCKING;
				} else {
					ERROR("Invalid server type: " + t);
					exit(-1);
				}
				break;
			case 'd':	// daemonize
				run_as_daemon = true;
				break;
			default:
				cout << "Usage: " << argv[0] << " [-?] [-d] [-n <worker num>] [-p <port>] [-t simple | nonblocking]\n";
				exit(-1);
		}
	}
}

void daemonize()
{
	ScopedLoggingCtx logging_ctx("<daemonize>");
	pid_t pid;

	// fork 1
	if(fork()) {
		// exit parent
		exit(0);
	}

	// separates child from parent
	chdir("/");
	setsid();
	umask(0);

	// fork 2
	pid = fork();
	if(pid) {
		// exit parent
		exit(0);
	}

	// redirect stdin/stdout/stderr to /dev/null
	int fd = open("/dev/null", O_RDWR);
	dup2(fd, STDIN_FILENO);
	dup2(fd, STDOUT_FILENO);
	dup2(fd, STDERR_FILENO);
	close(fd);
}

void run_simple_server(int port)
{
	ScopedLoggingCtx logging_ctx("<run_simple_server>");
	INFO("Starting simple server: port = " + lexical_cast<string>(port));

	Config &conf = Config::getInstance();
	shared_ptr<BHTIf> handler = conf.getHandlerInstance();

	shared_ptr<TProcessor> processor(new BHTProcessor(handler));

	shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
	shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
	shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

	// 在必要的时候初始化 Intel TBB 库
	task_scheduler_init init(task_scheduler_init::deferred);
	if(conf.getHandlerType() == "tbb") {
		if(conf.getHandlerWorkerNumber() == 0) {
			init.initialize();
		} else {
			init.initialize(conf.getHandlerWorkerNumber());
		}
	}

	TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
	server.serve();
}

void run_nonblocking_server(int port, int worker_num)
{
	ScopedLoggingCtx logging_ctx("<run_nonblocking_server>");
	INFO("Starting non-blocking server: port = " + lexical_cast<string>(port)
			+ ", worker number = " + lexical_cast<string>(worker_num));

	Config &conf = Config::getInstance();
	shared_ptr<BHTIf> handler = conf.getHandlerInstance();

	shared_ptr<TProcessor> processor(new BHTProcessor(handler));

	shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

	shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(worker_num);

	if(conf.getHandlerType() == "tbb") {
		// Intel TBB 库需要在每个工作线程中创建 task_scheduler_init 实例，因此需要使用自定义的线程工厂
		shared_ptr<ThreadFactory> threadFactory(new TBBThreadFactory());
		threadManager->threadFactory(threadFactory);
	} else {
		// 使用 Thrift 自带的 POSIX 线程工厂
		shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
		threadManager->threadFactory(threadFactory);
	}

	threadManager->start();

	TNonblockingServer server(processor, protocolFactory, port, threadManager);
	server.serve();
}

