/*
 * =====================================================================================
 *
 *       Filename:  Framework.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/09/2009 06:26:09 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Framework.h"
#include "Logger.h"
#include "Config.h"
#include "Logger.h"
#include "StoragePool.h"

#include <string>
#include <sstream>
#include <iostream>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <sys/types.h>
#include <sys/fcntl.h>

BHT_RELAY_BEGIN

DECLARE_LOGGER("BHT.Relay.Framework");

using namespace std;
using namespace boost;

Service::Ptr Framework::service_;
TargetPoolMgr::Ptr Framework::target_pool_mgr_;
Cryptor::Ptr Framework::cryptor_;

Mode::Type Framework::mode_ = Mode::NONE;
bool Framework::daemon_ = false;
string Framework::config_file_;

bool Framework::Init(int argc, char** argv)
{
	ParseOpt(argc, argv);
	if (!Singleton<Config>::Create(config_file_)) {
		return false;
	}
	Singleton<Config>::Get()->Configure();
	Logger::Init(Singleton<Config>::Get()->LoggerConfig());
	cryptor_.reset(new TripleDESCryptor(Singleton<Config>::Get()->SecretKey()));

	Daemonize();
	
	Operation::AddFactory<OpSet>();
	Operation::AddFactory<OpDel>();
	Operation::AddFactory<OpMSet>();
	Operation::AddFactory<OpMDel>();

	if (!Singleton<StoragePool>::Create(Singleton<Config>::Get()->StorageAddr().first, Singleton<Config>::Get()->StorageAddr().second)) {
		return false;
	}
	if (mode_ == Mode::LOCAL) {
		if (!Singleton<LocalService>::Create(Singleton<Config>::Get()->ServicePort(), Singleton<Config>::Get()->ServiceThreads())) {
			return false;
		}
		if (!Singleton<RemoteTargetPoolMgr>::Create()) {
			return false;
		}
		service_ = Singleton<LocalService>::Get();
		target_pool_mgr_ = Singleton<RemoteTargetPoolMgr>::Get();
	} else if (mode_ == Mode::REMOTE) {
		if (!Singleton<RemoteService>::Create(Singleton<Config>::Get()->ServicePort(), Singleton<Config>::Get()->ServiceThreads())) {
			return false;
		}
		if (!Singleton<BHTTargetPoolMgr>::Create()) {
			return false;
		}
		target_pool_mgr_ = Singleton<BHTTargetPoolMgr>::Get();
		service_ = Singleton<RemoteService>::Get();
	} else {
		// impossible
		assert(0);
	}
	if (!Singleton<WorkerMgr>::Create(Singleton<Config>::Get()->WorkerThreads(), Singleton<Config>::Get()->WorkerSleeps())) {
		return false;
	}
	const std::vector<Config::AddrType>& target_addrs = Singleton<Config>::Get()->TargetAddrs();
	std::vector<Config::AddrType>::const_iterator it = target_addrs.begin(), end = target_addrs.end();
	for (; it!=end; ++it) {
		target_pool_mgr_->MakePool(it->first, it->second);
	}
	return true;
}

void Framework::Cleanup()
{
	Singleton<WorkerMgr>::Destroy();
	if (mode_ == Mode::LOCAL) {
		Singleton<LocalService>::Destroy();
		Singleton<RemoteTargetPoolMgr>::Destroy();
	} else {
		Singleton<RemoteService>::Destroy();
		Singleton<BHTTargetPoolMgr>::Destroy();
	}
	target_pool_mgr_.reset();
	Singleton<Config>::Destroy();
	service_.reset();
}

bool Framework::Start()
{
	if (!Singleton<WorkerMgr>::Get()->Start()) {
		return false;
	}
	if (!service_->Start()) {
		return false;
	}
	return true;
}

void Framework::Stop()
{
	Singleton<WorkerMgr>::Get()->Stop();
}

void Framework::ParseOpt(int argc, char** argv)
{
	int opt;
	while ((opt = getopt(argc, argv, ":hdm:c:")) != -1) {
		switch(opt) {
		case 'm':
		{
			string mode_str = boost::to_lower_copy(boost::trim_copy(string(optarg)));
			if (mode_str == "l" || mode_str == "local") {
				mode_ = Mode::LOCAL;
			} else if (mode_str == "r" || mode_str == "remote") {
				mode_ = Mode::REMOTE;
			} else {
				Usage(argv[0], "Invalid relay mode: " + mode_str + ".");
			}
			break;
		}
		case 'd':
			daemon_ = true;
			break;
		case 'c':
			config_file_ = optarg;
			break;
		case ':':
		{
			stringstream err;
			err << "-" << static_cast<char>(optopt) << " without argument.";
			Usage(argv[0], err.str());
			break;
		}
		case '?':
		{
			stringstream err;
			err << "Unknown argument " << static_cast<char>(optopt);
			Usage(argv[0], err.str());
			break;
		}
		case 'h':
		default:
			Usage(argv[0]);
			break;
		}
	}
	if (mode_ == Mode::NONE) {
		Usage(argv[0], "Relay mode is unspecified.");
	}
	if (config_file_.empty()) {
		if (mode_ == Mode::LOCAL) {
			config_file_ = BHT_RELAY_ROOT_PATH "/" BHT_RELAY_CONF_PATH "/" BHT_RELAY_LOCAL_CONF_FILE;
		} else {
			config_file_ = BHT_RELAY_ROOT_PATH "/" BHT_RELAY_CONF_PATH "/" BHT_RELAY_REMOTE_CONF_FILE;
		}
	}
}

void Framework::Usage(const std::string& prog, const std::string& err)
{
	if (!err.empty()) {
		cout << err << endl;
	}
	cout <<"Usage: " << prog << " -m <mode> [-c <file>] [-d]" << endl
		<<"\t" << "-m <mode>\tselect relay mode, \"local\" or \"remote\"" << endl
		<<"\t" << "-c <file>\tuse <file> as the relay config file." <<endl
		<<"\t" << "\t\tdefault config file is " << BHT_RELAY_ROOT_PATH "/" BHT_RELAY_CONF_PATH "/" << "<mode>.conf" <<endl
		<<"\t" << "-d\t\trun as daemon." <<endl
		<<"\t" << "-h\t\tshow this message." << endl
		<<endl;
	if (err.empty()) {
		exit(0);
	} else {
		exit(1);
	}
}

void Framework::Daemonize()
{
	if (!daemon_) {
		return ;
	}
	pid_t pid;

	// fork 1
	if(fork()) {
		// exit parent
		exit(0);
	}

	// separates child from parent
	if (chdir("/") != 0) {
	}
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

BHT_RELAY_END

