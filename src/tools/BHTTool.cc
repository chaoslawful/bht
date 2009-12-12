#include <string>
#include <iostream>

#include <boost/lexical_cast.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/progress.hpp>

#include <sys/time.h>
#include <time.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "gen-cpp/BHT.h"

#define DEFAULT_SERVER_HOST "localhost"
#define DEFAULT_SERVER_PORT 9090
#define DEFAULT_REPEAT_TIMES 1

using namespace ::std;
using namespace ::boost;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::BHT;

typedef enum {UNKNOWN, SET, GET, DEL, MSET, MGET, MDEL, UPDATE} OpType;

int parse_cmdline(int argc, char **argv);

static OpType op = UNKNOWN;
static string host = DEFAULT_SERVER_HOST;
static int port = DEFAULT_SERVER_PORT;
static int repeat = DEFAULT_REPEAT_TIMES;
static bool verbose = false;

class Timer {
public:
	Timer()
	{
		gettimeofday(&_start, NULL);
	}

	~Timer() {}

	double elapsed() const
	{
		struct timeval _end;
		gettimeofday(&_end, NULL);
		return (_end.tv_sec - _start.tv_sec) + 1e-6 * (_end.tv_usec - _start.tv_usec);
	}

private:
	struct timeval _start;
};

int main(int argc, char **argv)
{
	int arg_idx = parse_cmdline(argc, argv);

	shared_ptr<TTransport> socket(new TSocket(host, port));
	shared_ptr<TTransport> transport(new TFramedTransport(socket));
	shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	BHTClient client(protocol);

	try {
		transport->open();

		switch(op) {
			case SET:
				if(arg_idx > argc - 4) {
					cerr << "Lost command arguments, must have <domain>, <key>, <subkey>, <val>\n";
					break;
				}
				{
					string domain(argv[arg_idx]);
					string key(argv[arg_idx + 1]);
					string subkey(argv[arg_idx + 2]);
					string val(argv[arg_idx + 3]);

					Key k;
					k.key = key;
					k.subkey = subkey;
					k.__isset.key = true;
					k.__isset.subkey = true;

					Val v;
					v.value = val;
					v.__isset.value = true;

					shared_ptr<progress_display> prog;
					if(verbose) {
						prog = shared_ptr<progress_display>(new progress_display(repeat,cout,"\n","\033[32;1m","\033[31;1m"));
					}

					Timer timer;
					for(int i=0; i < repeat; ++i) {
						client.Set(domain, k, v, true/*overwrite*/, true/*relay*/);
						if(verbose) {
							++(*prog);
						}
					}
					cout<<"\033[0m";

					double elapsed = timer.elapsed();
					cout << "Total time (" << repeat << " times): " << elapsed << " s, per op: " << elapsed / repeat << " s\n";
				}
				break;
			case GET:
				if(arg_idx > argc - 3) {
					cerr << "Lost command arguments, must have <domain>, <key>, <subkey>\n";
					break;
				}
				{
					string domain(argv[arg_idx]);
					string key(argv[arg_idx + 1]);
					string subkey(argv[arg_idx + 2]);

					Key k;
					k.key = key;
					k.subkey = subkey;
					k.__isset.key = true;
					k.__isset.subkey = true;

					Val v;

					shared_ptr<progress_display> prog;
					if(verbose) {
						prog = shared_ptr<progress_display>(new progress_display(repeat,cout,"\n","\033[32;1m","\033[31;1m"));
					}

					Timer timer;
					for(int i=0; i < repeat; ++i) {
						client.Get(v, domain, k);
						if(verbose) {
							++(*prog);
						}
					}
					cout<<"\033[0m";

					double elapsed = timer.elapsed();

					if(v.__isset.value) {
						cout << "Found: " << v.value << endl;
					} else {
						cout << "Not found!\n";
					}

					cout << "Total time (" << repeat << " times): " << elapsed << " s, per op: " << elapsed / repeat << " s\n";
				}
				break;
			case DEL:
				if(arg_idx > argc - 3) {
					cerr << "Lost command arguments, must have <domain>, <key>, <subkey>\n";
					break;
				}
				{
					string domain(argv[arg_idx]);
					string key(argv[arg_idx + 1]);
					string subkey(argv[arg_idx + 2]);

					Key k;
					k.key = key;
					k.subkey = subkey;
					k.__isset.key = true;
					k.__isset.subkey = true;

					shared_ptr<progress_display> prog;
					if(verbose) {
						prog = shared_ptr<progress_display>(new progress_display(repeat,cout,"\n","\033[32;1m","\033[31;1m"));
					}

					Timer timer;
					for(int i=0; i < repeat; ++i) {
						client.Del(domain, k, true/*overwrite*/, true/*relay*/);
						if(verbose) {
							++(*prog);
						}
					}
					cout<<"\033[0m";

					double elapsed = timer.elapsed();
					cout << "Total time (" << repeat << " times): " << elapsed << " s, per op: " << elapsed / repeat << " s\n";
				}
				break;
			case MSET:
				// syntax: mset <domain> <k1> <sk1> <v1> <k2> <sk2> <v2> <k3> <sk3> <v3> ...
				if((argc - arg_idx) < 4 || (argc - arg_idx) % 3 != 1) {
					cerr << "Incorrect number of command, syntax: mset <domain> <k1> <sk1> <v1> <k2> <sk2> <v2> <k3> <sk3> <v3> ...\n";
					break;
				}
				{
					string domain(argv[arg_idx]);
					map<Key, Val> kvs;
					for(int i = arg_idx + 1; i < argc; i += 3) {
						Key k;
						k.key = argv[i];
						k.subkey = argv[i+1];
						k.__isset.key = true;
						k.__isset.subkey = true;
						Val v;
						v.value = argv[i+2];
						v.__isset.value = true;
						kvs[k] = v;
					}
					shared_ptr<progress_display> prog;
					if(verbose) {
						prog = shared_ptr<progress_display>(new progress_display(repeat,cout,"\n","\033[32;1m","\033[31;1m"));
					}

					Timer timer;
					for(int i=0; i < repeat; ++i) {
						client.MSet(domain, kvs, true/*overwrite*/, true/*relay*/);
						if(verbose) {
							++(*prog);
						}
					}
					cout<<"\033[0m";

					double elapsed = timer.elapsed();
					cout << "Total time (" << repeat << " times): " << elapsed << " s, per op: " << elapsed / repeat << " s\n";
				}
				break;
			case MGET:
				// syntax: mget <domain> <k1> <sk1> <k2> <sk2> <k3> <sk3> ...
				if((argc - arg_idx) < 3 || (argc - arg_idx) % 2 != 1) {
					cerr << "Incorrect number of command, syntax: mget <domain> <k1> <sk1> <k2> <sk2> <k3> <sk3> ...\n";
					break;
				}
				{
					string domain(argv[arg_idx]);
					vector<Key> ks;
					map<Key, Val> kvs;
					for(int i = arg_idx + 1; i < argc; i += 2) {
						Key k;
						k.key = argv[i];
						k.subkey = argv[i+1];
						k.__isset.key = true;
						k.__isset.subkey = true;
						ks.push_back(k);
					}
					shared_ptr<progress_display> prog;
					if(verbose) {
						prog = shared_ptr<progress_display>(new progress_display(repeat,cout,"\n","\033[32;1m","\033[31;1m"));
					}

					Timer timer;
					for(int i=0; i < repeat; ++i) {
						kvs.clear();
						client.MGet(kvs, domain, ks);
						if(verbose) {
							++(*prog);
						}
					}
					cout<<"\033[0m";

					double elapsed = timer.elapsed();

					for(map<Key, Val>::iterator it = kvs.begin(); it != kvs.end(); ++it) {
						Key k = it->first;
						Val v = it->second;
						cout << "('" << k.key << "', '" << k.subkey << "') -> '" << v.value << "'\n";
					}

					cout << "Total time (" << repeat << " times): " << elapsed << " s, per op: " << elapsed / repeat << " s\n";
				}
				break;
			case MDEL:
				// syntax: mdel <domain> <k1> <sk1> <k2> <sk2> <k3> <sk3> ...
				if((argc - arg_idx) < 3 || (argc - arg_idx) % 2 != 1) {
					cerr << "Incorrect number of command, syntax: mdel <domain> <k1> <sk1> <k2> <sk2> <k3> <sk3> ...\n";
					break;
				}
				{
					string domain(argv[arg_idx]);
					vector<Key> ks;
					for(int i = arg_idx + 1; i < argc; i += 2) {
						Key k;
						k.key = argv[i];
						k.subkey = argv[i+1];
						k.__isset.key = true;
						k.__isset.subkey = true;
						ks.push_back(k);
					}
					shared_ptr<progress_display> prog;
					if(verbose) {
						prog = shared_ptr<progress_display>(new progress_display(repeat,cout,"\n","\033[32;1m","\033[31;1m"));
					}

					Timer timer;
					for(int i=0; i < repeat; ++i) {
						client.MDel(domain, ks, true/*overwrite*/, true/*relay*/);
						if(verbose) {
							++(*prog);
						}
					}
					cout<<"\033[0m";

					double elapsed = timer.elapsed();
					cout << "Total time (" << repeat << " times): " << elapsed << " s, per op: " << elapsed / repeat << " s\n";
				}
				break;
			case UPDATE:
				{
					shared_ptr<progress_display> prog;
					if(verbose) {
						prog = shared_ptr<progress_display>(new progress_display(repeat,cout,"\n","\033[32;1m","\033[31;1m"));
					}

					Timer timer;
					for(int i=0; i<repeat; ++i) {
						client.UpdateConfig();
						if(verbose) {
							++(*prog);
						}
					}
					cout<<"\033[0m";

					double elapsed = timer.elapsed();
					cout << "Total time (" << repeat << " times): " << elapsed << " s, per op: " << elapsed / repeat << " s\n";
				}
				break;
			default:
				cerr << "Unknown command: " << (int)op << endl;
		}

		transport->close();
	} catch(const InvalidOperation &tx) {
		cerr << "BHT error: " << tx.msg << endl;
	} catch(const TException &tx) {
		cerr << "Error: "<<tx.what()<<endl;
	}
}

int parse_cmdline(int argc, char **argv)
{
	int opt;

	while((opt = getopt(argc, argv, "?vh:p:n:")) != -1) {
		switch(opt) {
			case 'h':	// set server host
				host = string(optarg);
				break;
			case 'p':	// set server port
				port = lexical_cast<int>(optarg);
				if(port < 0 || port > 65535) {
					cerr << "Invalid server port: " << port << endl;
					exit(-1);
				}
				break;
			case 'n':	// set repeat times
				repeat = lexical_cast<int>(optarg);
				if(repeat < 1) {
					cerr << "Invalid repeat times: " << repeat <<endl;
					exit(-1);
				}
				break;
			case 'v':	// show progress bar
				verbose = true;
				break;
			default:
				cout << "Usage: " << argv[0] << " [-?] [-v] [-n <repeat times>] [-h <host>] [-p <port>] <cmd> <args>\n";
				exit(-1);
		}
	}

	if(optind >= argc) {
		cerr << "No command given!\n";
		exit(-1);
	}

	string cmd(argv[optind]);
	if(cmd == "set") {
		op = SET;
	} else if(cmd == "get") {
		op = GET;
	} else if(cmd == "del") {
		op = DEL;
	} else if(cmd == "mset") {
		op = MSET;
	} else if(cmd == "mget") {
		op = MGET;
	} else if(cmd == "mdel") {
		op = MDEL;
	} else if(cmd == "update") {
		op = UPDATE;
	} else {
		cerr << "Unknown command: " << cmd << endl;
		exit(-1);
	}

	return optind + 1;
}

