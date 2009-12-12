// 清理BHT后端存储中的无用数据，并进行存储系统紧缩
// 当前清理位置也记录在TokyoTyrant服务中，key 为 _clean_start_key，val 为上次清理结束位置处的 key
// _clean_start_key 不存在或对应 val 为空表示一轮清理已经完成，将从头开始下一轮清理
/**
 * TODO:
 * 	- 使用bht_tool同时进行密集型del/set/get操作时，运行bht_db_clean程序可能会导致bht_server同ttserver
 * 	之间的连接中断，使得bht_tool操作中断；在bht_server一侧的错误似乎是recv error，貌似连接上有数据同步
 * 	问题，但该问题的真正原因尚未找到。
 * 		- 关闭bht_db_clean程序删除数据的逻辑后，经反复测试，密集型set/get操作不受bht_db_clean程序影响，
 * 		但密集型del操作会被bht_db_clean程序所打断。这时bht_db_clean程序应该只进行只读的BDB遍历操作，
 * 		因此怀疑连接中断的根本原因在于BDB遍历操作。
 * 	*/
#include <vector>
#include <algorithm>
#include <boost/timer.hpp>
#include <boost/lexical_cast.hpp>

#include "BHTDbClean.h"

using namespace ::std;
using namespace ::boost;

static bool do_purge = false;	// 是否清理无用数据
static bool do_compact = false;	// 是否紧缩数据库
static int check_max_timespan = 3600 * 3;	// 遍历数据库查找无用数据最长时间
static int check_max_items = 20000;	// 内部缓存的无用数据条目 key 最多条数
static string tt_host = "localhost";	// 后端TokyoTyrant服务地址
static int tt_port = 1978;		// 后端TokyoTyrant服务端口
static string tc_path = "/home/z/var/bht/storage.tcb";	// 后端TokyoTyrant对应TCBDB数据库文件路径

int parse_cmdline(int argc, char **argv);
void bht_db_purge();
void bht_db_cleanup(const vector<string> &keys);
void bht_db_compact();

int main(int argc, char **argv)
{
	parse_cmdline(argc, argv);

	if(do_purge) {
		bht_db_purge();
	}

	if(do_compact) {
		bht_db_compact();
	}
}

int parse_cmdline(int argc, char **argv)
{
	int opt;

	while((opt = getopt(argc, argv, "?PCt:n:h:p:a:")) != -1) {
		switch(opt) {
			case 'P':	// do purge
				do_purge = true;
				break;
			case 'C':	// do compact
				do_compact = true;
				break;
			case 't':	// set check max timespan
				check_max_timespan = lexical_cast<int>(optarg);
				if(check_max_timespan < 0) {
					cerr << "Invalid maximum checking timespan: " << optarg << endl;
					exit(-1);
				}
				break;
			case 'n':	// set max cached purging item keys
				check_max_items = lexical_cast<int>(optarg);
				if(check_max_items < 0) {
					cerr << "Invalid maximum cached puring item keys: " << optarg <<endl;
					exit(-1);
				}
				break;
			case 'h':	// set tokyotyrant host
				tt_host = optarg;
				break;
			case 'p':	// set tokyotyrant port
				tt_port = lexical_cast<int>(optarg);
				if(tt_port < 0 || tt_port > 65535) {
					cerr << "Invalid tokyotyrant port: " << optarg << endl;
					exit(-1);
				}
				break;
			case 'a':	// set tokyocabinet file path used by tokyotyrant
				tc_path = optarg;
				break;
			default:
				cout << "Usage: " << argv[0] << " [-?] [-P] [-C] [-t <seconds>] [-n <num>] [-h <tt host>] [-p <tt port>] [-a <tc file path>]\n";
				exit(-1);
		}
	}

	return optind + 1;
}

void bht_db_purge()
{
	TCRDBWrapper rdb;
	TCBDBWrapper bdb;

	if(!tcrdbopen(rdb, tt_host.data(), tt_port)) {
		cerr << "tcrdbopen error: " << tcrdberrmsg(tcrdbecode(rdb)) << endl;
		return;
	}

	// 强制tt把内存中尚未回写的数据同步到磁盘上，以便在本程序中直接访问bdb文件时能看到最新变动
	if(!tcrdbsync(rdb)) {
		cerr << "tcrdbsync error: " << tcrdberrmsg(tcrdbecode(rdb)) << endl;
		return;
	}

	if(!tcbdbopen(bdb, tc_path.data(), BDBOREADER | BDBONOLCK)) {
		cerr << "tcbdbopen error: " << tcbdberrmsg(tcbdbecode(bdb)) << endl;
		return;
	}

	BDBCurWrapper cur(bdb);
	vector<string> useless_keys;
	string prev_prefix;
	char *key;
	int keysize;
	timer t;

	// 根据上次记录的结束位置重置本次扫描起始位置游标
	if((key = (char*)tcbdbget(bdb, CLEAN_POS_KEY, sizeof(CLEAN_POS_KEY) - 1, &keysize)) != NULL) {
		if(key) {
			// 上次记录的结束位置记录存在且有非空值
			string start_key(key, keysize);
			tcfree(key);

			// 重置扫描游标到上次结束处的 key
			if(!tcbdbcurjump(cur, start_key.data(), start_key.size())) {
				// 上次记录的结束位置后已经没有记录，将结束位置重置为空
				if(!tcrdbput(rdb, CLEAN_POS_KEY, sizeof(CLEAN_POS_KEY) - 1, "", 0)) {
					cerr << "tcrdbput error: " << tcrdberrmsg(tcrdbecode(rdb)) << endl;
				}
				// 从头开始新一轮扫描
				if(!tcbdbcurfirst(cur)) {
					cerr << "tcbdbcurfirst error: " << tcbdberrmsg(tcbdbecode(bdb)) << endl;
					return;
				}
			}
		}
	} else {
		// 记录结束位置的记录不存在，从头开始新一轮扫描
		if(!tcbdbcurfirst(cur)) {
			cerr << "tcbdbcurfirst error: " << tcbdberrmsg(tcbdbecode(bdb)) << endl;
			return;
		}
	}

	while(1) {
		key = (char*)tcbdbcurkey(cur, &keysize);
		if(!key) {
			// 无法获取当前游标处的 key，可能是到达数据库结束位置或出错，将结束位置重置为空
			if(!tcrdbput(rdb, CLEAN_POS_KEY, sizeof(CLEAN_POS_KEY) - 1, "", 0)) {
				cerr << "tcrdbput error: " << tcrdberrmsg(tcrdbecode(rdb)) << endl;
			}
			// 清理已缓存的待删除记录并结束清理流程
			bht_db_cleanup(useless_keys);
			break;
		}

		string k(key, keysize);
		tcfree(key);

		if(t.elapsed() >= check_max_timespan) {
			// 扫描超时，记录当前扫描位置
			if(!tcrdbput(rdb, CLEAN_POS_KEY, sizeof(CLEAN_POS_KEY) - 1, k.data(), k.size())) {
				cerr << "tcrdbput error: " << tcrdberrmsg(tcrdbecode(rdb)) << endl;
			}
			// 清理已缓存的待删除记录并结束清理流程
			bht_db_cleanup(useless_keys);
			break;
		}

		if(k.size() > 8) {
			// 剔除当前 key 结尾的 64-bit 时戳串
			string kprefix = k.substr(0, k.size() - 8);
			if(kprefix[kprefix.size() - 1] == '\001') {
				if(kprefix != prev_prefix) {
					// 当前 key 前缀同上个 key 前缀不同，说明当前 key 对应一个新的 BHT 记录
					// 记录当前 key 前缀并继续处理下条记录
					prev_prefix = kprefix;
					tcbdbcurnext(cur);
					continue;
				}

				// 当前 key 前缀同上个 key 前缀相同，说明当前 key 是上条 BHT 记录的历史数据
				// 将当前 key 添加到待删除 key 列表中
				useless_keys.push_back(k);
			}
		}

		if((int)useless_keys.size() >= check_max_items) {
			// 待删除 key 列表长度超限，清理已缓存的待删除记录并清空缓存
			bht_db_cleanup(useless_keys);
			useless_keys.clear();
		}

		tcbdbcurnext(cur);
	}
}

/**
 * 通过 TokyoTyrant 接口删除给定列表中的 key 对应的记录。
 * @param keys 待删除 key 列表。
 * */
void bht_db_cleanup(const vector<string> &keys)
{
	TCRDBWrapper rdb;

	if(!tcrdbopen(rdb, tt_host.data(), tt_port)) {
		cerr << "tcrdbopen error: " << tcrdberrmsg(tcrdbecode(rdb)) << endl;
		return;
	}

	for_each(keys.begin(), keys.end(), TCRDBRemover(rdb));
}

void bht_db_compact()
{
	TCRDBWrapper rdb;

	if(!tcrdbopen(rdb, tt_host.data(), tt_port)) {
		cerr << "tcrdbopen error: " << tcrdberrmsg(tcrdbecode(rdb)) << endl;
		return;
	}

	// XXX: 此处optimize时未设置具体的后端数据库tuning参数，若存储节点上TokyoTyrant使用的数据库tuning参数有变则需一同更改
	if(!tcrdboptimize(rdb, NULL)) {
		cerr << "tcrdboptimize error: " << tcrdberrmsg(tcrdbecode(rdb)) << endl;
		return;
	}
}

