#ifndef BHT_COMMON_H__
#define BHT_COMMON_H__

#define BHT_CODE_BEGIN namespace BHT {
#define BHT_CODE_END }

// 设置当前 us 级 UNIX 时戳到给定的目标中(目标应为 int64_t 类型)
#define BHT_SET_CUR_EPOCH(v) do {\
	struct timeval tv;\
	gettimeofday(&tv, NULL);\
	(v) = tv.tv_sec * 1000000LL + tv.tv_usec;\
} while(0)

#include <string>
#include <vector>
#include <map>
#include <list>

#include <boost/shared_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/functional/hash.hpp>

#include <thrift/Thrift.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/transport/TSocket.h>

#include <sys/time.h>
#include <time.h>

#include "gen-cpp/BHT.h"

#endif

