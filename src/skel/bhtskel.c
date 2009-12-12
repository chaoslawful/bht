#include <tcadb.h>
#include <tcbdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

/**
 * BHT 定制的 TokyoTyrant 记录查询接口，获取以给定 key 为前缀的首个记录值。
 * 获取 <domain, key> 对应记录时给定前缀应为 <domain> '\001' <key> '\001' '\001'
 * 获取 <domain, key, subkey> 对应记录时给定前缀应为 <domain> '\001' <key> '\001' <subkey> '\001'
 * @notes 该接口强制后端 TokyoCabinet 数据库为 BDB 形式！
 * @param adb 抽象数据库结构指针
 * @param kbuf key前缀数据串指针
 * @param ksiz key前缀数据串长度
 * @param sp 保存找到的记录值长度的整数变量指针
 * @retval 查询成功时返回记录值数据串首指针；记录不存在或查询出错时返回 NULL
 * */
static void* bhtget(TCADB *adb, const void *kbuf, int ksiz, int *sp)
{
	BDBCUR *cur;
	int ks, vs;
	void *key, *val;

	if(!adb || !adb->bdb) {
		return NULL;
	}

	cur = tcbdbcurnew(adb->bdb);

	if(tcbdbcurjump(cur, kbuf, ksiz)) {
		// 找到了 key 以给定数据串为前缀或前趋的记录
		if((key = tcbdbcurkey(cur, &ks)) != NULL) {
			// 获取当前游标位置处的 key 成功
			if(ks >= ksiz && !memcmp(key, kbuf, ksiz)) {
				// 当前 key 长度大于等于给定前缀长度，且当前 key 以给定数据串为前缀
				if((val = tcbdbcurval(cur, &vs)) != NULL) {
					// 获取当前游标位置处的 val 成功
					*sp = vs;
				}
			} else {
				val = NULL;
			}
			tcfree(key);
		} else {
			val = NULL;
		}
	} else {
		val = NULL;
	}

	tcbdbcurdel(cur);
	return val;
}

/**
 * ADB Skel 扩展入口初始化函数（必须为 C 符号）
 * @param skel 待初始化的ADB存储框架结构指针
 * @retval 初始化成功时返回true；否则返回false
 * */
bool initialize(ADBSKEL *skel)
{
	skel->opq = tcadbnew();
	skel->del = (void (*)(void*))tcadbdel;
	skel->open = (bool (*)(void*, const char*))tcadbopen;
	skel->close = (bool (*)(void*))tcadbclose;
	skel->put = (bool (*)(void*, const void*, int, const void*, int))tcadbput;
	skel->putkeep = (bool (*)(void*, const void*, int, const void*, int))tcadbputkeep;
	skel->putcat = (bool (*)(void*, const void*, int, const void*, int))tcadbputcat;
	skel->out = (bool (*)(void*, const void*, int))tcadbout;

	skel->get = (void* (*)(void*, const void*, int, int*))bhtget;	// 注册 BHT 定制版本的记录查询接口

	skel->vsiz = (int (*)(void*, const void*, int))tcadbvsiz;
	skel->iterinit = (bool (*)(void*))tcadbiterinit;
	skel->iternext = (void* (*)(void*, int*))tcadbiternext;
	skel->fwmkeys = (TCLIST* (*)(void*, const void*, int, int))tcadbfwmkeys;
	skel->addint = (int (*)(void*, const void*, int, int))tcadbaddint;
	skel->adddouble = (double (*)(void*, const void*, int, double))tcadbadddouble;
	skel->sync = (bool (*)(void*))tcadbsync;
	skel->optimize = (bool (*)(void*, const char*))tcadboptimize;
	skel->vanish = (bool (*)(void*))tcadbvanish;
	skel->copy = (bool (*)(void*, const char*))tcadbcopy;
	skel->tranbegin = (bool (*)(void*))tcadbtranbegin;
	skel->trancommit = (bool (*)(void*))tcadbtrancommit;
	skel->tranabort = (bool (*)(void*))tcadbtranabort;
	skel->path = (const char* (*)(void*))tcadbpath;
	skel->rnum = (uint64_t (*)(void*))tcadbrnum;
	skel->size = (uint64_t (*)(void*))tcadbsize;
	skel->misc = (TCLIST* (*)(void*, const char*, const TCLIST*))tcadbmisc;
	skel->putproc = (bool (*)(void*, const void*, int, const void*, int, TCPDPROC, void*))tcadbputproc;
	skel->foreach = (bool (*)(void*, TCITER, void*))tcadbforeach;
	return true;
}

