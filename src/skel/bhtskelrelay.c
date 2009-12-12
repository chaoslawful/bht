/*
 * =====================================================================================
 *
 *       Filename:  bhtskelrelay.c
 *
 *    Description:  relay模块使用tcbdb作为后端存储，原始的tcrdb无putdup接口，用以改造put接口为putdup接口。
 *
 *        Version:  1.0
 *        Created:  07/07/2009 04:39:30 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include <tcadb.h>
#include <tcbdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <assert.h>

bool bht_relay_putdup(TCADB *adb, const void *kbuf, int ksiz, const void *vbuf, int vsiz){
	assert(adb && kbuf && ksiz >= 0 && vbuf && vsiz >= 0);
	assert(adb->omode == ADBOBDB);
	return tcbdbputdup(adb->bdb, kbuf, ksiz, vbuf, vsiz);
}

/*************************************************************************************************
 * API
 *************************************************************************************************/

bool initialize(ADBSKEL *skel){
	skel->opq = tcadbnew();
	skel->del = (void (*)(void *))tcadbdel;
	skel->open = (bool (*)(void *, const char *))tcadbopen;
	skel->close = (bool (*)(void *))tcadbclose;

	// the putdup interface
	skel->put = (bool (*)(void *, const void *, int, const void *, int))bht_relay_putdup;

	skel->putkeep = (bool (*)(void *, const void *, int, const void *, int))tcadbputkeep;
	skel->putcat = (bool (*)(void *, const void *, int, const void *, int))tcadbputcat;
	skel->out = (bool (*)(void *, const void *, int))tcadbout;
	skel->get = (void *(*)(void *, const void *, int, int *))tcadbget;
	skel->vsiz = (int (*)(void *, const void *, int))tcadbvsiz;
	skel->iterinit = (bool (*)(void *))tcadbiterinit;
	skel->iternext = (void *(*)(void *, int *))tcadbiternext;
	skel->fwmkeys = (TCLIST *(*)(void *, const void *, int, int))tcadbfwmkeys;
	skel->addint = (int (*)(void *, const void *, int, int))tcadbaddint;
	skel->adddouble = (double (*)(void *, const void *, int, double))tcadbadddouble;
	skel->sync = (bool (*)(void *))tcadbsync;
	skel->optimize = (bool (*)(void *, const char *))tcadboptimize;
	skel->vanish = (bool (*)(void *))tcadbvanish;
	skel->copy = (bool (*)(void *, const char *))tcadbcopy;
	skel->tranbegin = (bool (*)(void *))tcadbtranbegin;
	skel->trancommit = (bool (*)(void *))tcadbtrancommit;
	skel->tranabort = (bool (*)(void *))tcadbtranabort;
	skel->path = (const char *(*)(void *))tcadbpath;
	skel->rnum = (uint64_t (*)(void *))tcadbrnum;
	skel->size = (uint64_t (*)(void *))tcadbsize;
	skel->misc = (TCLIST *(*)(void *, const char *, const TCLIST *))tcadbmisc;
	skel->putproc =
		(bool (*)(void *, const void *, int, const void *, int, TCPDPROC, void *))tcadbputproc;
	skel->foreach = (bool (*)(void *, TCITER, void *))tcadbforeach;
	return true;
}


