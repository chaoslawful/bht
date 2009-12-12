/*
 * =====================================================================================
 *
 *       Filename:  TargetPoolMgr.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/29/2009 10:27:48 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_TARGET_POOL_MGR_H
#define BHT_RELAY_TARGET_POOL_MGR_H

#include "Target.h"
#include "PoolMgr.h"

BHT_RELAY_BEGIN

typedef PoolMgrBase<Target, const std::string&, uint32_t> TargetPoolMgr;
typedef PoolMgr<RemoteTarget, Target, const std::string&, uint32_t> RemoteTargetPoolMgr;
typedef PoolMgr<BHTTarget, Target, const std::string&, uint32_t> BHTTargetPoolMgr;

BHT_RELAY_END

#endif // BHT_RELAY_TARGET_POOL_MGR_H

