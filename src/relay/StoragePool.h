/*
 * =====================================================================================
 *
 *       Filename:  StoragePool.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/23/2009 11:47:50 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_STORAGE_POOL_H
#define BHT_RELAY_STORAGE_POOL_H

#include "Pool.h"
#include "Storage.h"
#include <string>

BHT_RELAY_BEGIN

typedef Pool<Storage, Storage, const std::string&, uint32_t> StoragePool;

BHT_RELAY_END

#endif // BHT_RELAY_STORAGE_POOL_H

