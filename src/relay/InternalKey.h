/*
 * =====================================================================================
 *
 *       Filename:  InternalKey.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/08/2009 01:32:04 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */
#ifndef BHT_RELAY_INTERNAL_KEY_H
#define BHT_RELAY_INTERNAL_KEY_H

#include "Common.h"
#include "gen-cpp/BHT_types.h"
#include <string>


BHT_RELAY_BEGIN

class InternalKey
{
public:
	static std::string ToString(const BHT::Key& k);
	static BHT::Key FromString(const std::string& k);
};


BHT_RELAY_END

#endif // BHT_RELAY_INTERNAL_KEY_H

