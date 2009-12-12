/*
 * =====================================================================================
 *
 *       Filename:  RemoteHandler.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/15/2009 01:28:18 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_REMOTE_HANDLER_H
#define BHT_RELAY_REMOTE_HANDLER_H

#include "Common.h"
#include "gen-cpp/Remote.h"

BHT_RELAY_BEGIN

class RemoteHandler : virtual public RemoteIf
{
public:
	RemoteHandler() {}
	~RemoteHandler() {}
	void Relay(const std::string& skey, const std::string& sval);
};

BHT_RELAY_END

#endif // BHT_RELAY_REMOTE_HANDLER_H

