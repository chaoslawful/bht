/*
 * =====================================================================================
 *
 *       Filename:  Mode.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/22/2009 10:40:45 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_MODE_H
#define BHT_RELAY_MODE_H

BHT_RELAY_BEGIN

namespace Mode {
	enum Type {
		NONE = 0,
		LOCAL = 1,
		REMOTE = 2
	};
}

BHT_RELAY_END

#endif // BHT_RELAY_MODE_H

