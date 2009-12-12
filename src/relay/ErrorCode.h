/*
 * =====================================================================================
 *
 *       Filename:  ErrorCode.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/09/2009 02:54:22 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_ERROR_CODE_H
#define BHT_RELAY_ERROR_CODE_H

#include "Common.h"

BHT_RELAY_BEGIN

namespace ErrorCode {
	enum Errno {
		OK = 0,
		INVALID_DOMAIN = -1,
		INVALID_KEY = -2,
		INVALID_TIMESTAMP = -3,
		INVALID_VALUE = -4,
		INVALID_COUNT = -5,
		INTERNAL_ERROR = -999,
	};
}

BHT_RELAY_END

#endif // BHT_RELAY_ERROR_CODE_H

