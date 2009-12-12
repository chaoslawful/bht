/*
 * =====================================================================================
 *
 *       Filename:  Compare.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/27/2009 01:49:53 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "gen-cpp/BHT_types.h"

bool BHT::Key::operator < (BHT::Key const& k) const {
	if (key < k.key) {
		return true;
	}
	if (key == k.key) {
		if (subkey < k.subkey) {
			return true;
		}
		if (subkey == k.subkey && timestamp < k.timestamp) {
			return true;
		}
	}
	return false;
}

