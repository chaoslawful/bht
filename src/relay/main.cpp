/*
 * =====================================================================================
 *
 *       Filename:  main.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/06/2009 03:51:06 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Framework.h"
#include "Logger.h"

DECLARE_LOGGER("BHT.Relay");

using namespace BHT::Relay;

int main(int argc, char **argv)
{
	if (Framework::Init(argc, argv)) {
		if (Framework::Start()) {
			Framework::Stop();
			Framework::Cleanup();
			return 0;
		} else {
			Framework::Cleanup();
			return 2;
		}
	}
	return 1;
}

