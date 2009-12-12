/*
 * =====================================================================================
 *
 *       Filename:  LocalHandler.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/14/2009 08:57:57 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_LOCAL_HANDLER_H
#define BHT_RELAY_LOCAL_HANDLER_H

#include "Common.h"
#include "Operation.h"
#include "gen-cpp/Local.h"

#include <string>
#include <map>
#include <vector>

BHT_RELAY_BEGIN

class LocalHandler : virtual public LocalIf {
public:
	LocalHandler() {}
	~LocalHandler() {}

	void OnSet(const std::string& domain, const Key& k, const Val& v);
	void OnDel(const std::string& domain, const Key& k);
	void OnMSet(const std::string& domain, const std::map<Key, Val> & kvs);
	void OnMDel(const std::string& domain, const std::vector<Key> & ks);
private:
	void StoreOp(Operation::Ptr op);
};

BHT_RELAY_END

#endif // BHT_RELAY_LOCAL_HANDLER_H

