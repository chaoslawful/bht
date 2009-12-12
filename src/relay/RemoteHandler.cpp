/*
 * =====================================================================================
 *
 *       Filename:  RemoteHandler.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/15/2009 01:29:28 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "RemoteHandler.h"
#include "Logger.h"
#include "StoragePool.h"
#include "Singleton.hpp"
#include "Framework.h"
#include "ErrorCode.h"
#include "gen-cpp/BHT.h"
#include <boost/scoped_array.hpp>

BHT_RELAY_BEGIN

DECLARE_LOGGER("BHT.Relay.RemoteHandler");

using namespace std;
using namespace boost;

void RemoteHandler::Relay(const std::string& skey, const std::string& sval)
{
	ScopedLoggingCtx lc("<RemoteHandler::Relay>");
	TRACE("RemoteHandler::Relay() - called");
	boost::scoped_array<char> decrypted_key(new char[skey.size()]);
	boost::scoped_array<char> decrypted_val(new char[sval.size()]);
	Framework::getCryptor()->Decrypt(skey.data(), decrypted_key.get(), skey.size());
	Framework::getCryptor()->Decrypt(sval.data(), decrypted_val.get(), sval.size());

	Storage::Ptr storage = Singleton<StoragePool>::Get()->Alloc();
	if (!storage->Open()) {
		ERROR("storage open failed");
		InvalidOperation e;
		e.ec = ErrorCode::INTERNAL_ERROR;
		e.msg = "Internal Error";
		throw e;
	}

	if (!storage->Put(string(decrypted_key.get(), skey.size()), string(decrypted_val.get(), sval.size()))) {
		ERROR("Relay operation failed");
		return;
	}
	Singleton<StoragePool>::Get()->Free(storage);
}

BHT_RELAY_END

