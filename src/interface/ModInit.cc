#include "ModInit.h"
#include "Config.h"
#include "Cache.h"
#include "Logging.h"
#include "RelayClient.h"

BHT_CODE_BEGIN

using namespace ::std;

ModInit ModInit::_inst;

ModInit::ModInit()
{
	(void)Config::getInstance();
	(void)Cache::getInstance();
	(void)RelayClient::getInstance();
}

ModInit::~ModInit()
{
	(void)RelayClient::destroyInstance();
	(void)Cache::destroyInstance();
	(void)Config::destroyInstance();
}

BHT_CODE_END

