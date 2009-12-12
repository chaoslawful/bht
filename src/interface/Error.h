#ifndef BHT_ERROR_H__
#define BHT_ERROR_H__

#include "Common.h"

BHT_CODE_BEGIN

class Error {
public:
	enum {
		BHT_EC_OK = 0,
		BHT_EC_INVALID_DOMAIN = -1,
		BHT_EC_INVALID_KEY = -2,
		BHT_EC_INVALID_SUBKEY = -3,
		BHT_EC_INVALID_VALUE = -4,
		BHT_EC_OUT_OF_MEMORY = -5,
		BHT_EC_TUNE_BACKEND = -6,
		BHT_EC_CONNECT_BACKEND = -7,
		BHT_EC_BACKEND_ERROR = -8
	};
};

BHT_CODE_END

#endif

