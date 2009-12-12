#ifndef BHT_VALIDATOR_H__
#define BHT_VALIDATOR_H__

#include <string>

#include "Common.h"

BHT_CODE_BEGIN

class Validator {
	static const char domain_valid_charset[];
	static const char key_valid_charset[];
	static const char subkey_valid_charset[];
	static bool verifyLengthAndCharset(const ::std::string &str, size_t minlen, size_t maxlen, const char *valid_charset);
public:
	static bool isValidDomain(const ::std::string &domain);
	static bool isValidKey(const ::std::string &key);
	static bool isValidSubkey(const ::std::string &subkey);
	static bool isValidValue(const ::std::string &value);
};

BHT_CODE_END

#endif

