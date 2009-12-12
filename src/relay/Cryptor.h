/*
 * =====================================================================================
 *
 *       Filename:  Cryptor.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/22/2009 11:45:37 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#ifndef BHT_RELAY_CRYPTOR_H
#define BHT_RELAY_CRYPTOR_H

#include "Common.h"
#include <boost/shared_ptr.hpp>
#include <openssl/des.h>

BHT_RELAY_BEGIN

class Cryptor
{
public:
	typedef boost::shared_ptr<Cryptor> Ptr;
	virtual ~Cryptor() {}

	virtual void Encrypt(const char* input, char* output, size_t length) const = 0;
	virtual void Decrypt(const char* input, char* output, size_t length) const = 0;
protected:
	const static size_t KEY_LEN = 8;
};

class TripleDESCryptor : virtual public Cryptor
{
public:
	TripleDESCryptor(const std::string& key);

	void Encrypt(const char* input, char* output, size_t length) const;
	void Decrypt(const char* input, char* output, size_t length) const;
private:
	void InitKeys(const std::string& key24);
private:
	DES_key_schedule ks1_;
	DES_key_schedule ks2_;
	DES_key_schedule ks3_;
};

BHT_RELAY_END


#endif // BHT_RELAY_CRYPTOR_H

