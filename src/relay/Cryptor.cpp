/*
 * =====================================================================================
 *
 *       Filename:  Cryptor.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/22/2009 02:55:43 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */

#include "Cryptor.h"
#include "Logger.h"

BHT_RELAY_BEGIN

DECLARE_LOGGER("BHT.Relay.Cryptor");

TripleDESCryptor::TripleDESCryptor(const std::string& key)
{
	if (key.size() == KEY_LEN*3) {
		InitKeys(key);
	} else {
		std::string key24(key);
		if (key24.size() < KEY_LEN*3) {
			key24.append(KEY_LEN*3 - key24.size(), '\0');
		} else {
			key24.erase(KEY_LEN*3, std::string::npos);
		}
		InitKeys(key24);
	}
}

void TripleDESCryptor::InitKeys(const std::string& key24)
{
	BOOST_ASSERT(key24.size() == KEY_LEN*3);
	DES_cblock k;
	key24.copy((char*)&k, KEY_LEN);
	DES_set_key_unchecked(&k, &ks1_);
	key24.copy((char*)&k, KEY_LEN, KEY_LEN);
	DES_set_key_unchecked(&k, &ks2_);
	key24.copy((char*)&k, KEY_LEN, KEY_LEN*2);
	DES_set_key_unchecked(&k, &ks3_);
}

void TripleDESCryptor::Encrypt(const char* input, char* output, size_t length) const
{
	int n = 0;
	DES_cblock ivec = {0};
	DES_key_schedule ks1(ks1_);
	DES_key_schedule ks2(ks2_);
	DES_key_schedule ks3(ks3_);
	DES_ede3_cfb64_encrypt((const unsigned char*)input, (unsigned char*)output, length, &ks1, &ks2, &ks3, &ivec, &n, DES_ENCRYPT);
}

void TripleDESCryptor::Decrypt(const char* input, char* output, size_t length) const
{
	int n = 0;
	DES_cblock ivec = {0};
	DES_key_schedule ks1(ks1_);
	DES_key_schedule ks2(ks2_);
	DES_key_schedule ks3(ks3_);
	DES_ede3_cfb64_encrypt((const unsigned char*)input, (unsigned char*)output, length, &ks1, &ks2, &ks3, &ivec, &n, DES_DECRYPT);
}

BHT_RELAY_END

