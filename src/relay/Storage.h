/*
 * =====================================================================================
 *
 *       Filename:  Storage.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/08/2009 11:39:14 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjie.li@alibaba-inc.com
 *        Company:  Yahoo! CN
 *
 * =====================================================================================
 */
#ifndef BHT_RELAY_STORAGE_H
#define BHT_RELAY_STORAGE_H

#include "Common.h"
#include <tcrdb.h>
#include <string>
#include <iterator>
#include <boost/shared_ptr.hpp>

BHT_RELAY_BEGIN

class Storage
{
public:
	class iterator : public std::iterator<std::input_iterator_tag, std::string>
	{
		friend class Storage;
	private:
		explicit iterator(TCRDB* rdb);
	public:
		iterator();
		const value_type& operator*() const;
		const std::string* operator->() const {
			return &(operator*());
		}
		iterator& operator++();
		iterator operator++(int) {
			iterator tmp(*this);
			operator++();
			return tmp;
		}
		bool operator==(const iterator& rhs) const {
			return (ok_ == rhs.ok_) && (!ok_ || rdb_ == rhs.rdb_);
		}
		bool operator!=(const iterator& rhs) const {
			return !operator==(rhs);
		}
	private:
		TCRDB* rdb_;
		std::string val_;
		bool ok_;
	};

	iterator begin() { BOOST_ASSERT(rdb_); return iterator(rdb_); }
	iterator end() { BOOST_ASSERT(rdb_); return iterator(); }
public:
	typedef boost::shared_ptr<Storage> Ptr;
	Storage(const std::string& host, uint32_t port);
	~Storage();

	const std::string& host() const  { return host_; }
	const uint32_t port() const { return port_; }

	bool Open();
	void Close();

	bool Put(const std::string& skey, const std::string& sval) {
		BOOST_ASSERT(rdb_);
		return tcrdbput(rdb_, skey.data(), skey.size(), sval.data(), sval.size());
	}
	bool Del(const std::string& skey) {
		BOOST_ASSERT(rdb_);
		return tcrdbout(rdb_, skey.data(), skey.size());
	}
	boost::shared_ptr<char> Get(const std::string& skey, size_t& vsiz);
private:
	const std::string host_;
	const uint32_t port_;
	TCRDB* rdb_;
};

BHT_RELAY_END

#endif // BHT_RELAY_STORAGE_H

