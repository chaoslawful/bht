#ifndef BHTDBCLEAN_H__
#define BHTDBCLEAN_H__

#include <iostream>
#include <string>

#include <unistd.h>
#include <tcutil.h>
#include <tcrdb.h>
#include <tcbdb.h>
#include <stdlib.h>
#include <stdint.h>

#define CLEAN_POS_KEY "_clean_start_key"

// 对TCBDB的RAII包装
class TCBDBWrapper {
public:
	TCBDBWrapper()
	{
		_bdb = tcbdbnew();
	}

	~TCBDBWrapper()
	{
		tcbdbclose(_bdb);
		tcbdbdel(_bdb);
	}

	operator TCBDB* () const
	{
		return _bdb;
	}

private:
	TCBDB *_bdb;
};

// 对BDBCUR的RAII包装
class BDBCurWrapper {
public:
	BDBCurWrapper(TCBDB *bdb)
	{
		_cur = tcbdbcurnew(bdb);
	}

	~BDBCurWrapper()
	{
		tcbdbcurdel(_cur);
	}

	operator BDBCUR* () const
	{
		return _cur;
	}

private:
	BDBCUR *_cur;
};

// 对TCRDB的RAII包装
class TCRDBWrapper {
public:
	TCRDBWrapper()
	{
		_rdb = tcrdbnew();
	}

	~TCRDBWrapper()
	{
		tcrdbclose(_rdb);
		tcrdbdel(_rdb);
	}

	operator TCRDB* () const
	{
		return _rdb;
	}

private:
	TCRDB *_rdb;
};

// TCRDB删除操作函子
class TCRDBRemover {
public:
	TCRDBRemover(const TCRDBWrapper &rdb):_rdb(rdb) {}

	void operator() (const ::std::string &key)
	{
		if(!tcrdbout(_rdb, key.data(), key.size())) {
			::std::cerr << "tcrdbout error: " << tcrdberrmsg(tcrdbecode(_rdb)) << ::std::endl;
		}
	}

private:
	const TCRDBWrapper &_rdb;
};

#endif

