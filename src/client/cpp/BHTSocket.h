#ifndef BHT_BHTSOCKET_H__
#define BHT_BHTSOCKET_H__

#include <thrift/transport/TSocket.h>
#include "Common.h"

BHT_CODE_BEGIN

class BHTSocket:virtual public ::apache::thrift::transport::TSocket {
public:
	BHTSocket();
	BHTSocket(::std::string host, int port,
			int conn_timeout = 0,
			int send_timeout = 0,
			int recv_timeout = 0);
	bool ping() const;
};

BHT_CODE_END

#endif

