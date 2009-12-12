#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <thrift/transport/TSocket.h>
#include "BHTSocket.h"

BHT_CODE_BEGIN

using namespace ::std;
using namespace ::apache::thrift::transport;

BHTSocket::BHTSocket()
	:TSocket()
{
}

BHTSocket::BHTSocket(string host, int port, int conn_timeout, int send_timeout, int recv_timeout)
	:TSocket(host, port)
{
	connTimeout_ = conn_timeout;
	sendTimeout_ = send_timeout;
	recvTimeout_ = recv_timeout;
}

bool BHTSocket::ping() const
{
	uint8_t buf;
	int r = recv(socket_, &buf, sizeof(buf), MSG_PEEK | MSG_DONTWAIT);
	if((r < 0  && errno != EAGAIN) || r == 0) {
		return false;
	}
	return true;
}

BHT_CODE_END

