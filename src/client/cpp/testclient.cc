#include <iostream>
#include <cstdlib>
#include "Client.h"

using namespace ::std;
using namespace ::BHT;

int main(int argc, char *argv[])
{
	if(argc != 3) {
		cerr << "Usage: " << argv[0] << " <host> <port>\n";
		exit(1);
	}

	Client cli("test", argv[1], atoi(argv[2]));
	
	try {

		cli.open();

		cout << "Now restart bht_server please, we'll ping to reconnect. Press any key to continue...";
		(void)getchar();

		Client::key_type k = Client::make_key("hello","world");
		cli.set(k, Client::make_val("xxx"));

		Client::val_type v;
		if(!cli.get(k, &v)) {
			cerr << "Cannot get just set value!\n";
			exit(1);
		}

		if(Client::get_value(v) != "xxx") {
			cerr << "Just set value is wrong!\n";
			exit(1);
		}

		cli.del(k);

		cli.close();
		cli.close();	// repeat close handle will done nothing

//		cli.del(k);		// do operation on a closed handle will throw exception

	} catch(const runtime_error &ex) {
		cerr << "Run-time exception: " << ex.what() << endl;
	} catch(...) {
		cerr << "Unknown exception caught\n";
	}
}

