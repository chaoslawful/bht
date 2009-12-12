include "../BHT.thrift"
namespace cpp BHT.Relay

service Remote {
	void Relay(1:string skey, 2:string sval) throws (1:BHT.InvalidOperation e)
}

