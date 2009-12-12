include "../BHT.thrift"
namespace cpp BHT.Relay

service Local {
	void OnSet(1:string domain, 2:BHT.Key k, 3:BHT.Val v) throws (1: BHT.InvalidOperation e),
	void OnDel(1:string domain, 2:BHT.Key k) throws (1: BHT.InvalidOperation e),
	
	void OnMSet(1:string domain, 2:map<BHT.Key, BHT.Val> kvs) throws (1: BHT.InvalidOperation e),
	void OnMDel(1:string domain, 2:list<BHT.Key> ks) throws (1: BHT.InvalidOperation e)
}

