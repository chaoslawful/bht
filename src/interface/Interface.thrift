include "../BHT.thrift"

namespace cpp BHT

service BHT {
	// Single key operations
	void Del(1:string domain, 2:BHT.Key k, 3:bool overwrite=1, 4:bool relay=0) throws (1:BHT.InvalidOperation e),
	BHT.Val Get(1:string domain, 2:BHT.Key k) throws (1:BHT.InvalidOperation e),
	void Set(1:string domain, 2:BHT.Key k, 3:BHT.Val v, 4:bool overwrite=1, 5:bool relay=0) throws (1:BHT.InvalidOperation e),

	// Multiple key operations
	void MDel(1:string domain, 2:list<BHT.Key> ks, 3:bool overwrite=1, 4:bool relay=0) throws (1:BHT.InvalidOperation e),
	map<BHT.Key,BHT.Val> MGet(1:string domain, 2:list<BHT.Key> ks) throws (1:BHT.InvalidOperation e),
	void MSet(1:string domain, 2:map<BHT.Key,BHT.Val> kvs, 4:bool overwrite=1, 5:bool relay=0) throws (1:BHT.InvalidOperation e),

	// Force loading local config
	void UpdateConfig() throws (1:BHT.InvalidOperation e)
}

// vi:ft=thrift ts=4 sw=4

