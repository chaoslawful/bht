namespace cpp BHT

exception InvalidOperation {
	1: i32 ec,		// Error code
	2: string msg	// Error message
}

struct Key {
	// The primary key to locate record. record is distributed according to hash(domain, key).
	1: string key,
	// The optional subkey to further split record data.
	// BHT guarantees all subkeys under the same key will be positioned on the same physical server.
	// This behavior is useful when using BHT to store related data together for performance consideration.
	2: optional string subkey,
	// The optional timestamp is used to provide modification order, which is important for remote
	// data syncing (only used in Set/Del method).
	3: optional i64 timestamp
}

struct Val {
	1: optional binary value		// Value of the record, is a binary string with maximum length limitation.
}

