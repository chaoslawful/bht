#include "Common.h"

BHT_CODE_BEGIN

bool Key::operator < (const Key &k) const
{
	if(key < k.key) {
		return true;
	}

	if(key == k.key) {
		if(subkey < k.subkey) {
			return true;
		}

		if(subkey == k.subkey && timestamp < k.timestamp) {
			return true;
		}
	}

	return false;
}

bool Val::operator < (const Val &v) const
{
	if(value < v.value) {
		return true;
	}

	return false;
}

BHT_CODE_END

