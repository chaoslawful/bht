#ifndef BHT_MODINIT_H__
#define BHT_MODINIT_H__

#include "Common.h"

BHT_CODE_BEGIN

class ModInit {
public:
	ModInit();
	~ModInit();

private:
	static ModInit _inst;
};

BHT_CODE_END

#endif

