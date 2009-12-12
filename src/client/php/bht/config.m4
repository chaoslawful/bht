dnl config.m4 for extension bht

PHP_ARG_WITH(bht, for BHT support,
[  --with-bht              Include BHT support])

if test "$PHP_BHT" != "no"; then
	PHP_REQUIRE_CXX()
	
	CFLAGS="-g3 -O2 -Wall"
	PHP_SUBST(CFLAGS)
	CXXFLAGS="-g3 -O2 -Wall -Wno-write-strings -fno-strict-aliasing"
	PHP_SUBST(CXXFLAGS)

	PHP_ADD_INCLUDE(../../cpp/)

	PHP_SUBST(BHT_SHARED_LIBADD)
	PHP_ADD_LIBPATH(../../cpp/, BHT_SHARED_LIBADD)
	PHP_ADD_LIBRARY(stdc++, 1, BHT_SHARED_LIBADD)
	PHP_ADD_LIBRARY(bhtclient, 1, BHT_SHARED_LIBADD)

	PHP_NEW_EXTENSION(bht, bht.cc, $ext_shared)
fi

