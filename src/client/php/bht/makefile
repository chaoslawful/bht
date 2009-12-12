all: .libs/bht.so

.libs/bht.so: bht.cc configure
	make -f Makefile

configure: config.m4
	phpize
	./configure --with-bht

install: all
	PHP_EXT_DIR=`php -i|grep extension_dir|awk '{print $$3}'`; \
				sudo cp .libs/bht.so $$PHP_EXT_DIR/

test: all
	@php -d error_reporting=E_ALL bht.php; true

clean:
	make -f Makefile clean

dist_clean:
	phpize --clean

.PHONY: all dist_clean

