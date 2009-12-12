all:
	make -C src all

test: all
	make -C test test

clean:
	make -C src clean
	make -C test clean
	make -C pkg clean

.PHONY: all clean test

