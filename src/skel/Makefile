TARGETS := bhtskel.so bhtskelrelay.so
CC := gcc
CFLAGS := -g3 -O2 -std=c99 -Wall -fPIC -fsigned-char -I/usr/include -I/usr/local/include -I/home/z/include
LDFLAGS := -L/usr/local/lib64 -L/usr/lib64 -L/home/z/lib64 -L/usr/local/lib -L/usr/lib -L/home/z/lib
LIBS := -ltokyocabinet
TESTDB := /tmp/bhtskel.tcb
TESTDBRELAY := /tmp/bhtsklerelay.tcb

all: $(TARGETS)

%.so: %.c
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LDFLAGS) $(LIBS)

run: bhtskel.so
	ttserver -skel $^ $(TESTDB)

runrelay: bhtskelrelay.so
	ttserver -port 1979 -skel $^ $(TESTDBRELAY)

clean: 
	-rm -f $(TARGETS) $(TESTDB) $(TESTDBRELAY)

.PHONY: all clean run

