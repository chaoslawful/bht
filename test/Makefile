# Makefile for unit-tests

SRC=$(wildcard *.cc)
OBJS=$(patsubst %.cc,%.o,$(SRC))
TARGET=runtest
CXXFLAGS+=-Wall -g3 -O0 -I. -I../src/interface -I/usr/local/include -I/usr/local/include/thrift/ -I/home/z/include -I/home/z/include/thrift/ `cppunit-config --cflags`
LIBS=-L../src/interface -L/home/z/lib -L/home/z/lib64 -lbht `cppunit-config --libs`
SHFLAGS=-Wl,-rpath,../src/interface/

all: $(TARGET)

test: pre_test unit_test post_test

valgrind: pre_test valgrind_test post_test

unit_test: $(TARGET)
	-@./$(TARGET)

pre_test:
	-@rm -f /tmp/bht.tcb
	-@ttserver -skel `readlink -f ../src/skel/bhtskel.so` -dmn -pid /tmp/ttserver.pid /tmp/bht.tcb
	-@memcached -d -P /tmp/memcached.pid

post_test:
	-@kill `cat /tmp/ttserver.pid`
	-@kill `cat /tmp/memcached.pid`

valgrind_test: $(TARGET)
	valgrind --tool=memcheck --leak-check=full ./$(TARGET)

$(TARGET):$(OBJS) ../src/interface/libbht.so
	$(CXX) $(OBJS) -o $@ $(SHFLAGS) $(LIBS)

%.o: %.cc
	$(CXX) -c $(CXXFLAGS) $^ -o $@

clean:
	-@rm $(TARGET) *.o

