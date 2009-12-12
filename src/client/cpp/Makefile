STUB_SRC := $(wildcard gen-cpp/*.cpp)
STUB_OBJ := $(filter-out %.skeleton.o, $(patsubst %.cpp,%.o,$(STUB_SRC)))
CXX := g++
CXXFLAGS := -g3 -O2 -fno-strict-aliasing -Wall -I/usr/local/include -I/usr/local/include/thrift/ -I/usr/include -I/usr/include/thrift -I/home/z/include -I/home/z/include/thrift/ -I../../interface/ -I.
LDFLAGS := -pthread -L/usr/local/lib64 -L/usr/lib64 -L/home/z/lib64 -L/usr/local/lib -L/usr/lib -L/home/z/lib 
LIBS := -lthrift

all:
	make gen-cpp
	make testclient

testclient: testclient.o libbhtclient.so
	$(CXX) $(CXXFLAGS) testclient.o -o $@ -Wl,-rpath,. -L. -lbhtclient

libbhtclient.so: \
	Client.o \
	ClientImpl.o \
	Compare.o \
	Validator.o \
	BHTSocket.o \
	$(STUB_OBJ)
	$(CXX) -shared $^ -o $@ $(LDFLAGS) $(LIBS)

%.o: %.cc
	$(CXX) -c -fPIC $(CXXFLAGS) $^ -o $@

%.o: %.cpp
	$(CXX) -c -fPIC $(CXXFLAGS) $^ -o $@

gen-cpp: ../../interface/Interface.thrift ../../BHT.thrift
	for f in $^; do \
		thrift --gen cpp $$f; \
	done

clean:
	-rm -rf gen-cpp testclient libbhtclient.so *.o

.PHONY: all clean

