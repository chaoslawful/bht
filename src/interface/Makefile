STUB_SRC := $(wildcard gen-cpp/*.cpp)
STUB_OBJ := $(filter-out %.skeleton.o, $(patsubst %.cpp,%.o,$(STUB_SRC)))
ICC_ROOT := /opt/intel/Compiler/11.1/046
CXX := g++
CXXFLAGS := -g3 -O2 -fno-strict-aliasing -Wall -I/usr/local/include -I/usr/local/include/thrift/ -I/usr/include -I/usr/include/thrift -I/home/z/include -I/home/z/include/thrift/ -I$(ICC_ROOT)/tbb/include -I. -I/usr/local/include/boost
LDFLAGS := -pthread -L/usr/local/lib64 -L/usr/lib64 -L/home/z/lib64 -L$(ICC_ROOT)/tbb/lib/em64t/ -L$(ICC_ROOT)/tbb/lib/ia32/ -L/usr/local/lib -L/usr/lib -L/home/z/lib -L.
RTFLAGS := \
	-Wl,-rpath=$(ICC_ROOT)/tbb/lib/em64t/ \
	-Wl,-rpath=$(ICC_ROOT)/tbb/lib/ia32/ \
	-Wl,-rpath=$(shell pwd)/
LIBS := -llua -llog4cxx -lthrift -ltokyotyrant -lmemcached -ltbb

all:
	make gen-cpp
	make bht_server

bht_server: libbht.so BHTServer.o
	$(CXX) $^ -o $@ $(RTFLAGS) $(LDFLAGS) -lbht -lthriftnb -lthrift -levent -ltbb

libbht.so: \
	Compare.o \
	ModInit.o \
	Logging.o \
	Validator.o \
	InternalKey.o \
	BackendPool.o \
	CusHandler.o \
	SeqHandler.o \
	TBBHandler.o \
	TBBThreadFactory.o \
	NulHandler.o \
	Ring.o \
	Config.o \
	Cache.o \
	RelayClient.o \
	Node.o \
	RelayNode.o \
	RelayBackend.o \
	TokyoTyrantBackend.o \
	MemoryBackend.o \
	MemcachedBackend.o \
	Task.o \
	TaskSynchronizer.o \
	Processor.o \
	$(STUB_OBJ)
	$(CXX) -shared $^ -o $@ $(RTFLAGS) $(LDFLAGS) $(LIBS)

%.o: %.cc
	$(CXX) -c -fPIC $(CXXFLAGS) $^ -o $@

%.o: %.cpp
	$(CXX) -c -fPIC $(CXXFLAGS) $^ -o $@

gen-cpp: Interface.thrift ../BHT.thrift ../relay/Local.thrift
	for f in $^; do \
		thrift --gen cpp $$f; \
	done

clean:
	-rm -rf gen-cpp libbht.so *.o bht_server

.PHONY: all clean

