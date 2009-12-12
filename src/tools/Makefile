STUB_SRC := $(wildcard gen-cpp/*.cpp)
STUB_OBJ := $(filter-out %.skeleton.o, $(patsubst %.cpp,%.o,$(STUB_SRC)))
CXX = g++
CXXFLAGS = -g3 -O2 -Wall -I/usr/local/include -I/usr/local/include/thrift/ -I/usr/include -I/home/z/include -I/home/z/include/thrift/ -I.
LDFLAGS = -pthread -L/usr/local/lib64 -L/usr/lib64 -L/home/z/lib64 -L/usr/local/lib -L/usr/lib -L/home/z/lib

all:
	make gen-cpp
	make bht_tool bht_db_clean

bht_tool: BHTTool.o Compare.o $(STUB_OBJ)
	$(CXX) $^ -o $@ $(LDFLAGS) -lthrift

bht_db_clean: BHTDbClean.o
	$(CXX) $^ -o $@ $(LDFLAGS) -ltokyocabinet -ltokyotyrant

%.o: %.cc
	$(CXX) -c $(CXXFLAGS) $^ -o $@

%.o: %.cpp
	$(CXX) -c $(CXXFLAGS) $^ -o $@

gen-cpp: ../interface/Interface.thrift ../BHT.thrift
	for f in $^; do \
		thrift --gen cpp $$f; \
	done

Compare.o: ../interface/Compare.cc
	$(CXX) -c $(CXXFLAGS) $^ -o $@

clean:
	-rm -rf gen-cpp *.o bht_tool bht_db_clean

.PHONY: all clean

