CXX := g++
TARGETS := relay
SOURCES := $(wildcard *.cpp) $(wildcard gen-cpp/*.cpp)
SOURCES := $(filter-out %.skeleton.cpp, $(SOURCES))
OBJECTS := $(strip $(addsuffix .o,$(basename $(SOURCES)))) 
DEPENDS	:= $(strip $(addsuffix .d,$(basename $(SOURCES))))
CXXFLAGS:= -g3 -O2 -Wall -fno-strict-aliasing \
	-I. -I/usr/local/include -I/usr/local/include/thrift/ -I/usr/include -I/usr/include/thrift -I/usr/local/include/boost/ -I/home/z/include -I/home/z/include/thrift
LDFLAGS := -L/usr/local/lib64 -L/usr/lib64 -L/home/z/lib64 -L/usr/local/lib -L/usr/lib -L/home/z/lib \
	-lthrift -lthriftnb -lcrypto -llog4cxx -ltokyotyrant -levent -pthread -lboost_program_options

all:
	make gen-cpp
	make target

target: $(TARGETS)

deps: $(DEPENDS)
objs: $(OBJECTS)

gen-cpp: Local.thrift Remote.thrift ../BHT.thrift ../interface/Interface.thrift
	@rm -rf gen-cpp
	thrift --gen cpp Local.thrift
	thrift --gen cpp Remote.thrift
	thrift --gen cpp ../BHT.thrift
	thrift --gen cpp ../interface/Interface.thrift

relay: $(OBJECTS)
	$(CXX) -o $@ $^ $(LDFLAGS)

%.o : %.cpp
	$(CXX) -o $@ -c $< $(CXXFLAGS)
gen-cpp/%.o : gen-cpp/%.cpp
	$(CXX) -o $@ -c $< $(CXXFLAGS)

%.d : %.cpp
	-@$(CXX) -MM $< $(CXXFLAGS) 2>/dev/null | sed 's/$(notdir $*)\.o/$(subst /,\/,$*).o $(subst /,\/,$*).d/g' > $@
gen-cpp/%.d : gen-cpp/%.cpp
	-@$(CXX) -MM $< $(CXXFLAGS) 2>/dev/null | sed 's/$(notdir $*)\.o/$(subst /,\/,$*).o $(subst /,\/,$*).d/g' > $@

clean: cleandeps cleanobjs cleantargets
	-rm -rf gen-cpp

-include $(DEPENDS)

cleandeps:
	-rm -f $(DEPENDS)
cleanobjs:
	-rm -f $(OBJECTS)
cleantargets:
	-rm -f $(TARGETS)

.PHONY: all clean deps objs cleandeps cleanobjs cleantargets

