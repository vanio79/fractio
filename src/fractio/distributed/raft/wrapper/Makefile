# Makefile for Minimal NuRaft Shim
#
# This builds the minimal C++ wrapper that exposes NuRaft's internal
# process_req and interface implementations to Nim.
#
# All business logic is in Nim - this shim just provides the necessary
# C++ interface implementations.

CXX ?= g++
CXXFLAGS := -std=c++17 -fPIC -O2 -Wall -Wextra
# Relative path from src/fractio/distributed/raft/wrapper to project root
# wrapper -> raft -> distributed -> fractio (inner) -> src -> project_root
NURAFT_BASE := ../../../../..
INCLUDES := -I$(NURAFT_BASE)/thirdparty/NuRaft/src \
            -I$(NURAFT_BASE)/thirdparty/NuRaft/include \
            -I$(NURAFT_BASE)/thirdparty/NuRaft/include/libnuraft \
            -I$(NURAFT_BASE)/thirdparty/NuRaft/examples

# Output
LIB := build/libnuraft_shim.so

# Sources (include in_memory_log_store from examples)
SRCS := nuraft_shim.cxx
EXT_SRCS := $(NURAFT_BASE)/thirdparty/NuRaft/examples/in_memory_log_store.cxx
OBJS := nuraft_shim.o in_memory_log_store.o

# NuRaft library (located at project root, not build subdir)
NURAFT_LIB := $(NURAFT_BASE)/thirdparty/NuRaft/libnuraft.so

.PHONY: all clean

all: $(LIB)

$(LIB): $(OBJS)
	@mkdir -p build
	$(CXX) -shared -o $@ $^ -L$(NURAFT_BASE)/thirdparty/NuRaft -lnuraft -lssl -lcrypto -lpthread -ldl \
		-Wl,-rpath,/home/ingrid/devel/fractio/thirdparty/NuRaft

nuraft_shim.o: nuraft_shim.cxx nuraft_shim.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c -o $@ $<

in_memory_log_store.o: $(EXT_SRCS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c -o $@ $<

clean:
	rm -f $(OBJS) $(LIB)
	rm -rf build

# Debug build
debug: CXXFLAGS += -g -DDEBUG
debug: clean all