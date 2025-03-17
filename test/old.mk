# Compiler
CXX = g++

# Compiler flags
CXXFLAGS = -Wall -g

# Source files
SRCS = $(wildcard *.cpp)

# Object files
OBJS = libtest.so 
#$(SRCS:.cpp=.o)

# Executable name


# Include path
CXXFLAGS += -I../include -I../thirdparty/spdk/include

# Include lib path
# CXXFLAGS += -L../lib -L../thirdparty/spdk/build/lib -L../thirdparty/dpdk/build/lib

# Link SPDK libs
# CXXFLAGS += -luuid -lspdk_scsi -lspdk_event_scsi -lspdk_env_dpdk -lspdk_thread -lspdk_trace -lspdk_event \
#  -lspdk_log -lspdk_rpc -lspdk_json -lspdk_jsonrpc \
#   -lspdk_ut -lspdk_fsdev -lspdk_util 


# Default target
all: $(OBJS)

# $(LIB): $(OBJS)
# 	ar $(ARFLAGS) rcs $(LIB) $(OBJS)
# g++ -fPIC -shared -o libpublic.so public.cpp
CXXFLAGS += -fPIC -shared 

# Compile source files into object files
$(OBJS): $(SRCS)
	$(CXX) $(CXXFLAGS) -o $@ -c $< 

# %.o: %.cpp
# 	$(C++) $(CXXFLAGS) $< -c

# Clean up build files
clean:
	rm -f $(OBJS)

.PHONY: all clean