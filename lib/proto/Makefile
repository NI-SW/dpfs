#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2026 LBR.
#  All rights reserved.
#

DPFS_ROOT_DIR := $(abspath $(CURDIR)/../..)

GRPCSRCS := sysrpc.proto
CPPDIR := .
HEADERDIR := $(DPFS_ROOT_DIR)/include/proto
GRPCOBJS := $(CPPDIR)/sysrpc.grpc.pb.cpp $(CPPDIR)/sysrpc.pb.cpp $(HEADERDIR)/sysrpc.pb.h $(HEADERDIR)/sysrpc.grpc.pb.h

LIB := $(DPFS_ROOT_DIR)/lib/libdpfs_proto.so


include $(DPFS_ROOT_DIR)/mk/dpfs.common.mk


CXXFLAG += -g
CXXHEADER += -I$(DPFS_ROOT_DIR)/include/proto
CXXLIB += -lprotobuf -lgrpc++
EXTRACLEAN += $(GRPCOBJS)

CXXSOURCE := sysrpc.grpc.pb.cpp sysrpc.pb.cpp
OBJS := $(patsubst %.cpp,%.o,$(CXXSOURCE))


include $(DPFS_ROOT_DIR)/mk/dpfs.lib.mk

$(CXXSOURCE) &: $(GRPCSRCS)
	protoc -I . --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` $<
	protoc -I . --cpp_out=. $<
	mv sysrpc.pb.cc $(CPPDIR)/sysrpc.pb.cpp
	mv sysrpc.pb.h  $(HEADERDIR)/sysrpc.pb.h
	mv sysrpc.grpc.pb.cc $(CPPDIR)/sysrpc.grpc.pb.cpp
	mv sysrpc.grpc.pb.h  $(HEADERDIR)/sysrpc.grpc.pb.h


sysrpc.grpc.pb.o : sysrpc.grpc.pb.cpp
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -c $< -o $@

sysrpc.pb.o : sysrpc.pb.cpp
	$(CXX) $(CXXFLAG) $(CXXHEADER) $(CXXLIB) -c $< -o $@
