
#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

DPFS_ROOT_DIR := $(abspath $(CURDIR)/../..)

include $(DPFS_ROOT_DIR)/mk/dpfs.common.mk

LIB := $(DPFS_ROOT_DIR)/lib/libdpfs_parser.so

FLEXSRC := lexyy.cpp
BISONSRC := parser.cpp parser.hpp

OBJS += parser.o lexyy.o

EXTRACLEAN := parser.cpp parser.hpp lexyy.cpp 

all : $(FLEXSRC) $(BISONSRC)

$(FLEXSRC): lexyy.lpp
	flex -o lexyy.cpp lexyy.lpp

$(BISONSRC): parser.yy
	bison -d -o parser.cpp parser.yy

include $(DPFS_ROOT_DIR)/mk/dpfs.lib.mk