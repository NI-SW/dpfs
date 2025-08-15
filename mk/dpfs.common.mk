#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#
CXXHEADER := -I$(DPFS_ROOT_DIR)/include
CXXLIB := -L$(DPFS_ROOT_DIR)/lib
CXXFLAG := -O2 -Wall -Werror=return-type -finline-functions

# SRCS := $(wildcard *.cpp)
# OBJS := $(patsubst %.cpp, %.o, $(SRCS))

CXXSOURCE := $(wildcard *.cpp)
OBJS := $(patsubst %.cpp,%.o,$(CXXSOURCE))

