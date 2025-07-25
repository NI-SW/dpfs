#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#
CXXHEADER := -I$(DPFS_ROOT_DIR)/include
CXXLIB := -L$(DPFS_ROOT_DIR)/lib
CXXFLAG := -O2 -Wall -finline-functions
OBJS := $(patsubst %.cpp,%.o,$(wildcard *.cpp))
CXXSOURCE := $(wildcard *.cpp)
