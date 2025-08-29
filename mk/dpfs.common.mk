#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#
CXXHEADER := -I$(DPFS_ROOT_DIR)/include
CXXLIB := -L$(DPFS_ROOT_DIR)/lib
CXXFLAG := -O2 -Wall -Werror=return-type -finline-functions

OSNAME := $(shell uname)

ifeq ($(OSNAME), AIX)
CXXFLAG += -pthread -maix64 -D__AIX6__ -D_GETDELIM -gxcoff -D__STDC_FORMAT_MACROS -D_THREAD_SAFE_ERRNO
else ifeq ($(OSNAME), OS400)
CXXFLAG += -pthread -maix64 -D__AIX6__ -D_GETDELIM -gxcoff -D__STDC_FORMAT_MACROS -D_THREAD_SAFE_ERRNO
endif

# SRCS := $(wildcard *.cpp)
# OBJS := $(patsubst %.cpp, %.o, $(SRCS))

CXXSOURCE := $(wildcard *.cpp)
OBJS := $(patsubst %.cpp,%.o,$(CXXSOURCE))

