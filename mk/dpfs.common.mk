#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#
CXXHEADER := -I$(DPFS_ROOT_DIR)/include
CXXLIB := -L$(DPFS_ROOT_DIR)/lib
CXXFLAG := -O2 -Wall -Wextra -Werror=return-type -finline-functions -pthread
EXTRCLEAN := 
OSNAME := $(shell uname)
GCCVERSION := $(shell g++ -dumpversion)

ifeq ($(GCCVERSION), 4.8.5)
	CXXFLAG += -std=c++11
endif

ifeq ($(OSNAME), AIX)
CXXFLAG += -Wl,-berok -maix64 -D__AIX6__ -D_GETDELIM -gxcoff -D__STDC_FORMAT_MACROS -D_THREAD_SAFE_ERRNO -fconserve-space -Xlinker -brtl -static-libgcc \
-Bstatic -lstdc++
else ifeq ($(OSNAME), OS400)
CXXFLAG += -Wl,-berok -maix64 -D__AIX6__ -D_GETDELIM -gxcoff -D__STDC_FORMAT_MACROS -D_THREAD_SAFE_ERRNO -fconserve-space -Xlinker -brtl -static-libgcc \
-Bstatic -lstdc++
endif

# SRCS := $(wildcard *.cpp)
# OBJS := $(patsubst %.cpp, %.o, $(SRCS))

CXXSOURCE := $(wildcard *.cpp)
OBJS := $(patsubst %.cpp,%.o,$(CXXSOURCE))

