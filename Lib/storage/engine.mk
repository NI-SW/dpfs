#  DPFS-License-Identifier: BSD-3-Clause
#  Copyright (C) 2025 Libr Corporation.
#  All rights reserved.
#
DPFS_ROOT_DIR := $(abspath $(CURDIR)/../..)
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../thirdparty/spdk)

# PKG_CONFIG_PATH = $(SPDK_ROOT_DIR)/build/lib/pkgconfig
# SPDK_LIB = $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs spdk_nvme spdk_env_dpdk spdk_vmd)

# CCHEADER := -I$(SPDK_ROOT_DIR)/build/include -I$(DPFS_ROOT_DIR)/include -I$(SPDK_ROOT_DIR)/include
# CCLINK := $(SPDK_LIB) -lpthread
# CCFLAG := -g -O2 -Wall

# test:
# 	$(CC) -o test $(CCFLAG) $(CCHEADER) $(CCLINK)  engine.cpp spdkcontrol.cpp


include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

SO_VER := 6
SO_MINOR := 0
CXXFLAGS += -I$(DPFS_ROOT_DIR)/include
CXX_SRCS = spdkcontrol.cpp
LIBNAME = spdkcontrol

# ifeq ($(OS),Linux)
# LOCAL_SYS_LIBS = -laio
# endif

SPDK_MAP_FILE = $(SPDK_ROOT_DIR)/mk/spdk_blank.map

include $(SPDK_ROOT_DIR)/mk/spdk.lib.mk
