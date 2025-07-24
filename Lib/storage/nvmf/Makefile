#  DPFS-License-Identifier: BSD-3-Clause
#  Copyright (C) 2025 Libr Corporation.
#  All rights reserved.
#
DPFS_ROOT_DIR := $(abspath $(CURDIR)/../../..)
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../../thirdparty/spdk)

PKG_CONFIG_PATH = $(SPDK_ROOT_DIR)/build/lib/pkgconfig
SPDK_LIB = $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs spdk_nvme spdk_env_dpdk spdk_vmd)

# SYS_LIB := $(SHELL PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs --static spdk_syslibs
CXXHEADER := -I$(SPDK_ROOT_DIR)/build/include -I$(DPFS_ROOT_DIR)/include # -I$(SPDK_ROOT_DIR)/include
CXXFLAG := -g -O2

app: nvmf.cpp
	$(CXX) -o app $(CXXFLAG) $(CXXHEADER) $(SPDK_LIB)  nvmf.cpp

