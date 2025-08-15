#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

DPFS_ROOT_DIR := $(abspath $(CURDIR)/../../..)
SPDK_ROOT_DIR := $(DPFS_ROOT_DIR)/thirdparty/spdk

include $(DPFS_ROOT_DIR)/mk/dpfs.common.mk

PKG_CONFIG_PATH = $(SPDK_ROOT_DIR)/build/lib/pkgconfig
SPDK_LIB = $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs spdk_nvme spdk_env_dpdk spdk_vmd)

CXXHEADER += -I$(SPDK_ROOT_DIR)/build/include # -I$(SPDK_ROOT_DIR)/include
CXXFLAG += -g
CXXLIB += $(SPDK_LIB)

LIB := ../../libdpfs_nvmf.so


include $(DPFS_ROOT_DIR)/mk/dpfs.lib.mk
