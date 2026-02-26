#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

DPFS_ROOT_DIR := $(abspath $(CURDIR)/../../..)

include $(DPFS_ROOT_DIR)/mk/dpfs.common.mk

CXXFLAG += -g

LIB := $(DPFS_ROOT_DIR)/lib/libdpfs_grpc.so

CXXLIB += -ldpfs_log

include $(DPFS_ROOT_DIR)/mk/dpfs.lib.mk
