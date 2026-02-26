#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

DPFS_ROOT_DIR := $(abspath $(CURDIR)/../../..)

include $(DPFS_ROOT_DIR)/mk/dpfs.common.mk

CXXFLAG += -g -std=c++17
CXXLIB += -ldpfs_net -ldpfs_tcp -ldpfs_grpc -ldpfs_log -lprotobuf -lgrpc++


APP := test_gclient

include $(DPFS_ROOT_DIR)/mk/dpfs.app.mk
