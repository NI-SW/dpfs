#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

DPFS_ROOT_DIR := $(abspath $(CURDIR)/../../..)

include $(DPFS_ROOT_DIR)/mk/dpfs.common.mk

CXXFLAG += -g
CXXLIB += -ldpfs_net -ldpfs_tcp -ldpfs_log


APP := test_tcp_server

include $(DPFS_ROOT_DIR)/mk/dpfs.app.mk
