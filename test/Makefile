#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

DPFS_ROOT_DIR := $(abspath $(CURDIR)/..)
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../thirdparty/spdk)

include $(DPFS_ROOT_DIR)/mk/dpfs.common.mk

CXXFLAG += -g
CXXLIB += -ldpfs_nvmf -ldpfs_log


APP := app

include $(DPFS_ROOT_DIR)/mk/dpfs.app.mk
