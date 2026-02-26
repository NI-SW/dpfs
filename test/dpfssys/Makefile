#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

DPFS_ROOT_DIR := $(abspath $(CURDIR)/../..)

include $(DPFS_ROOT_DIR)/mk/dpfs.common.mk

CXXFLAG += -g
CXXLIB += -ldpfs_log -ldpfs_nvmf -ldpfs_engine -ldpfs_proto -ldpfs_grpc -ldpfs_tcp -ldpfs_net -ldpfs_basic -ldpfs_module -ldpfs_dpfssys -ldpfs_collect -ltiparser -ldpfs_parser -lmydecimal


APP := test_system

include $(DPFS_ROOT_DIR)/mk/dpfs.app.mk
