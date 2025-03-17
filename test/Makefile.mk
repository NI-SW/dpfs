#                                              test.app(Makefile)
#                                                |
#                             ---------------------------------------
#                             |                                     |
#                           libs(spdk,dpdk.etc...)               dpfsDeps
# 
# 
# 

DPFS_ROOT_DIR := $(abspath $(CURDIR)/..)

# include $(DPFS_ROOT_DIR)/mk/dpfs.spdk_vars.mk
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../thirdparty/spdk)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk

APP = app

# change here to compile your own source files
CXX_SRCS := test.cpp


SPDK_LIB_LIST = $(ALL_MODULES_LIST)


SPDK_LIB_LIST += event event_iscsi event_nvmf

ifeq ($(SPDK_ROOT_DIR)/lib/env_dpdk,$(CONFIG_ENV))
SPDK_LIB_LIST += env_dpdk_rpc
endif

ifeq ($(OS),Linux)
SPDK_LIB_LIST += event_nbd
ifeq ($(CONFIG_UBLK),y)
SPDK_LIB_LIST += event_ublk
endif
ifeq ($(CONFIG_VHOST),y)
SPDK_LIB_LIST += event_vhost_blk event_vhost_scsi
endif
ifeq ($(CONFIG_VFIO_USER),y)
SPDK_LIB_LIST += event_vfu_tgt
endif
endif

ifeq ($(CONFIG_FSDEV),y)
SPDK_LIB_LIST += event_fsdev
endif

include $(SPDK_ROOT_DIR)/mk/spdk.app_cxx.mk
# spdk.app_cxx.mk # 实际完成编译任务的Makefile

install: $(APP)
	$(INSTALL_APP)

uninstall:
	$(UNINSTALL_APP)

