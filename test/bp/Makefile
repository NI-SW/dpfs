DPFS_ROOT_DIR := $(abspath $(CURDIR)/../..)

include $(DPFS_ROOT_DIR)/mk/dpfs.common.mk

CXXFLAG += -g
CXXLIB  += -ldpfs_collect -ldpfs_log -ldpfs_engine -ldpfs_nvmf -lmydecimal

APP := test_bp

include $(DPFS_ROOT_DIR)/mk/dpfs.app.mk

