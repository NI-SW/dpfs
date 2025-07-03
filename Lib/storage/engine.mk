DPFS_ROOT_DIR := $(abspath $(CURDIR)/../..)
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../thirdparty/spdk)

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
