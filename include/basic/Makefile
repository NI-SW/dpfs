DPFS_ROOT_DIR := $(abspath $(CURDIR)/../..)


SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../thirdparty/spdk)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk

APP := test.out

CXXFLAGS += -I$(DPFS_ROOT_DIR)/include

CXX_SRCS = test.cpp

all : $(APP)
    @:

$(APP) : $(CXX_SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(SPDK_LIB_LIST) $(LDFLAGS)
	@echo "Build complete. Run './$(APP)' to execute the program."

clean : 
	$(CLEAN_C) $(APP)
	@echo "Clean complete. Removed './$(APP)'."