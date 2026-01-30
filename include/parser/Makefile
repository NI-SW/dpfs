
#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2025 LBR.
#  All rights reserved.
#

DPFS_ROOT_DIR := $(abspath $(CURDIR)/../..)

include $(DPFS_ROOT_DIR)/mk/dpfs.common.mk

FLEXSRC := $(DPFS_ROOT_DIR)/lib/parser/lexyy.cpp
BISONSRC := $(DPFS_ROOT_DIR)/lib/parser/parser.cpp 

OBJS += parser.o lexyy.o

EXTRACLEAN := $(DPFS_ROOT_DIR)/lib/parser/parser.cpp $(DPFS_ROOT_DIR)/lib/parser/parser.hpp $(DPFS_ROOT_DIR)/lib/parser/lexyy.cpp $(DPFS_ROOT_DIR)/lib/parser/lexer.h stack.hh parser_test

all : $(FLEXSRC) $(BISONSRC)
	
	
$(FLEXSRC): lexyy.l
	flex --header-file=$(DPFS_ROOT_DIR)/lib/parser/lexer.h -o $(DPFS_ROOT_DIR)/lib/parser/lexyy.cpp lexyy.l

$(BISONSRC): parser.y
	bison -d --defines=$(DPFS_ROOT_DIR)/lib/parser/parser.hpp -o $(DPFS_ROOT_DIR)/lib/parser/parser.cpp parser.y

# 

clean : 
	rm -f $(LIB) $(OBJS) $(EXTRACLEAN)


# include $(DPFS_ROOT_DIR)/mk/dpfs.app.mk
# include $(DPFS_ROOT_DIR)/mk/dpfs.lib.mk