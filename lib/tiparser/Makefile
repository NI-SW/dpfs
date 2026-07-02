
#  DPFS-License-Identifier: Apache-2.0 license
#  Copyright (C) 2026 WSL.
#  All rights reserved.
#


DPFS_ROOT_DIR := $(abspath $(CURDIR)/../..)

HEADER := $(DPFS_ROOT_DIR)/include/tiparser/tidb_parser.h
LIB := $(DPFS_ROOT_DIR)/lib/libtiparser.so

# BUILD_DIR ?= build

# UNAME_S := $(shell uname -s)
# ifeq ($(UNAME_S),Darwin)
#   LIB_EXT := dylib
# else
#   LIB_EXT := so
# endif

.PHONY: all build clean cpp-example

all: $(LIB)

$(LIB):
	mkdir -p $(CURDIR)/cache
	GOCACHE="$(CURDIR)/cache/.gocache" GOMODCACHE="$(CURDIR)/cache/.gomodcache" \
		go build -buildmode=c-shared -o "$(LIB)" .
	cp tidb_parser.h "$(HEADER)"
	

clean:
	rm -rf $(LIB) $(HEADER)

cpp-example: build
	$(MAKE) -C cpp example
