#  DPFS-License-Identifier: BSD-3-Clause
#  Copyright (C) 2025 Libr Corporation.
#  All rights reserved.
#
SUBDIRS := dpfsmodule basic log storage collect dpfsnet dpfssys client 

all clean::
	@for subdir in $(SUBDIRS); \
	do \
	echo "making $@ in $$subdir"; \
	( cd $$subdir && $(MAKE) $@ ); \
	done
