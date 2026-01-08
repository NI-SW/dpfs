#  DPFS-License-Identifier: BSD-3-Clause
#  Copyright (C) 2025 Libr Corporation.
#  All rights reserved.
#
SUBDIRS := item nvmf dpfssys tcp client page collect bp

all clean::
	@for subdir in $(SUBDIRS); \
	do \
	echo "making $@ in $$subdir"; \
	( cd $$subdir && $(MAKE) $@ ); \
	done
