#  DPFS-License-Identifier: BSD-3-Clause
#  Copyright (C) 2025 Libr Corporation.
#  All rights reserved.
#
SUBDIRS := item nvmf diskman collect dpfssys tcp client page bp tiparser mysql_decimal grpc

all clean::
	@for subdir in $(SUBDIRS); \
	do \
	echo "making $@ in $$subdir"; \
	( cd $$subdir && $(MAKE) $@ ); \
	done
