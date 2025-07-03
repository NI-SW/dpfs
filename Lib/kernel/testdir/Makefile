# SPDX-License-Identifier: GPL-2.0
ccflags-y				+= -I$(src)

# obj-$(CONFIG_ZONEFS_FS) += zonefs.o

zonefs-y	:= super.o file.o sysfs.o


# obj-m := foodfile.o
obj-m := zonefs.o

KERNELBUILD := /lib/modules/$(shell uname -r)/build

default: 
	make -C $(KERNELBUILD) M=$(shell pwd) modules


clean:
	rm -rf *.o .*.cmd .*.symvers *.mod.* *.ko *.mod *.order *.out


