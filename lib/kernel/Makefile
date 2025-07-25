obj-m := foodfs.o

KERNELBUILD := /lib/modules/$(shell uname -r)/build

default: 
	make -C $(KERNELBUILD) M=$(shell pwd) modules


clean:
	rm -rf *.o .*.cmd .foodfs* *.symvers *.mod.* *.ko *.mod *.order *.out




