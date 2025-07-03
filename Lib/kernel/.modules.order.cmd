cmd_/home/workdir/dpfs/Lib/kernel/modules.order := {   echo /home/workdir/dpfs/Lib/kernel/foodfs.ko; :; } | awk '!x[$$0]++' - > /home/workdir/dpfs/Lib/kernel/modules.order
