cmd_/home/workdir/dpfs/Lib/kernel/testdir/modules.order := {   echo /home/workdir/dpfs/Lib/kernel/testdir/zonefs.ko; :; } | awk '!x[$$0]++' - > /home/workdir/dpfs/Lib/kernel/testdir/modules.order
