cmd_/home/workdir/dpfs/Lib/kernel/foodfs.mod := printf '%s\n'   foodfs.o | awk '!x[$$0]++ { print("/home/workdir/dpfs/Lib/kernel/"$$0) }' > /home/workdir/dpfs/Lib/kernel/foodfs.mod
