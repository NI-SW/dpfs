#!/bin/bash
dir=$(readlink -f $(dirname $0))

# avoid use core 0 or core 1 to improve performance
nohup $dir/../thirdparty/spdk/build/bin/nvmf_tgt -m [6,7] -c $dir/config_nvmf.json 2>&1 > ~/nohup.log &

