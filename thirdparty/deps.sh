#!/bin/bash

osname=$(cat /etc/os-release | grep ID= | awk -F '=' '{if(NR == 1) print $2}')

if [ $osname = '"centos"' ] 
then
    yum install -y meson ninja-build numactl-devel
elif [ $osname = '"ubuntu"' ] 
then
    apt install -y meson ninja-build numactl-devel
fi

pip install pyelftools

