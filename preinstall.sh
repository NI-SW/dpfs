#!/bin/bash

yum install -y gcc-c++ libaio-devel numactl-devel openssl-devel CUnit-devel libuuid-devel python3-pip libibverbs 
yum install -y libnvme-devel librdmacm rdma-core libnuma-devel ncurses-devel nvme-cli nvmetcli
yum install -y rapidjson-devel grpc-devel grpc-plugins grpc-headers grpc-cpp grpc-cpp-plugin grpc-cpp-devel
pip install pyelftools

