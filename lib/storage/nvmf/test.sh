#!/bin/bash

# ./app -r 'trtype:pcie traddr:0000.1b.00.0'
./app -r 'trtype:tcp adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1'
