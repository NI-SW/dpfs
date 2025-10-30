#!/bin/bash

kill -9 $(ps -ef | grep nvmf_tgt | grep config_nvmf | awk '{print $2}') 
