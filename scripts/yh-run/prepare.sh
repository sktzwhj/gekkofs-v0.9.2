#!/bin/bash

source $(dirname $0)/../../env.sh
#GKFS_HOME=/thfs3/home/wuhuijun/gekkofs-port-bsc-master-20240531/
DATA=/dev/shm/data
MOUNT=/dev/shm/gkfs
mkdir -p $DATA
mkdir -p $MOUNT




cp ${GKFS_HOME}/scripts/yh-run/gkfs_hosts.txt /dev/shm/gkfs
