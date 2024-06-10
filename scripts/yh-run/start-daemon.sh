#!/bin/bash

GKFS_HOME=/thfs3/home/wuhuijun/gekkofs-port-bsc-master-20240531
BIN=${GKFS_HOME}/install/bin/gkfs_daemon
LISTEN=`ifconfig gn0 | grep inet | head -n 1 | awk '{print $2}'`
source ${GKFS_HOME}/env.sh
DATA=/dev/shm/data
MOUNT=/dev/shm/gkfs
echo "cd ${GKFS_HOME}/scripts/yh-run"
cd ${GKFS_HOME}/scripts/yh-run
rm -rf $DATA $MOUNT > /dev/null 2>&1
mkdir -p $DATA
mkdir -p $MOUNT
for i in {1..1}
do
	LIBGKFS_LOG_OUTPUT=/tmp/gkfs_daemon-new-"$i".log  nohup  $BIN -P "ucx+all" -l $LISTEN -r $DATA -m $MOUNT 1>/tmp/gkfs_daemon-new-"$i".log 2>/tmp/gkfs_daemon-new-"$i" &
done
