#!/bin/bash

#GKFS_HOME=/thfs3/home/wuhuijun/gekkofs-port-bsc-master-20240614
echo $(dirname $0)
source $(dirname $0)/../../env.sh
BIN=${GKFS_HOME}/install/bin/gkfs_daemon
LISTEN=`ifconfig gn0 | grep inet | head -n 1 | awk '{print $2}'`
source ${GKFS_HOME}/env.sh
DATA=/dev/shm/data
MOUNT=/dev/shm/gkfs
DAEMON_EACH_NODE=1
echo "creating ${DAEMON_EACH_NODE} daemon proc on each node"
echo "cd ${GKFS_HOME}/scripts/yh-run"
cd ${GKFS_HOME}/scripts/yh-run
rm -rf $DATA $MOUNT > /dev/null 2>&1
mkdir -p $DATA
mkdir -p $MOUNiT

for i in $(seq 1 ${DAEMON_EACH_NODE});
do
	echo "starting daemon $i at node `hostname`"
	LIBGKFS_LOG_OUTPUT=/tmp/gkfs_daemon-new-"$i".log  nohup  $BIN -P "ucx+all" -l $LISTEN -r $DATA -m $MOUNT 
done


daemon_count=`ps -ef | grep gkfs_daemon | grep -v grep | wc -l`
if [ ${daemon_count} -lt ${DAEMON_EACH_NODE} ];then
	echo "[error] daemon creation failed"
fi

