#!/bin/bash

#GKFS_HOME=/thfs3/home/wuhuijun/gekkofs-port-bsc-master-20240614
YHRUN_DIR=$(dirname `realpath $0`)
echo "YHRUN_DIR="${YHRUN_DIR}
SCRIPT_DIR=$(dirname ${YHRUN_DIR})
echo "SCRIPT_DIR="${SCRIPT_DIR}
GKFS_HOME=$(dirname ${SCRIPT_DIR})
source ${GKFS_HOME}/env.sh
BIN=${GKFS_HOME}/install/bin/gkfs_daemon
echo "BIN is ${BIN}"
LISTEN=`ifconfig gn0 | grep inet | head -n 1 | awk '{print $2}'`
#source ${GKFS_HOME}/env.sh
DATA=/dev/shm/data
MOUNT=/dev/shm/gkfs
DAEMON_EACH_NODE=1
echo "creating ${DAEMON_EACH_NODE} daemon proc on each node"
echo "cd ${GKFS_HOME}/scripts/yh-run"
cd ${GKFS_HOME}/scripts/yh-run
rm -rf $DATA $MOUNT > /dev/null 2>&1
mkdir -p $DATA
mkdir -p $MOUNT

for i in $(seq 1 ${DAEMON_EACH_NODE});
do
	echo "starting daemon $i at node `hostname`"
	LIBGKFS_LOG_OUTPUT=/tmp/gkfs_daemon-new-"$i".log  nohup  $BIN -P "ucx+all" -l $LISTEN -r $DATA -m $MOUNT &
done


daemon_count=`ps -ef | grep gkfs_daemon | grep -v grep | wc -l`
if [ ${daemon_count} -lt ${DAEMON_EACH_NODE} ];then
	echo "[error] daemon creation failed"
fi

