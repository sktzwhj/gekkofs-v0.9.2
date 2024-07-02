#!/bin/bash

if [ $1 ==  "-help" ];then
	echo "$0 [stage name] [number of daemons]"
	exit
fi

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
DATA_PREFIX=/dev/shm/data
MOUNT=/dev/shm/gkfs
DAEMON_EACH_NODE=$2
echo "creating ${DAEMON_EACH_NODE} daemon proc on each node"
echo "cd ${GKFS_HOME}/scripts/yh-run"
cd ${GKFS_HOME}/scripts/yh-run
rm -rf $DATA $MOUNT > /dev/null 2>&1
#mkdir -p $DATA
mkdir -p $MOUNT

#Registry which the gekkofs instance should report to
export GKFS_REGISTRY_FILE=${GKFS_HOME}/tmp/gkfs_registry.txt
#The name for one stage, we may use LIBGKFS_WORKFLOW_STAGE
export LIBGKFS_WORK_FLOW=$1
export LIBGKFS_HOSTS_FILE=${GKFS_HOME}/tmp/gkfs_hosts_${LIBGKFS_WORK_FLOW}.txt
export LIBGKFS_REGISTRY=on
#This file indicate the number of daemon processes for each gekkofs instance
export LIBGKFS_HOSTS_CONFIG_FILE=${GKFS_HOME}/tmp/hcfile_${LIBGKFS_WORK_FLOW}.txt


for i in $(seq 1 ${DAEMON_EACH_NODE});
do
	echo "starting daemon $i at node `hostname`"
	DATA="${DATA_PREFIX}-${LIBGKFS_WORK_FLOW}-$i"
	echo "DATA dir set to $DATA"
	mkdir -p $DATA
	LIBGKFS_LOG_OUTPUT=${GKFS_HOME}/tmp/gkfs_daemon-new-"$i".log  
	nohup  $BIN -P "ucx+all" -l $LISTEN -r $DATA -m $MOUNT -H ${LIBGKFS_HOSTS_FILE} 1>${LIBGKFS_LOG_OUTPUT} 2>${LIBGKFS_LOG_OUTPUT} &
done
rm ${LIBGKFS_HOSTS_CONFIG_FILE}
echo "${DAEMON_EACH_NODE} 1" > ${LIBGKFS_HOSTS_CONFIG_FILE}

daemon_count=`ps -ef | grep gkfs_daemon | grep -v grep | wc -l`
if [ ${daemon_count} -lt ${DAEMON_EACH_NODE} ];then
	echo "[error] daemon creation failed"
fi

