#!/bin/bash
if [ $1 == "-h" ];then
	echo "$0 [nodelist] [nproc] [blocksize] [transfersize]"
	echo "$0 --help"
	echo "$0 --check [nodelist] "
elif [ $1 == "-check" ];then
	NODES=`nodeset -e $2`
	echo $NODES
	#now verify that files are indeed written to gekkofs
	for node in $NODES
        do
                PS_RESULT=`clush --conf ./clush.conf -w $node "ps -ef | grep gkfs | grep -v grep"`
		echo ${PS_RESULT}
		pid=`echo ${PS_RESULT} | awk '{print $3}'`
		clush --conf ./clush.conf -w $node "ls -l /dev/shm/data/chunks"
        done
else
	GKFS_HOME=/thfs3/home/wuhuijun/gekkofs-port-bsc-master-20240531
	IOR_BIN=/thfs3/home/wuhuijun/ior/src/ior
	cd /dev/shm/gkfs
	source ${GKFS_HOME}/env.sh
	NODES=`nodeset -e $1 | tr " " "," `
	echo "running IOR on $NODES with $2 procs"
	mpirun -hosts $NODES -np $2 -env LD_PRELOAD=${GKFS_HOME}/install/lib/libgkfs_intercept.so ${IOR_BIN} -b $3 -t $4 -F -o /dev/shm/gkfs/testfile -k
fi

