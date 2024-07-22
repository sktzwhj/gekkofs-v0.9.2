#!/bin/bash
source mpi.env
nodes=`srun hostname |  nodeset -f`
#hostsize=`nodeset -c $nodes`
echo $nodes $hostsize
set -x
#sed 's/,$//'
mpirun -hosts `srun hostname |nodeset -f|nodeset -e| tr " " ","` -env LD_PRELOAD /fs2/home/wuhuijun/lmj/gkfs/lib64/libgkfs_intercept.so -env HOST_SIZE  `nodeset -c $nodes` -ppn 1 $PWD/stage-in /fs2/home/wuhuijun/lmj/software/test/1G /dev/shm/gekkofs/gkfs/1G $PWD/gkfs_hosts.txt.pid /dev/shm/gekkofs/data 
