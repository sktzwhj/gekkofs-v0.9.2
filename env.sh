#!/bin/bash

machine=TH
if [ $machine == "TH" ];then
	export GKFS_HOME=/thfs3/home/wuhuijun/gekkofs-port-bsc-master-20240614
	MPI_PATH=/thfs3/home/wuhuijun/mpich4/
	export PATH=${MPI_PATH}/bin:$PATH
fi	
ARCH=`lscpu | grep Architecture | awk '{print $2}'`
DEPS_PATH="deps_install"
if [ $ARCH == "aarch64" ];then
        DEPS_PATH="deps-arm-install"
fi
DEPSROOT=${GKFS_HOME}
export PKG_CONFIG_PATH=${GKFS_HOME}/${DEPS_PATH}/lib/pkgconfig
export CMAKE_PREFIX_PATH=${GKFS_HOME}/${DEPS_PATH}
# put /usr/bin at the head to enable default gcc
export LD_LIBRARY_PATH=${DEPSROOT}/${DEPS_PATH}/lib:${DEPSROOT}/${DEPS_PATH}/lib64:/lib/aarch64-linux-gnu/:${MPI_PATH}/lib:${LD_LIBRARY_PATH}
echo "setting LD_LIBRARY_PATH to ${LD_LIBRARY_PATH}"
export UCX_TLS=tcp,glex
export UCX_NET_DEVICES=gn0,gni0
