#!/bin/bash

ARCH=`lscpu | grep Architecture | awk '{print $2}'`
GKFS_HOME=$PWD
DEPS_PATH="deps_install"
if [ $ARCH == "aarch64" ];then
	DEPS_PATH="deps-install-arm"
fi
echo "architecutre=${ARCH}"

mkdir -p ${GKFS_HOME}/install
mkdir -p ${GKFS_HOME}/build
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${GKFS_HOME}/${DEPS_PATH}/lib:${GKFS_HOME}/${DEPS_PATH}/lib64
cd build
export PKG_CONFIG_PATH=${GKFS_HOME}/${DEPS_PATH}/lib/pkgconfig
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=${GKFS_HOME}/${DEPS_PATH} -DCMAKE_INSTALL_PREFIX=${GKFS_HOME}/install ..
