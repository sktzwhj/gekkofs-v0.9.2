#!/bin/bash


export GKFS_HOME=$(dirname `dirname \`realpath  ${BASH_SOURCE[-1]}\``)
export GKFS_INSTALL_PATH=${GKFS_HOME}/install
export GKFS_DEPS_INSTALL=${GKFS_HOME}/deps-install
export LD_LIBRARY_PATH=${GKFS_DEPS_INSTALL}/lib:${GKFS_DEPS_INSTALL}/lib64:${LD_LIBRARY_PATH}
export LIBGKFS_REGISTRY_FILE=${GKFS_HOME}/tmp/gkfs_registry.txt

#We need to build a new hostfile for the merged view, so the client will fetch the configuration from the above registry and generate the correspond hcfile 
export LIBGKFS_WORK_FLOW=$1

export LIBGKFS_HOSTS_FILE=${GKFS_HOME}/tmp/gkfs_hosts_${LIBGKFS_WORK_FLOW}.txt
export LIBGKFS_HOSTS_CONFIG_FILE=${GKFS_HOME}/tmp/hcfile_${LIBGKFS_WORK_FLOW}.txt
export LIBGKFS_REGISTRY=on

env | grep GKFS

