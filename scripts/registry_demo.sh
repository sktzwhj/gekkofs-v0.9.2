#!/bin/bash


#We should first start the registry so that gekkofs instances could register.



#GKFS_INSTALL_PATH is founded relative to the current script
export GKFS_HOME=$(dirname `dirname \`realpath  $0\``)
export GKFS_INSTALL_PATH=${GKFS_HOME}/install
export GKFS_DEPS_INSTALL=${GKFS_HOME}/deps-install
#Add necessary paths for gekkofs runtime
export LD_LIBRARY_PATH=${GKFS_DEPS_INSTALL}/lib:${GKFS_DEPS_INSTALL}/lib64:${LD_LIBRARY_PATH}

mkdir -p ${GKFS_HOME}/tmp
export GKFS_REGISTRY_FILE=${GKFS_HOME}/tmp/gkfs_registry.txt


REGISTRY_EXIST=`ps -ef | grep gkfs_registry | grep -v grep | wc -l`
echo "Current registry number is ${REGISTEY_EXIST}"
if [ ${REGISTRY_EXIST} -ge 1 ];then
	echo "REGISTRY already exist"
	cat ${GKFS_REGISTRY_FILE}
	killall gkfs_registry
	rm -f ${GKFS_REGISTRY_FILE}
fi
#gkfs_registry_file stores the information of the started registry, just similar to the gkfs_hosts.txt file for gekkofs instances.

${GKFS_INSTALL_PATH}/bin/gkfs_registry -P ucx+all -l gn0 1>${GKFS_HOME}/tmp/error.log 2>${GKFS_HOME}/tmp/error.log &

cat ${GKFS_HOME}/tmp/error.log 1>/dev/null 2>/dev/null
sleep 3
echo "gekkofs registry started at"`cat ${GKFS_REGISTRY_FILE}`
ps -ef | grep gkfs_registry | grep -v grep

