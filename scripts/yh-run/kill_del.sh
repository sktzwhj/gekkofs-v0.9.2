#!/bin/bash

HOSTFILE_DIR=$PWD
killall -9 gkfs_daemon > /dev/null 2>&1
killall -9 gkfs_daemon > /dev/null 2>&1
killall -9 gkfs_daemon > /dev/null 2>&1

rm -f ${HOSTFILE_DIR}/gkfs_hosts.txt > /dev/null
