#!/bin/bash

if [[ $1=="-h" ]];then
	echo "$0 [nodelist] [expected num of server on each node]"
fi

nodes=$1
expectSrvCount=$2
echo "expected to see $2 daemon process on each node"
clush -v -w  $nodes "if [[  `ps -ef | grep gkfs_daemon | grep -v grep |  wc -l` -eq $2 && `ps aux | grep gkfs_daemon | grep -v grep | awk '{print \$3}' | sed 's/\..*//' ` -le 10  ]];then echo "daemon check passed";else echo "daemon check failed";fi"
#clush -v -w $nodes "ps -ef | grep ior | grep -v grep | wc -l"
