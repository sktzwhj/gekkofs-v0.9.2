#!/bin/bash

WORKDIR=$PWD
nodes=$1
echo "nodes count: `nodeset -c $nodes`"
echo "del"
clush --conf $WORKDIR/clush.conf -b -w $nodes  $WORKDIR/kill_del.sh

rm -f gkfs_hosts.txt

echo "del done"
echo "start"

clush --conf $WORKDIR/clush.conf -b -w $nodes $WORKDIR/start-daemon.sh

sleep 5

clush --conf $WORKDIR/clush.conf -b -w $nodes $WORKDIR/prepare.sh


if [ $? != 0 ]; then
    echo "something failed"
        exit 1
else
        echo "done"
fi

clush --conf $WORKDIR/clush.conf -b -w $nodes ps -ef | grep gkfs
