#!/bin/bash

WORKDIR=$PWD
nodes=$1
groupSeq=$2
echo "nodes count: `nodeset -c $nodes`"
echo "del"
clush --conf $WORKDIR/clush.conf -b -w $nodes  $WORKDIR/kill_del.sh

echo "del done"
echo "start"
rm -rf $groupSeq
mkdir -p $groupSeq
clush --conf $WORKDIR/clush.conf -b -w $nodes "$WORKDIR/start-daemon-group.sh $groupSeq"

sleep 5

clush --conf $WORKDIR/clush.conf -b -w $nodes "$WORKDIR/prepare-group.sh $groupSeq"


if [ $? != 0 ]; then
    echo "something failed"
        exit 1
else
        echo "done"
fi

clush --conf $WORKDIR/clush.conf -b -w $nodes ps -ef | grep gkfs
