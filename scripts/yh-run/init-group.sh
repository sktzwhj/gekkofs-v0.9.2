#!/bin/bash
if [[ $1 == "-h" ]];then
	echo "./group_restart.sh [GROUP_COUNT] [AVALABLE_NODES] [NODE_EACH_GROUP]"
	exit
fi
GROUP_COUNT=$1
AVALABLE_NODES=$2
#NodeEachGroup=$((2000/${GROUP_COUNT}))
NodeEachGroup=$3
GROUP_COUNT=`echo "$1-1" | bc`
rm -rf gkfs_hosts.txt
echo "GROUP_COUNT = ${GROUP_COUNT}"
for gid in `seq 0 ${GROUP_COUNT}`
do
#	echo "./restart.sh cn[`echo "$NodeEachGroup*$gid" | bc`-`echo "$NodeEachGroup*($gid+1)+$NodeEachGroup-1" | bc`]"
	seqStart=`echo "$NodeEachGroup*$gid" | bc`
        seqEnd=`echo "$NodeEachGroup*$gid+$NodeEachGroup-1" | bc`
	nodes=`nodeset -I $seqStart-$seqEnd -f $AVALABLE_NODES`
	echo "starting daemons on $nodes"
	echo "./restart-group.sh $nodes $gid"
	./restart-group.sh $nodes $gid & 
	

done
