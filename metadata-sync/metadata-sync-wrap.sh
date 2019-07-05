#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: $0 <job-id> <tenant-id>"
    exit -1
fi

TENANT_ID=$2
CUR_DIR=$(dirname $(realpath $0))
LOGDIR=$CUR_DIR/logs
WORKDIR=$CUR_DIR/work.$TENANT_ID
TODAY=$(date +%Y-%m-%d)

if [ ! -d $WORKDIR ]; then
	rm -f $WORKDIR
	mkdir $WORKDIR
fi

if [ ! -d $WORKDIR/backup ]; then
	rm -f $WORKDIR/backup
	mkdir $WORKDIR/backup
fi


if [ ! -d $LOGDIR ]; then
	rm -f $LOGDIR
	mkdir $LOGDIR
fi

if [ -e $WORKDIR/lock ]; then
	echo "The last metadata synchronization for tenant $TENANT_ID is still running, please wait."
	exit -1
fi

touch $WORKDIR/lock

$CUR_DIR/metadata-sync.sh $@ 1>> $LOGDIR/metadata-sync.${TENANT_ID}.${TODAY}.log 2>&1

rm -f $WORKDIR/lock

