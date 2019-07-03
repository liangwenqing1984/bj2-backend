#!/bin/bash

WORKDIR=$(dirname $(realpath $0))
LOGDIR=$WORKDIR/logs
TODAY=$(date +%Y-%m-%d)

if [ ! -d $LOGDIR ]; then
	rm -f $LOGDIR
	mkdir $LOGDIR
fi

$WORKDIR/metadata-sync.sh $@ 1> $LOGDIR/metadata-sync.${TODAY}.log 2>&1

