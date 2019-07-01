#!/bin/bash

WORKDIR=$(dirname $(realpath $0))
TODAY=$(date +%Y-%m-%d)

$WORKDIR/metadata-sync.sh $@ 1> $WORKDIR/metadata-sync.${TODAY}.log 2>&1

