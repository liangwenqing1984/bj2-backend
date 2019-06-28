#!/bin/sh
#set -x
JOB_ID=$1
TABLE_ID=$2
MODE=$3
IFPRO=$4
SAMPLE=$5
DATE=$6


SCRIPT_DIR="/home/tomcat/wash/bin"
VENV_DIR="/home/tomcat/dqc/venv"
LOG_DIR="/home/tomcat/wash/log"

CURR_TIME=`date +%Y%m%d%H%M%S`
LOG_FILE=${JOB_ID}_${CURR_TIME}.log
cd $SCRIPT_DIR
source ${VENV_DIR}/bin/activate
nohup python ${SCRIPT_DIR}/wash.py ${JOB_ID} ${TABLE_ID} ${MODE} ${IFPRO} ${SAMPLE} ${DATE}> $LOG_DIR/${LOG_FILE} 2>&1 &
