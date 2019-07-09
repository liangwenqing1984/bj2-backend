#!/bin/sh

CUR_TIME=`date  "+%Y-%m-%d %H:%M:%S"`
echo "${CUR_TIME} create etl_trigger_job"
ETL_HOME="/home/etl/ETLAuto"
RECEIVE_DIR="${ETL_HOME}/DATA/receive/"
CUR_DATE=`date +%Y%m%d`
YESTERDAY=`date -d "yesterday $CUR_DATE" +%Y%m%d`

TRIGGER_FILE_NAME="dir.WSJ_WASH_START_JOB${YESTERDAY}"
TRIGGER_FILE=$RECEIVE_DIR$TRIGGER_FILE_NAME
echo "TRIGGER_FILE=${TRIGGER_FILE}"
touch $TRIGGER_FILE
CUR_TIME=$(date  "+%Y-%m-%d %H:%M:%S")
echo "${CUR_TIME}  create etl_trigger_job finished"