#!/bin/sh
# set -x

JOB_ID=$1
TABLE_ID=$2
MODE=$3
SAMPLE=$4
DATE=$5

SCRIPT_DIR="/Users/leiyry/Work/pycode/dqc"
VENV_DIR="${SCRIPT_DIR}/venv"
LOG_DIR="${SCRIPT_DIR}/log"

CURR_TIME=`date +%Y%m%d%H%M%S`
LOG_FILE=${JOB_ID}_${CURR_TIME}.log

source ${VENV_DIR}/bin/activate
nohup python ${SCRIPT_DIR}/dqc.py ${JOB_ID} ${TABLE_ID} ${MODE} ${SAMPLE} ${DATE}> $LOG_DIR/${LOG_FILE} 2>&1 &
