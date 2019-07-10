#!/bin/sh
# set -x

JOB_ID=$1
START=$2
END=$3

SCRIPT_DIR="/Users/leiyry/Work/pycode/dqc"
VENV_DIR="${SCRIPT_DIR}/venv"
LOG_DIR="${SCRIPT_DIR}/logs"

CURR_TIME=`date +%Y%m%d%H%M%S`
LOG_FILE=${JOB_ID}_${CURR_TIME}.log

echo "parameter:[${JOB_ID}] [${START}] [${END}]" > $LOG_DIR/${LOG_FILE}

source ${VENV_DIR}/bin/activate
nohup python ${SCRIPT_DIR}/mask.py ${JOB_ID} ${START} ${END} >> $LOG_DIR/${LOG_FILE} 2>&1 &
