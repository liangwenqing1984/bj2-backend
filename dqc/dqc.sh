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
LOG_FILE=`echo ${JOB_ID}|sed s/,/-/g`_${CURR_TIME}.log

echo "parameter:[${JOB_ID}] [${TABLE_ID}] [${MODE}] [${SAMPLE}] [${DATE}]" > $LOG_DIR/${LOG_FILE}

IFS=,
job_array=(${JOB_ID})
table_array=(${TABLE_ID})

source ${VENV_DIR}/bin/activate

for key in "${!job_array[@]}"
do
  echo "parameter:[${job_array[$key]}] [${table_array[$key]}] [${MODE}] [${SAMPLE}] [${DATE}]" >> $LOG_DIR/${LOG_FILE}
  nohup python ${SCRIPT_DIR}/dqc.py ${job_array[$key]} ${table_array[$key]} ${MODE} ${SAMPLE} ${DATE}>> $LOG_DIR/${LOG_FILE} 2>&1 &
done
