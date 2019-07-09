#!/bin/sh
#set -x
JOB_IDS=$1
TABLE_IDS=$2
MODE=$3
IFPRO=$4
SAMPLE=$5
DATE=$6


#SCRIPT_DIR="/home/tomcat/wash/bin"
#VENV_DIR="/home/tomcat/dqc/venv"
#LOG_DIR="/home/tomcat/wash/log"

SCRIPT_DIR="/home/etl/ETLAuto/wash/data_wash/bin"
VENV_DIR="/home/etl/ETLAuto/wash/wash_venv"
LOG_DIR="/home/etl/ETLAuto/wash/data_wash/log"

echo "JOB_IDS=["${JOB_IDS}"]"
echo "TABLE_IDS=["${TABLE_IDS}"]"
echo "MODE=["${MODE}"]"
echo "SAMPLE=["${SAMPLE}"]"
echo "DATE=["${DATE}"]"
job_ids_array=(${JOB_IDS//,/ })
table_ids_array=(${TABLE_IDS//,/ })
jobcnt=${#job_ids_array[@]}

cd $SCRIPT_DIR
source ${VENV_DIR}/bin/activate

for(( i=0;i<${jobcnt};i++)) do
    JOB_ID=${job_ids_array[i]}
    TABLE_ID=${table_ids_array[i]}
    echo "JOB_ID=["${JOB_ID}"],TABLE_ID=["${TABLE_ID}"],MODE=["${MODE}"],IFPRO=["${IFPRO}"],SAMPLE=["${SAMPLE}"],DATE=["${DATE}"]"
#    echo "PARAMETERS=["${JOB_ID}"],TABLE_ID=["${TABLE_ID}"],MODE=["${MODE}"],IFPRO=["$IFPRO"],SAMPLE=["$SAMPLE"],DATE=["DATE"]
    CURR_TIME=`date +%Y%m%d%H%M%S`
    LOG_FILE=${JOB_ID}_${CURR_TIME}.log
    nohup python ${SCRIPT_DIR}/wash.py ${JOB_ID} ${TABLE_ID} ${MODE} ${IFPRO} ${SAMPLE} ${DATE}> $LOG_DIR/${LOG_FILE} 2>&1 &
done;