#!/bin/sh

ETL_HOST="192.168.137.130"
ETL_USER="etl"
ETL_PASS="etl123"
ETL_PORT="3306"
ETL_DB="etldb"
if [ $# -eq 0 ];then
    CUR_DATE=`date +%Y%m%d`
else
    CUR_DATE=$1
fi
CUR_TIME=`date  "+%Y-%m-%d %H:%M:%S"`
echo "${CUR_TIME} create etl_trigger_job"
ETL_HOME="/home/etl/ETLAuto"
RECEIVE_DIR="${ETL_HOME}/DATA/receive/"

YESTERDAY=`date -d "yesterday $CUR_DATE" +%Y%m%d`
TEMP_FILE="${ETL_HOME}/tmp/etl_trigger_files.txt"
SELECT_SQL="select concat('dir.',etl_job,replace(CURRENT_DATE()-1,'-','')) as ctlfile from etl_job where etl_job like '%WASH_START_JOB' and enable = 1"
mysql -h${ETL_HOST} -u${ETL_USER} -p${ETL_PASS} -P ${ETL_PORT} -D ${ETL_DB} -e "${SELECT_SQL}" >  ${TEMP_FILE}
cat ${TEMP_FILE} | while read line
do
    if [ $line == "ctlfile" ];then
        continue;
    else
        etl_trigger_file="${RECEIVE_DIR}/${line}"
        echo "etl_trigger_file=====" $etl_trigger_file
        touch ${etl_trigger_file}
    fi
done
#TRIGGER_FILE_NAME="dir.WSJ_WASH_START_JOB${YESTERDAY}"
#TRIGGER_FILE=$RECEIVE_DIR$TRIGGER_FILE_NAME
#echo "TRIGGER_FILE=${TRIGGER_FILE}"
#touch $TRIGGER_FILE
CUR_TIME=$(date  "+%Y-%m-%d %H:%M:%S")
echo "${CUR_TIME}  create etl_trigger_job finished"