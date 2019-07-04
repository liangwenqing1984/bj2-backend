#!/bin/bash
tbid=$1
jobcat=$2
delflag=$3
jobtype=$4
frequency=$5

ETL_HOME=/home/etl/ETLAuto
WASH_DIR=$ETL_HOME/wash/data_wash/bin
VENV_FILE=$ETL_HOME/wash/wash_venv/bin/activate
source $VENV_FILE
cd $WASH_DIR
python publish.py ${tbid} ${jobcat} ${delflag} ${jobtype} ${frequency}
if [ $? -ne 0 ];then
    CURR_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    echo $CURR_TIME "作业发布或删除失败"
    exit 1
else
    CURR_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    echo CURR_TIME "作业发布或删除成功"
    exit 0
fi
