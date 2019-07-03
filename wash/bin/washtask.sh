#!/bin/bash
ctl_file=$1
ETL_HOME=/home/etl/ETLAuto
WASH_DIR=$ETL_HOME/wash/data_wash/bin
VENV_FILE=$ETL_HOME/wash/wash_venv/bin/activate
source $VENV_FILE
cd $WASH_DIR
python washtask.py $ctl_file
if [ $? -ne 0 ];then
    CURR_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    echo $CURR_TIME "作业运行失败"
    exit 1
else
    CURR_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    echo CURR_TIME "作业运行成功"
    exit 0
fi
