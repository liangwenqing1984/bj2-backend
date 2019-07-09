#!/bin/sh

CURR_TIME=$(date "+%Y-%m-%d %H:%M:%S")
echo $CURR_TIME "该租户下所有清洗作业完成！,更新impala metadata..."
beeline -u "jdbc:hive2://192.166.162.147:21050/share;auth=noSasl" -n cleanse -p cleanse -e "INVALIDATE METADATA"
if [ $? -ne 0 ];then
    CURR_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    echo ${CURR_TIME} ":更新impala metadata 失败，后续该租户下脱敏作业将无法调起"
    exit 1
else
    CURR_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    echo  ${CURR_TIME} ":更新impala metadata 成功，后续该租户下脱敏作业将被调起"
    exit 0
fi
