#!/bin/bash
########################################
#          同步元数据                  #
# 第一次同步:                          #
# meta_etl.py dbname yyyy-mm-dd first  #
# 周期同步:                            #
# meta_etl.py dbname yyyy-mm-dd        #
########################################

#数据日期
data_dt=`date  +"%Y-%m-%d" -d  "-1 days"`

echo "开始同步hive historydb 元数据 ..."
if [ $1 == "first" ];then
    echo "第一次导入 ..."
    python ./meta_etl.py $1 ${data_dt}  $2
else
    echo "周期性同步 ..."
    echo $1 ${data_dt}
    python ./meta_etl.py $1 ${data_dt}
fi

if [ $? -eq 0 ];then
    echo "hive 元数据同步完成 ..."
else
    echo "hive 元数据同步失败 ..."
fi
