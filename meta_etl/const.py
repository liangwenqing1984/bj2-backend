#!/bin/env python
# -*- coding: UTF-8 -*-
import sys

try:
    reload(sys)
    sys.setdefaultencoding("utf-8")
except AttributeError:
    pass
#从hive里获取表元数据
getTSql="""select 
-- t1.db_id,
t3.name as Dbid,
-- t1.TBL_ID,
t1.TBL_NAME as Data_Tbl_Phys_Nm,
ifnull(t2.PARAM_VALUE,'')  as Data_Tbl_Cn_Nm
from tbls t1
left join 
table_params t2
on t1.TBL_ID = t2.TBL_ID and t2.PARAM_KEY='comment'
left join dbs t3
on t1.db_id = t3.db_id
where  t3.name='{}'"""  #参数是库名

#从hive里获取分区元数据
getPSql="""select 
-- t1.PART_ID,
t3.TBL_NAME as Data_Tblid,
-- replace(t1.PART_NAME,'[a-z]',''),
case when length(t1.PART_NAME)=76
then
concat(substr(t1.PART_NAME,23,4),'-',substr(t1.PART_NAME,51,2),'-',substr(t1.PART_NAME,75,2))
else t1.PART_NAME
end AS Dp_Dt,
t1.PART_NAME AS Dp_Path,
t2.PARAM_VALUE AS Rec_Qty
from partitions t1
left join partition_params t2
on t1.PART_ID=t2.PART_ID
left join tbls t3
on t1.tbl_id=t3.tbl_id
left join dbs t4 
on t3.db_id=t4.db_id
where t2.PARAM_KEY='numRows' and t4.NAME='{}'"""  #参数是库名

#从hive里获取字段元数据
getCSql="""select 
t3.tbl_name AS Data_Tblid,
t1.COLUMN_NAME as Fld_Phys_Nm,
ifnull(t1.COMMENT,'') as Fld_Cn_Nm,
t1.TYPE_NAME as Fld_Data_Type,
t1.INTEGER_IDX as Fld_Ord
from columns_v2 t1
left join sds t2
on t1.cd_id = t2.cd_id
left join tbls t3
on t2.sd_id = t3.sd_id
left join dbs t4
on t3.db_id=t4.db_id
where t4.name='{}'"""  #参数是库名

#从清洗库获取表元数据
getCTSql="""select t1.Dbid as Dbid,Data_Tbl_Phys_Nm,Data_Tbl_Cn_Nm from data_tbl t1 
left join db t2 on t1.Dbid=t2.Dbid 
where Del_Dt is null and t2.Db_Phys_Nm='{}'"""  #参数库名
#从清洗库获取分区元数据
getCPSql="""select t1.Data_Tblid as Data_Tblid,Dp_Dt,Dp_Path,Rec_Qty from dp t1 
left join data_tbl t2 
on t1.Data_Tblid=t2.Data_Tblid
left join db t3 on t2.Dbid=t3.Dbid
where t3.Db_Phys_Nm='{}'  """ #参数库名
#从清洗库获取字段元数据
getCCSql="""select t1.Data_Tblid as Data_Tblid,Fld_Phys_Nm,Fld_Cn_Nm,Fld_Data_Type,Fld_Ord from data_fld t1 
left join  data_tbl t2 
on t1.Data_Tblid=t2.Data_Tblid
left join db t3 on t2.Dbid=t3.Dbid
where t1.Del_Dt is null and t3.Db_Phys_Nm='{}'""" #参数库名
#从清洗库获取数据库ID
getDBs = """select distinct Dbid,Db_Phys_Nm from db"""
#从hive元数据库获取某个表的分区信息
getTPSql = """select 
t3.TBL_NAME as Data_Tbl_Phys_Nm,
t1.PART_NAME AS Dp_Path
from partitions t1
left join partition_params t2
on t1.PART_ID=t2.PART_ID
left join tbls t3
on t1.tbl_id=t3.tbl_id
left join dbs t4
on t3.db_id=t4.db_id
where t2.PARAM_KEY='numRows' and t4.NAME='{}' and t3.TBL_NAME='{}'""" #参数 库名 表名  待定
#从清洗库获取某个表的分区信息
getCTPSql = """select distinct t2.Data_Tbl_Phys_Nm as Data_Tbl_Phys_Nm,Dp_Path 
from dp t1
left join data_tbl t2
on t1.Data_Tblid = t2.Data_Tblid
left join db t3
on t2.Dbid=t3.Dbid
where  t3.Db_Phys_Nm='{}' and t2.Data_Tbl_Phys_Nm='{}' and t2.Del_Dt is null"""  #参数 库名 表名  待定
#从hive元数据库获取某个表的字段信息
getTCSql = """select 
t3.tbl_name AS Data_Tbl_Phys_Nm,
t1.COLUMN_NAME as Fld_Phys_Nm,
ifnull(t1.COMMENT,'') as Fld_Cn_Nm,
t1.TYPE_NAME as Fld_Data_Type,
t1.INTEGER_IDX as Fld_Ord
from columns_v2 t1
left join sds t2
on t1.cd_id = t2.cd_id
left join tbls t3
on t2.sd_id = t3.sd_id
left join dbs t4
on t3.db_id=t4.db_id
where t4.name='{}' and t3.tbl_name='{}'"""  #参数 库名 表名 待定
#从清洗库获取某个表的字段信息
getCTCSql = """select 
t1.Data_Tbl_Phys_Nm ,
t2.Fld_Phys_Nm,
t2.Fld_Cn_Nm,
t2.Fld_Data_Type,
t2.Fld_Ord 
from data_tbl t1
left join data_fld t2
on t1.Data_Tblid = t2.Data_Tblid 
left join db t3
on t1.Dbid=t3.Dbid
where t3.Db_Phys_Nm='{}' and t1.Data_Tbl_Phys_Nm='{}' and t2.Del_Dt is null"""  #参数 库名 表名 待定