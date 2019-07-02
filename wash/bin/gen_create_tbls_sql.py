#!/bin/env python
# -*- coding: UTF-8 -*-

from myutils import *
from get_metadata import *
from const import *
import logging


# 生成建表语句工具
def gen_crt_util(dbtb,partition_str,crt_columns_str,aftnm,ifpro):
    rands_column = ""
    if(aftnm == "_rowid"):
        rands_column = "\t\trands string,\n"
    drop_str = DropStr + dbtb +";\n"
    create_str = CreateStr + dbtb + "( \n"
    if(ifpro == '1'):
        drop_str = "";
        create_str = CreateIfStr + dbtb + "( \n"
    crt_hqsql = drop_str +\
                create_str +\
                rands_column +\
                RowidStr +\
                crt_columns_str + ",\n" +\
                TagsStr +\
                partition_str + Formats
    crt_hqsql = crt_hqsql.replace("\t","    ")
    return crt_hqsql


#建开发及生产环境清洗库、问题库、临时库公共脚本
def crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,usecd,aftnm):
    try:
        datadt = chg_date_format(data_dt)
        jobinfo = str(jobid)+ "_" + str(tbid) + "_"+ str(ifpro)+"_"+str(datadt)+aftnm
        logger.info("Genarate  table  sql for job " + jobinfo  )
        dbtbmaps = get_dbtbmaps_by_tbid(logger,conn,tbid)
        crt_columns_str = get_crt_columns_by_tbid(logger,conn,tbid)
        dbtb = dbtbmaps.get(usecd)+aftnm
        dbtb_sql_file = CrtProDir+jobinfo+ ".sql"
        partition_str = PartitionbyStr
        if (usecd == "04"):
            partition_str = PartitionbyStrIsu
            dbtb_sql_file = CrtProDir+jobinfo+ "_isu.sql"
        if(ifpro == '0'):
            dbtb = dbtb+"_"+jobid
            dbtb_sql_file = CrtDevDir+jobinfo+ ".sql"
            partition_str = ""
            if (usecd == "04"):
                partition_str = PartitionbyStrIsuDev
                dbtb_sql_file = CrtDevDir+jobinfo+ "_isu.sql"
        crt_hqsql=gen_crt_util(dbtb,partition_str,crt_columns_str,aftnm,ifpro)
        write_sql_to_file(dbtb_sql_file,crt_hqsql)
        logger.info(crt_hqsql)
    except Exception as err:
        logger.error("创建表 %s 失败 %s" %(jobinfo,err))
        raise err
    return crt_hqsql





#crt_cls_tmp_tb_by_tbid(logger,conn,jobid,tbid,ifpro,datadt,'04','')
#crt_cls_tmp_tb_by_tbid(logger,conn,jobid,tbid,ifpro,datadt,'03','')
#crt_cls_tmp_tb_by_tbid(logger,conn,jobid,tbid,ifpro,datadt,'05','_rowid')
#crt_cls_tmp_tb_by_tbid(logger,conn,jobid,tbid,ifpro,datadt,'05','_deal_dump')
#crt_cls_tmp_tb_by_tbid(logger,conn,jobid,tbid,ifpro,datadt,'05','_deal_column')


