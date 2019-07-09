#!/bin/env python
# -*- coding: UTF-8 -*-

from myutils import *
from get_metadata import *
from const import *

# 组装rowid表数据插入SQL
def get_rowid_tbl_insql(logger,hive_conn,conn,jobid,tbid,ifpro,data_dt,records,pect=100):
    try:
        datadt = chg_date_format(data_dt)
        jobinfo = str(jobid)+ "_" + str(tbid) + "_"+ str(ifpro)+"_"+str(datadt)+"_rowid"
        logger.info(jobinfo)
        sample_records = int(int(records) * int(pect) / 100)
        columns, pk_columns = get_columns_by_tbid(logger,conn,tbid)
        dbtbmaps = get_dbtbmaps_by_tbid(logger,conn,tbid)
        where_part_dt = get_whpart_by_date(logger,conn,jobid,tbid,ifpro,data_dt)
        rowid_dbtb_name = dbtbmaps.get("05") + "_rowid"
        his_dbtb_name = dbtbmaps.get("02")
        ins_sql_file = InsProDir + jobinfo +".sql"
        ins_part_str = " partition(data_dt='" + data_dt + "')\n"
        limit_str = ""
        order_by_str = ""
        if(ifpro == '0'):
            rowid_dbtb_name = rowid_dbtb_name+"_"+jobid
            ins_sql_file = InsDevDir + jobinfo +".sql"
            ins_part_str = "\n"
            limit_str = " limit "+ str(sample_records)
            order_by_str = " order by rands "
        ins_sql = "insert overwrite table " + rowid_dbtb_name + ins_part_str + \
                  "select \n" + \
                  "\t\trand() as rands,\n" +\
                  "\t\tdefault.md5(concat_ws(','," + list_to_concat(columns) + ")),\n" + \
                  list_to_str2(columns) + "\t\t''\n" + \
                  "from " + his_dbtb_name +"\n" + \
                  "where " + where_part_dt + order_by_str +limit_str+";\n\n\n\n\n"
        ins_sql = ins_sql.replace("\t", "    ")
        write_sql_to_file(ins_sql_file, ins_sql)
        logger.info(ins_sql)
    except Exception as err:
        logger.error("组装rowid表拼装SQL语句失败%s" %err)
        raise err
        exit(1)
    return ins_sql



# 组装rowid去重SQL
def get_dupl_rec_sql(logger,conn,jobid,tbid,ifpro,data_dt):
    try:
        datadt = chg_date_format(data_dt)
        jobinfo = str(jobid)+ "_" + str(tbid) + "_"+ str(ifpro)+"_"+str(datadt)+"_deal_dump1"
        logger.info(jobinfo)
        columns, pk_columns = get_columns_by_tbid(logger,conn, tbid)
        hive_sql_str_columns = list_to_str2(columns)
        dbtbmaps = get_dbtbmaps_by_tbid(logger,conn, tbid)
        rowid_dbtb_name = dbtbmaps.get("05") + "_rowid"
        isu_dbtb_name = dbtbmaps.get("04")
        tmp_dbtb_name_deal_dump = dbtbmaps.get("05") + "_deal_dump"
        ins_sql_file = InsProDir + jobinfo + ".sql"
        where_par_str = " where data_dt='" + data_dt + "') as tt\n"
        ins_par_str1 = " partition(data_dt='" + data_dt + "')\n"
        ins_par_str2 = " partition(data_dt='" + data_dt + "',isu_type='1')\n"
        if(ifpro == '0'):
            rowid_dbtb_name = rowid_dbtb_name +"_"+jobid
            isu_dbtb_name = isu_dbtb_name +"_"+jobid
            tmp_dbtb_name_deal_dump = tmp_dbtb_name_deal_dump+"_"+jobid
            ins_sql_file = InsDevDir + jobinfo + ".sql"
            where_par_str=") as tt\n"
            ins_par_str1 = "\n"
            ins_par_str2 = " partition(isu_type='1')\n"
        ins_sql = "from(\n\tselect\n\t\trowidlwq,\n" + hive_sql_str_columns + \
                  "\t\trow_number() over(partition by rowidlwq ) as rank_num\n" + \
                  "\tfrom " + rowid_dbtb_name + " t " + where_par_str + \
                  "insert overwrite table " + tmp_dbtb_name_deal_dump + ins_par_str1 + \
                  "\tselect\n\t\trowidlwq,\n" + list_to_str2(columns) + "\t\t''\n\t where rank_num=1\n" + \
                  "insert overwrite table " + isu_dbtb_name + ins_par_str2 + \
                  "\tselect\n\t\trowidlwq,\n" + list_to_str2(columns)+\
                  "\t\t'rowid'"+\
                  "\n\t where rank_num>1;\n\n\n\n\n\n"
        ins_sql = ins_sql.replace("\t", "    ")
        write_sql_to_file(ins_sql_file, ins_sql)
        logger.info(ins_sql)
    except Exception as err:
        logger.error("组装按rowid所有字段去重插入SQL语句失败%s" %err)
        raise err
        exit(1)
    return ins_sql




# 组装字段处理SQL(一次性组装方式)
def get_columns_deal_sql_once(logger,conf, conn,jobid,tbid,ifpro, data_dt):
    try:
        datadt = chg_date_format(data_dt)
        jobinfo = str(jobid)+ "_" + str(tbid) + "_"+ str(ifpro)+"_"+str(datadt)+"_deal_column"
        logger.info(jobinfo)
        columns, pk_columns = get_columns_by_tbid(logger,conn, tbid)
        re_part_dt = get_whpart_by_date(logger,conn,jobid,tbid,ifpro,data_dt)
        dbtbmaps = get_dbtbmaps_by_tbid(logger,conn, tbid)
        rowid_dbtb_name = dbtbmaps.get("05") + "_rowid"
        isu_dbtb_name = dbtbmaps.get("04")
        cls_dbtb_name = dbtbmaps.get("03")
        tmp_dbtb_name_tmp1 = dbtbmaps.get("05") + "_deal_dump"
        tmp_dbtb_name_tmp2 = dbtbmaps.get("05") + "_deal_column"
        casewhen2 = get_case_when_pkg_by_tbid(logger,conf, conn, tbid, '2',ifpro)
        where1 = get_where_or_pkg_by_tbid(logger,conf, conn, tbid, 'or',ifpro)
        columns_deal_str = get_all_columns_deals(logger,conf,conn,tbid,ifpro)
        casewhen1 = get_case_when_pkg_by_tbid(logger,conf, conn, tbid, '1',ifpro)
        where2 = get_where_or_pkg_by_tbid(logger,conf, conn, tbid, 'and',ifpro)
        ins_sql_file = InsProDir + jobinfo + ".sql"
        where_str = "\twhere data_dt='" + data_dt + "') t\n"
        ins_par_str1 = " partition(data_dt='" + data_dt + "',isu_type='2')\n"
        ins_par_str2 = " partition(data_dt='" + data_dt + "')\n"
        if(ifpro == '0'):
            rowid_dbtb_name = dbtbmaps.get("05") + "_rowid_"+jobid
            isu_dbtb_name = dbtbmaps.get("04")+"_" + jobid
            cls_dbtb_name = dbtbmaps.get("03")+"_" + jobid
            tmp_dbtb_name_tmp1 = dbtbmaps.get("05") + "_deal_dump_"+jobid
            tmp_dbtb_name_tmp2 = dbtbmaps.get("05") + "_deal_column_"+jobid
            ins_sql_file = InsDevDir + jobinfo + ".sql"
            where_str = ") t\n"
            ins_par_str1 = " partition(isu_type='2')\n"
            ins_par_str2 = "\n"
        ins_sql = "from (\n" \
                  "\tselect * from " + tmp_dbtb_name_tmp1 + "\n" + \
                  where_str + \
                  "insert into " + isu_dbtb_name + ins_par_str1 + \
                  "\tselect\n" + \
                  "\t\trowidlwq,\n" + \
                  list_to_str2(columns)  + \
                  casewhen2 + AsTagsStr + \
                  where1 + \
                  "insert  overwrite table "+tmp_dbtb_name_tmp2+ins_par_str2 + \
                  "\tselect\n" + \
                  "\t\trowidlwq,\n" + \
                  columns_deal_str+\
                  casewhen1 + AsTagsStr + \
                  where2+";\n\n\n\n\n\n"
        ins_sql = ins_sql.replace("\t", "    ")
        write_sql_to_file(ins_sql_file, ins_sql)
        logger.info(ins_sql)
    except Exception as err:
        logger.error("组装按rowid所有字段去重插入SQL语句失败%s" %err)
        raise err
        exit(1)
    return ins_sql

# 组装主键去重SQL
def get_all_dupl_rec_sql(logger,conn,jobid,tbid,ifpro,data_dt):
    try:
        datadt = chg_date_format(data_dt)
        jobinfo = str(jobid)+ "_" + str(tbid) + "_"+ str(ifpro)+"_"+str(datadt)+"_deal_dump2"
        logger.info(jobinfo)
        columns, pk_columns = get_columns_by_tbid(logger,conn, tbid)
        hive_sql_str_columns = list_to_str2(columns)
        dbtbmaps = get_dbtbmaps_by_tbid(logger,conn, tbid)
        rowid_dbtb_name = dbtbmaps.get("05") + "_rowid"
        cls_dbtb_name = dbtbmaps.get("03")
        isu_dbtb_name = dbtbmaps.get("04")
        tmp_dbtb_name = dbtbmaps.get("05") + "_deal_column"
        ins_sql_file = InsProDir + jobinfo + ".sql"
        where_part_str =" where data_dt='" + data_dt + "') as tt\n"
        ins_part_str1 = " partition(data_dt='" + data_dt + "')\n"
        ins_part_str2 = " partition(data_dt='" + data_dt + "',isu_type='3')\n"
        if len(pk_columns) != 0:
            partition_columns_str = list_to_str3(pk_columns)
        else:
            partition_columns_str = list_to_str3(columns)
        if(ifpro == "0"):
            rowid_dbtb_name = dbtbmaps.get("05") + "_rowid_"+jobid
            # cls_dbtb_name = dbtbmaps.get("03")+"_"+jobid
            cls_dbtb_name = dbtbmaps.get("05")+"_cls_"+jobid
            isu_dbtb_name = dbtbmaps.get("04")+"_"+jobid
            tmp_dbtb_name = dbtbmaps.get("05") + "_deal_column_"+jobid
            ins_sql_file = InsDevDir + jobinfo + ".sql"
            where_part_str =") as tt\n"
            ins_part_str1 = "\n"
            ins_part_str2 = " partition(isu_type='3')\n"
        ins_sql = "from(\n\tselect\n\t\trowidlwq,\n" + hive_sql_str_columns +"\t\ttagslwq,\n" \
                  "\t\trow_number() over(partition by " + partition_columns_str + ") as rank_num\n" + \
                  "\tfrom " + tmp_dbtb_name + " t "+ where_part_str + \
                  "insert overwrite table " + cls_dbtb_name + ins_part_str1 + \
                  "\tselect\n\t\trowidlwq,\n" + list_to_str2(columns)+"\t\ttagslwq" + "\n\t where rank_num=1\n" + \
                  "insert overwrite table " + isu_dbtb_name + ins_part_str2 + \
                  "\tselect\n\t\trowidlwq,\n" + list_to_str2(columns)+"\t\ttagslwq" + "\n\t where rank_num>1;\n\n\n\n\n\n"
        ins_sql = ins_sql.replace("\t", "    ")
        write_sql_to_file(ins_sql_file, ins_sql)
        logger.info(ins_sql)
    except Exception as err:
        logger.error("组装组装主键去重插入SQL语句失败%s" %err)
        raise err
        exit(1)
    return ins_sql


# 组装rowid去重SQL-清洗规则为空的情况
def get_dupl_rec_rule_null_sql(logger,conn,jobid,tbid,ifpro,data_dt):
    try:
        datadt = chg_date_format(data_dt)
        jobinfo = str(jobid)+ "_" + str(tbid) + "_"+ str(ifpro)+"_"+str(datadt)+"_deal_dump1"
        logger.info(jobinfo)
        columns, pk_columns = get_columns_by_tbid(logger,conn, tbid)
        hive_sql_str_columns = list_to_str2(columns)
        dbtbmaps = get_dbtbmaps_by_tbid(logger,conn, tbid)
        rowid_dbtb_name = dbtbmaps.get("05") + "_rowid"
        isu_dbtb_name = dbtbmaps.get("04")
        # tmp_dbtb_name_deal_dump = dbtbmaps.get("05") + "_deal_dump"
        tmp_dbtb_name_deal_dump = dbtbmaps.get("03")
        ins_sql_file = InsProDir + jobinfo + ".sql"
        where_par_str = " where data_dt='" + data_dt + "') as tt\n"
        ins_par_str1 = " partition(data_dt='" + data_dt + "')\n"
        ins_par_str2 = " partition(data_dt='" + data_dt + "',isu_type='1')\n"
        if(ifpro == '0'):
            rowid_dbtb_name = rowid_dbtb_name +"_"+jobid
            isu_dbtb_name = isu_dbtb_name +"_"+jobid
            tmp_dbtb_name_deal_dump = dbtbmaps.get("05")+"_cls_"+jobid
            ins_sql_file = InsDevDir + jobinfo + ".sql"
            where_par_str=") as tt\n"
            ins_par_str1 = "\n"
            ins_par_str2 = " partition(isu_type='1')\n"
        ins_sql = "from(\n\tselect\n\t\trowidlwq,\n" + hive_sql_str_columns + \
                  "\t\trow_number() over(partition by rowidlwq ) as rank_num\n" + \
                  "\tfrom " + rowid_dbtb_name + " t " + where_par_str + \
                  "insert overwrite table " + tmp_dbtb_name_deal_dump + ins_par_str1 + \
                  "\tselect\n\t\trowidlwq,\n" + list_to_str2(columns) + "\t\t''\n\t where rank_num=1\n" + \
                  "insert overwrite table " + isu_dbtb_name + ins_par_str2 + \
                  "\tselect\n\t\trowidlwq,\n" + list_to_str2(columns)+\
                  "\t\t'rowid'"+\
                  "\n\t where rank_num>1;\n\n\n\n\n\n"
        ins_sql = ins_sql.replace("\t", "    ")
        write_sql_to_file(ins_sql_file, ins_sql)
        logger.info(ins_sql)
    except Exception as err:
        logger.error("组装按rowid所有字段去重插入SQL语句失败%s" %err)
        raise err
        exit(1)
    return ins_sql

# 组装字段处理SQL(按轮次方式)
# def get_columns_deal_sql(conn, tbid, data_dt):
#     ins_sqls = []
#     max_exct_ord = get_max_part_date_by_tbid(conn, tbid)
#     re_part_dt = get_whpart_by_date(conn, tbid, data_dt)
#     dbtbmaps = get_dbtbmaps_by_tbid(conn, tbid)
#     for i in range(max_exct_ord):
#         step = i + 1
#         column_dealed = get_fld_ord_and_column_dealed(conn, tbid, step)
#         column_dealed_str = list_to_str(column_dealed)
#         print("-----------\n", column_dealed_str)
#         tmp_dbtb_name = dbtbmaps.get(5) + "_tmp_col_" + str(step)
#         if i == 0:
#             up_tmp_dbtb_name = dbtbmaps.get(5) + "_tmp_deal_dump"
#         else:
#             up_tmp_dbtb_name = dbtbmaps.get(5) + "_tmp_col_" + str(step - 1)
#         ins_sql = "insert overwrite table " + tmp_dbtb_name + " partition(data_dt='" + data_dt + "')\n" + \
#                   "select\n" + \
#                   "\t\trowid,\n" + \
#                   column_dealed_str + "\n" + \
#                   "from " + up_tmp_dbtb_name + "\n" + \
#                   "where data_dt='" + data_dt + "';"
#         ins_sql = ins_sql.replace("\t", "    ")
#         ins_sqls.append(ins_sql)
#     print(ins_sqls[0], "\n" + ins_sqls[1])
#     return ins_sqls
