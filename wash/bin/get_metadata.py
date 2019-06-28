#!/bin/env python
# -*- coding: UTF-8 -*-

from myutils import *
import operator
import datetime
import logging

# 根据表id查找表物理名称
def get_tb_phys_nm_by_tbid(logger,conn,tbid):
    try:
        cursor = conn.cursor();
        sql = "select data_tbl_phys_nm from data_tbl where data_tblid = {} ".format(tbid)
        logger.info(sql)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result == None or len(result)==0:
            logger.error("根据表id: %s 未查找到表物理名称" %tbid)
            raise Exception("[根据表id: %s 未查找到表物理名称]" %tbid)
        data_tbl_phys_nm = result.get("data_tbl_phys_nm")
        logger.info("data_tbl_phys_nm====\n"+data_tbl_phys_nm)
    except Exception as err:
        logger.error("根据表id: %s 查找表物理名称失败%s" %(tbid,err))
        raise err
    finally:
        cursor.close()
    return data_tbl_phys_nm

# 根据表id查找租户id
def get_tnmtid_by_tbid(logger,conn, tbid):
    try:
        cursor = conn.cursor();
        sql = "select t3.tnmtid from data_tbl t1 " + \
              "left join db t2 on t1.Dbid = t2.dbid " + \
              "left join data_part t3 on t2.Partid = t3.partid " + \
              "where t1.Data_Tblid= {} ".format(tbid)
        logger.info(sql)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result == None  or len(result)==0:
            logger.error("根据表id: %s 未查找到相应的租户id!"%tbid)
            raise Exception("[根据表id: %s 未查找到相应的租户id!]"%tbid)
        tnmtid = result.get("tnmtid")
        logger.info("tnmtid====\n" + str(tnmtid))
    except Exception as err:
        logger.error("根据表id %s 查找租户id失败:   %s "%(tbid,err))
        raise err
    finally:
        cursor.close()
    return tnmtid


#根据租户id找出该租户下所有的库及用途代码
def get_dbtbmaps_by_tbid(logger,conn,tbid):
    try:
        dbtbmaps = {}
        tnmtid = get_tnmtid_by_tbid(logger,conn, tbid)
        tbname = get_tb_phys_nm_by_tbid(logger,conn,tbid)
        cursor = conn.cursor();
        sql =   "select t2.db_phys_nm,db_usage_cd from data_part t1 "+\
                "left join db t2 on t1.partid = t2.partid  "+\
                "left join db_usage t3 on t2.db_usageid = t3.db_usageid  "+\
                "where t1.tnmtid = {} ".format(tnmtid)
        logger.info(sql)
        count = cursor.execute(sql)
        result = cursor.fetchall()
        logger.info(result)
        if result == None  or len(result)==0:
            logger.error("根据租户id: %s 未找出该租户下所有的库及用途代码!" %tnmtid)
            raise Exception("[根据租户id: %s 未找出该租户下所有的库及用途代码!]" %tnmtid)
        for item in result:
            db_usage_cd = item.get("db_usage_cd")
            db_tb_name = item.get("db_phys_nm")+"."+tbname
            dbtbmaps[db_usage_cd] = db_tb_name
        logger.info("dbtbmaps====\n" + str(dbtbmaps))
    except Exception as err:
        logger.error("根据租户id %s 找出该租户下所有的库及用途代码失败%s" %(tnmtid,err))
        raise err
    finally:
        cursor.close()
    return dbtbmaps


# 根据表id获取表所有字段/主键字段
def get_columns_by_tbid(logger,conn, tbid):
    try:
        cursor = conn.cursor()
        sql = "select t1.Fld_Phys_Nm,t1.Fld_Ord,t1.If_Pk " \
              "from data_fld t1 where t1.data_tblid= {} and DEL_DT is null order by t1.Fld_Ord asc".format(tbid)
        logger.info(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        if result == None or len(result)==0:
            logger.error("根据表id %s 未获取到表所有字段/主键字段" %tbid)
            raise Exception("[根据表id %s 未获取到表所有字段/主键字段]" %tbid)
        columns = []
        pk_columns = []
        for i in range(len(result)):
            Fld_Ord = result[i].get("Fld_Ord")
            Fld_Phys_Nm = result[i].get("Fld_Phys_Nm")
            If_Pk = result[i].get("If_Pk")
            columns.append(Fld_Phys_Nm)
            if (If_Pk == 1):
                pk_columns.append(Fld_Phys_Nm)
        logger.info("columns====\n" + str(columns))
        logger.info("primary key columns====\n" + str(pk_columns))
        cursor.close()
    except Exception as err:
        logger.error("获取元数据字段和主键信息失败%s" %err)
        raise err
    finally:
        cursor.close()
    return columns, pk_columns


# 根据表id获取建表字段、类型、名称
def get_crt_columns_by_tbid(logger,conn, tbid):
    try:
        cursor = conn.cursor()
        sql = "select t1.Fld_Phys_Nm,t1.Fld_Data_Type,t1.Fld_Cn_Nm,t1.Fld_Ord,t1.If_Pk " \
              "from data_fld t1 where t1.data_tblid= {} and t1.DEL_DT is null order by t1.Fld_Ord asc".format(tbid)
        logger.info(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        if result == None or len(result)==0:
            logger.error("根据表id %s 未获取建表字段、类型、名称" %tbid)
            raise Exception("[根据表id %s 未获取建表字段、类型、名称]" %tbid)
        columns_cnt = len(result)
        crt_columns = []
        for i in range(columns_cnt):
            Fld_Ord = result[i].get("Fld_Ord")
            Fld_Phys_Nm = result[i].get("Fld_Phys_Nm")
            Fld_Data_Type = result[i].get("Fld_Data_Type")
            Fld_Cn_Nm = result[i].get("Fld_Cn_Nm")
            crt_column = Fld_Phys_Nm + " " + Fld_Data_Type + " comment '" + Fld_Cn_Nm + "'"
            crt_columns.append(crt_column)
        crt_columns_str = list_to_str(crt_columns)
        logger.info("crt_columns_str====\n" + str(crt_columns_str))
    except Exception as err:
        logger.error("根据表id获取建表字段、类型、名称失败%s" %err)
        raise err
    finally:
        cursor.close()
    return crt_columns_str


# 根据传入日期获取分区信息，组装where分区条件
def get_whpart_by_date(logger,conn, tbid, txdate):
    try:
        cursor = conn.cursor()
        sql = "select dp_path from dp t where t.data_tblid= {} and dp_dt= '{}';".format(tbid,txdate)
        logger.info(sql)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result == None or len(result)==0:
            logger.error("根据表 %s 传入日期 %s 未获取分区信息"%(tbid,txdate))
            raise Exception("[根据表 %s 传入日期 %s 未获取分区信息]"%(tbid,txdate))
        where_part_str = result.get("dp_path").replace("/", "' and ").replace("=","='")+"'"
        logger.info("where_part_str====\n" + where_part_str)
    except Exception as err:
        logger.error("根据传入日期获取分区信息组装where条件失败%s" %err)
        raise err
    finally:
        cursor.close()
    return where_part_str


# 根据表id获取最大分区日期
def get_max_part_date_by_tbid(conn, tbid):
    try:
        cursor = conn.cursor()
        sql = "select max(dp_dt) as maxdt from dp where Data_Tblid= {};".format(tbid)
        print("sql====\n" + sql)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result == None or len(result)==0:
            raise Exception("[根据表 %s 未获取最大分区日期]"%tbid)
        maxdt = str(result.get("maxdt"))
    except Exception as err:
        raise err
    finally:
        cursor.close()
    return maxdt



# 根据表id获取需要进行清洗的字段、清洗运算代码、清洗执行顺序、字段顺序，用于组装case when 和 where or的原材料
def get_casewhen_mate(logger,conn, tbid,ifpro):
    try:
        wash_proj_tb = "data_fld_wash_proj"
        if(ifpro == '1'):
            wash_proj_tb = "prd_data_fld_wash_proj"
        cursor = conn.cursor()
        sql = "select t2.fld_phys_nm,t4.Data_Wash_Cmpu_Cd,t3.exct_ord,t2.fld_ord from data_tbl t1 " \
              "left join data_fld t2 on t1.Data_Tblid = t2.Data_Tblid " \
              "left join {} t3 on t2.Fldid = t3.Fldid " \
              "left join data_wash_cmpu t4 on t3.data_wash_cmpuid =t4.data_wash_cmpuid " \
              "where t1.Data_Tblid = {} and t2.Del_Dt is null and t4.Data_Wash_Cmpu_Cd is not null " \
              "order by t2.Fld_Ord asc ,exct_ord asc".format(wash_proj_tb,tbid)
        logger.info(sql)
        cursor.execute(sql)
        casewhen_mate = cursor.fetchall()
        if casewhen_mate == None or len(casewhen_mate)==0:
            logger.error("根据表 %s 未获取清洗的字段、清洗运算代码、清洗执行顺序、字段顺序"%tbid)
            raise Exception("[根据表 %s 未获取清洗的字段、清洗运算代码、清洗执行顺序、字段顺序]"%tbid)
        logger.info("casewhen_mate====\n" + str(casewhen_mate))
    except Exception as err:
        logger.error("获取清洗字段、清洗运算id、清洗顺序失败%s" %err)
        raise err
    finally:
        cursor.close()
    return casewhen_mate

#case when组装工具
def get_case_when_pkg_util(logger,conf, fld_phys_nm, data_wash_cmpu_cd, end_flag, wash_flag):
    try:
        case_when_str = ""
        if (end_flag == 1):
            case_when_str = "\t\t\tcase when default.canClean" + conf.get("canClean",
                                                                          data_wash_cmpu_cd) + "(" + fld_phys_nm + ") = " + wash_flag + " then '<" + conf.get(
                    "canClean", data_wash_cmpu_cd) + "#" + fld_phys_nm + ">' else '' end)\n"
        else:
            case_when_str = "\t\t\tcase when default.canClean" + conf.get("canClean",
                                                                          data_wash_cmpu_cd) + "(" + fld_phys_nm + ") = " + wash_flag + " then '<" + conf.get(
                    "canClean", data_wash_cmpu_cd) + "#" + fld_phys_nm + ">' else '' end,\n"
        logger.info("case_when_str====\n" + str(case_when_str))
    except Exception as err:
        logger.error("case when组装工具失败%s" %err)
        raise  err
    return case_when_str


# 根据原材料进行case when 的组装
def get_case_when_pkg_by_tbid(logger,conf, conn, tbid, wash_flag,ifpro):
    try:
        mates = get_casewhen_mate(logger,conn, tbid,ifpro)
        mates_len = len(mates)
        case_when_str = "\t\tconcat("
        for i in range(mates_len):
            fld_phys_nm = mates[i].get("fld_phys_nm")
            data_wash_cmpu_cd = mates[i].get("Data_Wash_Cmpu_Cd")
            if (i + 1 == mates_len):
                case_when_str += get_case_when_pkg_util(logger,conf, fld_phys_nm, data_wash_cmpu_cd, 1, wash_flag)
            else:
                case_when_str += get_case_when_pkg_util(logger,conf, fld_phys_nm, data_wash_cmpu_cd, 0, wash_flag)
        case_when_str = case_when_str.replace("\t", "    ")
        logger.info("case_when_str====\n" + str(case_when_str))
    except Exception as err:
        logger.error("根据原材料进行case when 的组装失败%s" %err)
        raise err
    return case_when_str

#组装where or 语句工具
def get_where_or_pkg_util(logger,conf, fld_phys_nm, data_wash_cmpu_cd, start_flag, and_or_flag):
    try:
        where_or_str = ""
        if (start_flag == 1 and and_or_flag == 'or'):
            where_or_str = "\tdefault.canClean" + conf.get("canClean", data_wash_cmpu_cd) + "(" + fld_phys_nm + ") = 2\n"
        elif (start_flag == 1 and and_or_flag == 'and'):
            where_or_str = "\tdefault.canClean" + conf.get("canClean", data_wash_cmpu_cd) + "(" + fld_phys_nm + ") != 2\n"
        elif (start_flag == 0 and and_or_flag == 'or'):
            where_or_str = "\tor default.canClean" + conf.get("canClean", data_wash_cmpu_cd) + "(" + fld_phys_nm + ") = 2\n"
        elif (start_flag == 0 and and_or_flag == 'and'):
            where_or_str = "\tand default.canClean" + conf.get("canClean",
                                                               data_wash_cmpu_cd) + "(" + fld_phys_nm + ") != 2\n"
        logger.info("where_or_str====\n" + where_or_str)
    except:
        logger.error("组装where or 语句工具失败%s" %err)
        raise err
    return where_or_str


# 根据原材料进行where or 的组装
def get_where_or_pkg_by_tbid(logger,conf, conn, tbid, and_or_flag,ifpro):
    try:
        mates = get_casewhen_mate(logger,conn, tbid,ifpro)
        mates_len = len(mates)
        where_or_str = "where \n"
        for i in range(mates_len):
            fld_phys_nm = mates[i].get("fld_phys_nm")
            data_wash_cmpu_cd = mates[i].get("Data_Wash_Cmpu_Cd")
            if (i == 0):
                where_or_str += get_where_or_pkg_util(logger,conf, fld_phys_nm, data_wash_cmpu_cd, 1, and_or_flag)
            else:
                where_or_str += get_where_or_pkg_util(logger,conf, fld_phys_nm, data_wash_cmpu_cd, 0, and_or_flag)
        where_or_str = where_or_str.replace("\t", "    ")
        logger.info("where_or_str====\n" + where_or_str)
    except Exception as err:
        logger.error("根据原材料进行where or 的组装失败%s" %err)
        raise err
    return where_or_str


# 查询所有字段的清洗运算(包括不做任何清洗运算的字段),为所有字段运算组装准备
def get_all_columns_cmpus_pkg(logger,conn, tbid,ifpro):
    try:
        wash_proj_tb = "data_fld_wash_proj"
        if(ifpro == '1'):
            wash_proj_tb = "prd_data_fld_wash_proj"
        cursor = conn.cursor()
        sql = "select t2.fld_phys_nm,t4.Data_Wash_Cmpu_Cd,t3.exct_ord,t2.fld_ord from data_tbl t1 " \
              "left join data_fld t2 on t1.Data_Tblid = t2.Data_Tblid " \
              "left join {} t3 on t2.Fldid = t3.Fldid " \
              "left join data_wash_cmpu t4 on t3.data_wash_cmpuid =t4.data_wash_cmpuid " \
              "where t1.Data_Tblid = {} and t2.Del_Dt is null  " \
              "order by t2.Fld_Ord asc ,exct_ord asc".format(wash_proj_tb,tbid)
        logger.info(sql)
        cursor.execute(sql)
        columns_cmpu = cursor.fetchall()
        if columns_cmpu == None or len(columns_cmpu)==0 or (len(columns_cmpu)==1 and columns_cmpu[0].get("fld_phys_nm") ==None):
            logger.error("根据表 %s 未获取查询所有字段的清洗运算"%tbid)
            raise Exception("[根据表 %s 未获取查询所有字段的清洗运算]"%tbid)
        logger.info("columns_cmpu====\n" + str(columns_cmpu))
    except Exception as err:
        logger.error("查询所有字段的清洗运算(包括不做任何清洗运算的字段)失败%s" %err)
        raise err
    finally:
        cursor.close()
    return columns_cmpu


# 组装所有字段处理逻辑
def get_all_columns_deals(logger,conf, conn, tbid,ifpro):
    try:
        columns, pk_columns = get_columns_by_tbid(logger,conn, tbid)
        all_columns_deal_list = get_all_columns_cmpus_pkg(logger,conn, tbid,ifpro)
        list_len = len(all_columns_deal_list)
        column_deal_dict = {}
        dealed_list = []
        deal_dict = {}
        for i in range(list_len):
            column = all_columns_deal_list[i].get("fld_phys_nm")
            cmpu_cd = all_columns_deal_list[i].get("Data_Wash_Cmpu_Cd")
            exct_ord = all_columns_deal_list[i].get("exct_ord")
            deal_dict["column"] = column
            deal_dict["exct_ord"] = exct_ord
            if (exct_ord == 1 and cmpu_cd != None):
                cmpu_str = conf.get("dealColumn", cmpu_cd)
                cmpu_str = cmpu_str.replace("xxxx", column)
                column_deal_dict[column] = cmpu_str
            elif (exct_ord == None or cmpu_cd == None):
                cmpu_str = column
                column_deal_dict[column] = cmpu_str
            else:
                last_cmpu_str = column_deal_dict.get(column)
                this_cmpu_str = conf.get("dealColumn", cmpu_cd)
                cmpu_str = this_cmpu_str.replace("xxxx", last_cmpu_str)
                column_deal_dict[column] = cmpu_str
        # print(column_deal_dict)
        for i in range(len(columns)):
            col_item = columns[i]
            for key in column_deal_dict:
                if (col_item == key):
                    dealed_list.append(column_deal_dict.get(key))
        dealed_list_str = list_to_str2(dealed_list)
        logger.info("dealed_list_str====\n"+dealed_list_str)
    except Exception as err:
        logger.error("组装所有字段处理逻辑失败%s" %err)
        raise err
        
    return dealed_list_str


#更新作业状态
def update_job_status(logger,conn,jobid,stat,startdt,enddt,tot=0,dump=0,pkdump=0):
    try:
        now_time = get_cur_time()
        cursor = conn.cursor()
        sql =   "update data_proc_job set Job_Stus= '{}' ,Rfrsh_Tm= '{}' ,Data_Start_Dt = '{}',"\
                "Data_Terminate_Dt = '{}',Proc_Rec_Total_Qty={},All_Dupl_Rec_Qty={},Pk_Dupl_Rec_Qty={} "\
                "where jobid = {}".format(stat,now_time,startdt,enddt,tot,dump,pkdump,jobid)
        logger.info("update_job_sql====\n" + sql)
        cursor.execute(sql)
        conn.commit()
    except Exception as err:
        logger.error("更新作业jobid: %s 状态失败 %s"%(jobid,err))
        raise err
    finally:
        cursor.close()



#检查清洗库和问题库相应生产表是否存在
def check_cls_isu_tb_exist(logger,conn,jobid,tbid):
    try:
        tnmtid = get_tnmtid_by_tbid(logger,conn,tbid)
        data_tbl_phys_nm = get_tb_phys_nm_by_tbid(logger,conn,tbid)
        cursor = conn.cursor()
        sql =   "select t2.Db_Phys_Nm,t4.data_tbl_phys_nm from data_part t1 "\
                "left join db t2 on t1.partid = t2.Partid "\
                "left join Db_Usage t3 on t2.Db_Usageid = t3.Db_Usageid  "\
                "left join data_tbl t4 on t2.Dbid = t4.dbid "\
                "where t1.tnmtid = {} and t3.db_usage_cd = '03' "\
                "and t4.data_tbl_phys_nm = '{}'".format(tnmtid,data_tbl_phys_nm)
        logger.info(sql)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result ==  None or len(result) == 0:
            return '0'
        else:
            return '1'
    except Exception as err:
        logger.error("检查清洗库和问题库相应生产表是否存在失败%s" %err)
        raise err
    finally:
        cursor.close()

#统计表处理记录总数、字段重复记录数、主键重复记录数
def get_hive_process_tab_info(logger,conf,hive_conn,conn,jobid,tbid,ifpro,data_dt):
    try:
        dbtbmaps = get_dbtbmaps_by_tbid(logger,conn,tbid)
        wherepart = get_whpart_by_date(logger,conn,tbid,data_dt)
        hcursor = hive_conn.cursor()
        tmpdbtb = dbtbmaps.get("05")+"_rowid"
        isudbtb = dbtbmaps.get("04")
        if(ifpro == '1'):
           hql = "select sum(Proc_Rec_Total_Qty),sum(All_Dupl_Rec_Qty),sum(Pk_Dupl_Rec_Qty) from ( "\
                    "select count(1) as Proc_Rec_Total_Qty ,0 as All_Dupl_Rec_Qty,0 as Pk_Dupl_Rec_Qty from  {}  where data_dt = '{}' union "\
                    "select 0 as Proc_Rec_Total_Qty ,count(1) as All_Dupl_Rec_Qty,0 as Pk_Dupl_Rec_Qty from  {}  where data_dt='{}' and  isu_type='1' union "\
                    "select 0 as Proc_Rec_Total_Qty,0 as All_Dupl_Rec_Qty,count(1) as Pk_Dupl_Rec_Qty  from  {}  where data_dt='{}' and  isu_type='3' "\
                    ") t".format(tmpdbtb,data_dt,isudbtb,data_dt,isudbtb,data_dt)
        else:
            tmpdbtb = tmpdbtb+"_"+jobid
            isudbtb = isudbtb+"_"+jobid
            hql = "select sum(Proc_Rec_Total_Qty),sum(All_Dupl_Rec_Qty),sum(Pk_Dupl_Rec_Qty) from ( "\
                    "select count(1) as Proc_Rec_Total_Qty ,0 as All_Dupl_Rec_Qty,0 as Pk_Dupl_Rec_Qty from  {}  union "\
                    "select 0 as Proc_Rec_Total_Qty ,count(1) as All_Dupl_Rec_Qty,0 as Pk_Dupl_Rec_Qty from  {}  where  isu_type='1' union "\
                    "select 0 as Proc_Rec_Total_Qty,0 as All_Dupl_Rec_Qty,count(1) as Pk_Dupl_Rec_Qty  from  {}  where  isu_type='3' "\
                    ") t".format(tmpdbtb,isudbtb,isudbtb)
        logger.info("hql====\n"+hql)
        hcursor.execute(hql)
        result = hcursor.fetchone()
        if result != None:
            logger.info("result:" + str(result))
            Proc_Rec_Total_Qty = result[0]
            All_Dupl_Rec_Qty = result[1]
            Pk_Dupl_Rec_Qty = result[2]
            return Proc_Rec_Total_Qty,All_Dupl_Rec_Qty,Pk_Dupl_Rec_Qty
        else:
            return 0,0,0
    except Exception as err:
        logger.error("统计表处理记录总数、字段重复记录数、主键重复记录数失败%s" %err)
        raise err
    finally:
        hcursor.close()


#统计字段-清洗算法 处理记录数、回退记录数-预装材料
def get_hive_process_col_info(logger,conn,tbid,ifpro):
    try:
        wash_proj_tb = "data_fld_wash_proj"
        if(ifpro == '1'):
            wash_proj_tb = "prd_data_fld_wash_proj"
        cursor = conn.cursor()
        sql =   "select t2.fldid,t2.fld_phys_nm,t3.data_wash_cmpuid,t4.data_wash_cmpu_cd from data_tbl t1"\
                " left join data_fld t2 on t1.data_tblid =  t2.data_tblid"\
                " inner join {} t3 on t2.fldid = t3.fldid"\
                " left join data_wash_cmpu t4 on t3.data_wash_cmpuid = t4.data_wash_cmpuid"\
                " where t1.data_tblid = {}".format(wash_proj_tb,tbid)
        logger.info(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        logger.info(result)
    except Exception as err:
        logger.error("统计字段-清洗算法 处理记录数、回退记录数-预装材料失败%s" %err)
        raise err
    finally:
        cursor.close()
    return result

#组装字段清洗统计SQL并执行返回结果
def get_hive_process_col_hql(logger,conf,hive_conn,conn,jobid,tbid,ifpro,data_dt):
    try:
        dbtbmaps = get_dbtbmaps_by_tbid(logger,conn, tbid)
        cls_dbtb = dbtbmaps.get("03")
        isu_dbtb = dbtbmaps.get("04")
        if(ifpro == '0'):
            cls_dbtb = cls_dbtb+"_"+jobid
            isu_dbtb = isu_dbtb+"_"+jobid
        mate_info_list = get_hive_process_col_info(logger,conn,tbid,ifpro)
        sesql_in = ""
        for i in (range(len(mate_info_list))):
            fldid = mate_info_list[i].get("fldid")
            fld_phys_nm = mate_info_list[i].get("fld_phys_nm")
            data_wash_cmpuid = mate_info_list[i].get("data_wash_cmpuid")
            data_wash_cmpu_cd = mate_info_list[i].get("data_wash_cmpu_cd")
            tag = "%<" + conf.get('canClean',data_wash_cmpu_cd) + "#"+fld_phys_nm+">%"
            if(ifpro == '1'):
                if(i+1 == len(mate_info_list)):
                    sesql_in += "select '"+str(jobid)+"' as jobid,'"+str(fldid)+"' as fldid,'"+str(data_wash_cmpuid)+"' as data_wash_cmpuid,count(1) as Wash_Rec_Qty,0 as Retn_Rec_Qty from " + cls_dbtb + " where data_dt='" + data_dt+"' and tagslwq like '"+ tag + "' union   select '"+str(jobid)+"' as jobid,'"+str(fldid)+"' as fldid,'"+str(data_wash_cmpuid)+"' as data_wash_cmpuid ,0 as Wash_Rec_Qty,count(1) as Retn_Rec_Qty from " + isu_dbtb + " where data_dt='" +data_dt+ "' and isu_type='2'  and tagslwq like '"+ tag + "'  "
                else:
                    sesql_in += "select '"+str(jobid)+"' as jobid,'"+str(fldid)+"' as fldid,'"+str(data_wash_cmpuid)+"' as data_wash_cmpuid,count(1) as Wash_Rec_Qty,0 as Retn_Rec_Qty from " + cls_dbtb + " where data_dt='" + data_dt+"' and tagslwq like '"+ tag + "' union   select '"+str(jobid)+"' as jobid,'"+str(fldid)+"' as fldid,'"+str(data_wash_cmpuid)+"' as data_wash_cmpuid ,0 as Wash_Rec_Qty,count(1) as Retn_Rec_Qty from " + isu_dbtb + " where data_dt='" +data_dt+ "' and isu_type='2'  and tagslwq like '"+ tag + "' union  "
            else:
                if(i+1 == len(mate_info_list)):
                    sesql_in += "select '"+str(jobid)+"' as jobid,'"+str(fldid)+"' as fldid,'"+str(data_wash_cmpuid)+"'  as data_wash_cmpuid,count(1) as Wash_Rec_Qty,0 as Retn_Rec_Qty from " + cls_dbtb + " where   tagslwq like '"+ tag + "' union   select '"+str(jobid)+"' as jobid,'"+str(fldid)+"' as fldid,'"+str(data_wash_cmpuid)+"' as data_wash_cmpuid,0 as Wash_Rec_Qty,count(1) as  Retn_Rec_Qty from " + isu_dbtb + " where   isu_type='2' and tagslwq like '"+ tag + "'  "
                else:
                    sesql_in += "select '"+str(jobid)+"' as jobid,'"+str(fldid)+"' as fldid,'"+str(data_wash_cmpuid)+"'  as data_wash_cmpuid,count(1) as Wash_Rec_Qty,0 as Retn_Rec_Qty from " + cls_dbtb + " where   tagslwq like '"+ tag + "' union   select '"+str(jobid)+"' as jobid,'"+str(fldid)+"' as fldid,'"+str(data_wash_cmpuid)+"' as data_wash_cmpuid,0 as Wash_Rec_Qty,count(1) as  Retn_Rec_Qty from " + isu_dbtb + " where   isu_type='2' and tagslwq like '"+ tag + "' union  "

        sesql = "select jobid,fldid,data_wash_cmpuid,sum(Wash_Rec_Qty),sum(Retn_Rec_Qty) from ( " + sesql_in +") as t group by jobid,fldid,data_wash_cmpuid"
        logger.info("sesql====\n"+sesql)
        hive_cursor = hive_conn.cursor()
        hive_cursor.execute(sesql)
        result = hive_cursor.fetchall()
        logger.info("hive_colum_static_result=====\n" + str(result))
    except Exception as err:
        logger.error("组装字段清洗统计SQL并执行返回结果失败%s" %err)
        raise err
    finally:
        hive_cursor.close()
    return result


# 更新字段统计数据到mysql
def insert_col_static(logger,conf,hive_conn,conn,jobid,tbid,ifpro,data_dt):
     static_rs = get_hive_process_col_hql(logger,conf,hive_conn,conn,jobid,tbid,ifpro,data_dt)
     sql = ""
     try:
         cursor = conn.cursor()
         for i in range(len(static_rs)):
             jobid = static_rs[i][0]
             fldid = static_rs[i][1]
             data_wash_cmpuid = static_rs[i][2]
             Wash_Rec_Qty = static_rs[i][3]
             Retn_Rec_Qty = static_rs[i][4]
             sql = "replace into FLD_WASH_RESULT(Jobid,Fldid,Data_Wash_Cmpuid,Wash_Rec_Qty,Retn_Rec_Qty) values({},{},{},{},{});".format(jobid,fldid,data_wash_cmpuid,Wash_Rec_Qty,Retn_Rec_Qty)
             logger.info("insert_col_static====\n"+sql)
             cursor.execute(sql)
         conn.commit()
     except Exception as err:
         logger.error("更新字段统计数据到mysql失败%s" %err)
         raise err
     finally:
         cursor.close()



#获取hive中表字段中文、英文名称、类型，并将类型转换为mysql的
def get_crt_columns_by_tbid_for_msql(logger,conn, tbid):
    try:
        cursor = conn.cursor()
        sql = "select t1.Fld_Phys_Nm,t1.Fld_Data_Type,t1.Fld_Cn_Nm,t1.Fld_Ord,t1.If_Pk " \
              "from  data_fld t1 where t1.data_tblid= {} and t1.DEL_DT is null ".format(tbid)
        logger.info(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        columns_cnt = len(result)
        crt_columns = []
        for i in range(columns_cnt):
            Fld_Ord = result[i].get("Fld_Ord")
            Fld_Phys_Nm = result[i].get("Fld_Phys_Nm")
            Fld_Data_Type = str(result[i].get("Fld_Data_Type")).lower().split('\\(')[0]
            Fld_Cn_Nm = result[i].get("Fld_Cn_Nm")
            if(Fld_Data_Type=='string'):
                Fld_Data_Type = 'varchar(255)'
            elif(Fld_Data_Type == 'int' or Fld_Data_Type == 'bigint'):
                Fld_Data_Type = 'bigint'
            elif(Fld_Data_Type == 'double'):
                Fld_Data_Type = 'double'
            elif(Fld_Data_Type == 'decimal'):
                Fld_Data_Type = 'decimal'
            elif(Fld_Data_Type =='date'):
                Fld_Data_Type = 'date'
            elif(Fld_Data_Type =='datetime'):
                Fld_Data_Type = 'datetime'
            else:
                Fld_Data_Type = 'varchar(255)'
            if i + 1 == Fld_Ord:
                if (Fld_Ord == columns_cnt):
                    # crt_column = Fld_Phys_Nm + " " + Fld_Data_Type + " comment '" + Fld_Cn_Nm + "'"
                    crt_column = Fld_Phys_Nm + " " + Fld_Data_Type
                else:
                    crt_column = Fld_Phys_Nm + " " + Fld_Data_Type
                crt_columns.append(crt_column)
        msql_crt_columns_str = list_to_str3(crt_columns)
        logger.info("crt_columns_str====\n" + str(msql_crt_columns_str))
    except Exception as err:
        raise
    finally:
        cursor.close()
    return msql_crt_columns_str

#创建mysql问题表
def create_mysql_isu_tab(logger,conn,jobid,tbid):
    try:
        cursor = conn.cursor()
        tbname = get_tb_phys_nm_by_tbid(logger,conn,tbid)
        dbtbname = "isudb."+tbname + "_" + jobid
        columns, pk_columns = get_columns_by_tbid(logger,conn, tbid)
        msql_crt_columns_str = get_crt_columns_by_tbid_for_msql(logger,conn, tbid)
        crt_sql =   "drop table if exists "+ dbtbname+";" +\
                    "create table " + dbtbname +"(" +\
                    "rowid varchar(50)," + \
                    msql_crt_columns_str + \
                    ")ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        logger.info(crt_sql)
        cursor.execute(crt_sql)
        conn.commit()
    except Exception as err:
        raise
    finally:
        cursor.close()
    return crt_sql

#获取hive原始区表数据记录总数
def get_orig_hive_tb_records(logger,hive_conn,conn,tbid,data_dt):
    try:
        hive_cursor = hive_conn.cursor()
        dbtbmaps = get_dbtbmaps_by_tbid(logger,conn,tbid)
        wherepart = get_whpart_by_date(logger,conn,tbid,data_dt)
        orig_tbnm = dbtbmaps.get("02")
        hql = "select count(1) from " + orig_tbnm + " where " + wherepart
        hive_cursor.execute(hql)
        records = hive_cursor.fetchone()[0]
    except Exception as err:
        logger.error("获取表 %s 分区 %s 记录总数失败"%(tbid,data_dt))
        raise err
    return records

# 获取要清洗的表清单
# def get_wash_tables(conn):
#     cursor = conn.cursor();
#     sql = "select t1.Data_Tblid,t5.Db_Phys_Nm,t4.Data_Tbl_Phys_Nm " \
#           "from data_tbl_wash_proj t1 " \
#           "left join data_tbl t4 on t1.Data_Tblid = t4.Data_Tblid " \
#           "left join db t5 on t4.Dbid = t5.Dbid "
#     cursor.execute(sql)
#     result = cursor.fetchall()
#     db_tbls = {}
#     for i in range(len(result)):
#         tblid = result[i].get("Data_Tblid")
#         db_tbl = result[i].get("Db_Phys_Nm") + "." + result[i].get("Data_Tbl_Phys_Nm")
#         db_tbls[tblid] = db_tbl
#     cursor.close()
#     return db_tbls