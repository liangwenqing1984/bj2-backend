#!/bin/env python
# -*- coding: UTF-8 -*-
import pymysql
import configparser
from myutils import *
from get_metadata import *
from gen_create_tbls_sql import *
from gen_inst_sql import *
import sys
import os
import logging
from pyhive import hive
import traceback


#帮助信息
def help():
    print("参数错误!\n\t示例:python wash.py 9999 1001 0 0 50 2019-05-03\n"
          "\t参数1:作业id\n"
          "\t参数2:表id\n"
          "\t参数3:批量模式(1:批量清洗 0:指定表清洗)\n"
          "\t参数4:清洗模式(0:开发 1:生产 )\n"
          "\t参数5:抽样比例\n"
          "\t参数6:清洗日期(非必须)\n"
          )

#获得mysql数据库连接
def get_conn(config):
    try:
        conn = pymysql.connect(host=config.get('mysql', 'dbhost'),
                               user=config.get('mysql', 'dbuser'),
                               password=config.get('mysql', 'dbpasswd'),
                               db=config.get('mysql', 'db'),
                               port=int(config.get('mysql', 'dbport')),
                               charset=config.get('mysql', 'charset'),
                               cursorclass=pymysql.cursors.DictCursor)
    except Exception as err:
        logger.error("获得mysql数据库连接失败 %s"  %err)
        raise err
    return conn

#获得mysql问题数据库连接
def get_isu_conn(config):
    try:
        isu_conn = pymysql.connect(host=config.get('mysql_isu', 'dbhost'),
                               user=config.get('mysql_isu', 'dbuser'),
                               password=config.get('mysql_isu', 'dbpasswd'),
                               db=config.get('mysql_isu', 'db'),
                               port=int(config.get('mysql_isu', 'dbport')),
                               charset=config.get('mysql_isu', 'charset'),
                               cursorclass=pymysql.cursors.DictCursor)
    except Exception as err:
        logger.error("获得mysql数据库连接失败 %s"  %err)
        raise err
    return isu_conn

#获得hive数据库连接
def get_hive_conn(config):
    try:
        hive_conn = hive.Connection(host=config.get('hive', 'host'),
                                    port=config.get('hive', 'port'),
                                    username=config.get('hive', 'username'),
                                    database=config.get('hive', 'database')
                                    )
    except Exception as err:
        logger.error("获得hive数据库连接失败 %s"  %err)
        raise err
    return hive_conn


#向日志和标准输出写日志
def get_logger(logfile):
    fh = logging.FileHandler(logfile,encoding='utf-8',mode='w')
    sh     = logging.StreamHandler()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fm = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    logger.addHandler(fh)
    logger.addHandler(sh)
    fh.setFormatter(fm)
    sh.setFormatter(fm)
    return logger

#检查所清洗表分区是否存在或表分区记录数是否为0
def chk_orig_tb(logger,hive_conn,conn,tbid,data_dt):
    records = get_orig_hive_tb_records(logger,hive_conn,conn,tbid,data_dt)
    if(records == None):
        logger.error("表 %s 分区 %s 分区不存在"%(tbid,data_dt))
        raise Exception("[表 %s 分区 %s 分区不存在]"%(tbid,data_dt))
    elif(records == '0'):
        logger.error("表 %s 分区 %s 分区记录数为0"%(tbid,data_dt))
        raise Exception("[表 %s 分区 %s 分区记录数为0]"%(tbid,data_dt))
    else:
        logger.info("表 %s 分区 %s 分区记录数为 %s "%(tbid,data_dt,records))
    return int(records)

#生成创建各个区的表的SQL
def gen_tbs_sql(hqlfile,logger,conn,tbid,ifpro,data_dt):
    try:
        with open(hqlfile,"w") as f:
            f.write(crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,'04',''))
            f.write(crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,'03','') )
            f.write(crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,'05','_rowid'))
            f.write(crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,'05','_deal_dump'))
            f.write(crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,'05','_deal_column'))
    except Exception as err:
        logger.error("生成创建各个区的表的SQL失败 %s" %err)
        raise err
    finally:
        f.close()


#生成创建各个步骤处理SQL
def gen_ins_sql(hqlfile,logger,conf,hive_conn,conn,tbid,ifpro,data_dt,records,pect):
    try:
        mates = get_casewhen_mate(logger,conn, tbid,ifpro)
        mates_len = len(mates)
        with open(hqlfile,"a") as f:
            f.write(get_rowid_tbl_insql(logger,hive_conn,conn, jobid,tbid,ifpro,data_dt,records,pect))
            if mates_len== 0:
                f.write(get_dupl_rec_rule_null_sql(logger,conn,jobid,tbid,ifpro,data_dt))
            else:
                f.write(get_dupl_rec_sql(logger,conn,jobid,tbid,ifpro,data_dt))
                f.write(get_columns_deal_sql_once(logger,config, conn,jobid,tbid,ifpro, data_dt))
                f.write(get_pk_dupl_rec_sql(logger,conn,jobid,tbid,ifpro,data_dt))
    except Exception as err:
        logger.error("生成创建各个步骤处理SQL失败 %s"  %err)
        raise err
    finally:
        f.close()


#同步hive问题库数据到mysql
def exp_hive_to_msql(logger,hive_conn,conn,jobid,tbid,ifpro,data_dt):
    try:
        columns, pk_columns = get_columns_by_tbid(logger,conn, tbid)
        dbtbmaps = get_dbtbmaps_by_tbid(logger,conn, tbid)
        hive_isu_tb = dbtbmaps.get("04")
        msql_isu_tb = "isudb."+hive_isu_tb.split('.')[1]+"_"+jobid
        hive_cursor = hive_conn.cursor()
        hql = "select rowid, " + list_to_str3(columns) + " from " +  hive_isu_tb + "where data_dt='"+data_dt+ "' limit 100"
        if(ifpro == '0'):
            hive_isu_tb = dbtbmaps.get("04")+"_" + jobid
            hql = "select rowid, " + list_to_str3(columns) + " from " +  hive_isu_tb +" limit 100"
        logger.info("hql====\n" + hql)
        hive_cursor.execute(hql)
        hive_result = hive_cursor.fetchall()
        logger.info(hive_result)
        msql_cursor = conn.cursor();
        result_str,kuohao_str = gen_result_str(columns)
        for i in range(len(hive_result)):
            insql = "insert into "+ msql_isu_tb  + " values("
            for j in range(len(columns)+1):
                if j+1 < len(columns)+1:
                    insql +=  "'" + str(hive_result[i][j]) + "',"
                else:
                    insql +=  "'" + str(hive_result[i][j]) + "'"
            insql += ");"
            logger.info("isql===\n"+insql)
            msql_cursor.execute(insql)
        conn.commit()
    except Exception as err:
        logger.error("同步hive问题库数据到mysql失败 %s"  %err)
        raise err
    finally:
        hive_cursor.close()
        msql_cursor.close()
        pass



#正式处理程序
def process(hqlfile,logger,conf,hive_conn,conn,tbid,mode,ifpro,pect,data_dt):
    try:
        records = chk_orig_tb(logger,hive_conn,conn,tbid,data_dt)
        hiveserver = host=conf.get('hive', 'host')
        ifexist = check_cls_isu_tb_exist(logger,conn,jobid,tbid)
        if(ifexist == '1'):
            if(ifpro =='1'):
                with open(hqlfile,'w') as f:
                    f.write("")
                gen_ins_sql(hqlfile,logger,conf,hive_conn,conn,tbid,ifpro,data_dt,records,pect)
            else:
                gen_tbs_sql(hqlfile,logger,conn,tbid,ifpro,data_dt)
                gen_ins_sql(hqlfile,logger,conf,hive_conn,conn,tbid,ifpro,data_dt,records,pect)
        else:
                gen_tbs_sql(hqlfile,logger,conn,tbid,ifpro,data_dt)
                gen_ins_sql(hqlfile,logger,conf,hive_conn,conn,tbid,ifpro,data_dt,records,pect)


        rtcode = os.system("beeline -u jdbc:hive2://"+hiveserver+":10000/ -n hive -f" +hqlfile )
        if(rtcode != 0):
            raise Exception("hive数据处理失败")
        insert_col_static(logger,config,hive_conn,conn,jobid,tbid,ifpro,data_dt)
        tot,dump,pkdump,allpass = get_hive_process_tab_info(logger,conf,hive_conn,conn,jobid,tbid,ifpro,data_dt)
        update_job_status(logger,conn,jobid,'DONE',data_dt,data_dt,ifpro,tot,dump,pkdump,allpass)
    except Exception as err:
        raise err



if __name__ == '__main__':
    if len(sys.argv) != 6 and len(sys.argv) != 7:
        help()
        exit(1)

    jobid = sys.argv[1]
    tbid = sys.argv[2]
    mode = sys.argv[3]
    ifpro = sys.argv[4]
    pect  = sys.argv[5]
    try:
        config = configparser.ConfigParser()
        config.read('../etc/app.properties')
        conn = get_conn(config)
        hive_conn = get_hive_conn(config)
        margs = ("jobid"+str(jobid),"tblid"+str(tbid),"mode"+str(mode),"ifpro"+str(ifpro),"pect"+str(pect))
        jobinfo = "_".join(margs)
        hqlfile = "../sql/" + jobinfo +".sql"
        logfile = "../log/" + jobinfo +".log"
        logger = get_logger(logfile)
        logger.info("Begin to process "+ jobinfo)
        if len(sys.argv) == 7:
            data_dt = sys.argv[6]
        elif len(sys.argv) == 6:
            data_dt = get_max_part_date_by_tbid(conn,tbid)
        if ifpro=='1':
            pect = 100
        process(hqlfile,logger,config,hive_conn,conn,tbid,mode,ifpro,pect,data_dt)
        # gen_ins_sql(hqlfile,logger,config,hive_conn,conn,tbid,ifpro,data_dt,1000,pect)
    except Exception as err:
        update_job_status(logger,conn,jobid,'FAILED',data_dt,data_dt,ifpro,0,0,0,0)
        warning_message = traceback.format_exc()
        logging.error(warning_message)
        logger.error("Process finished failed ,exit!")
        exit(1)
    finally:
        conn.close()
        hive_conn.close()
    logger.info("Process finished success ,exit!")
    exit(0)















    # wash_tbls_map = get_wash_tables(conn)
    # get_columns_by_tbid(conn,1001)
    # get_tnmtid_by_tbid(conn,1001)
    # get_dbtbmaps_by_tbid(conn,1001)
    # get_crt_columns_by_tbid(conn,1001)
    # crt_cls_tb_by_tbid(conn,1001)
    # crt_rowid_tb_by_tbid(conn,1001)
    # crt_isu_tb_by_tbid(conn,1001)
    # crt_tmp_deal_dump_tb_by_tbid(conn,1001)
    # crt_tmpN_tb_by_tbid(conn,1001,'1')
    # crt_tmpN_tb_by_tbid(conn,1001,'deal_column')
    # get_columns_to_deals(conn,1001)
    # crt_tmp1_tb_by_tbid(conn,1001)
    # get_max_part_date_by_tbid(conn,1001)
    # get_column_deal_by_tbid_exct_ord(conn,1001,1)
    # get_fld_ord_and_column_dealed(conn,1001,2)
    # get_columns_deal_sql(conn,1001)
    # get_columns_deal_sql(conn,1001, '2019-05-03')
    # get_casewhen_mate(conn,1001)
    # get_case_when_pkg_by_tbid(config,conn,1001,'1')
    # get_case_when_pkg_by_tbid(config,conn,1001,'2')
    # get_where_or_pkg_by_tbid(config,conn,1001,'or')
    # get_where_or_pkg_by_tbid(config,conn,1001,'and')
    # get_all_columns_cmpus_pkg(conn,1001)
    # deal_column_cmpu_util(config,"mycolumn","0003",None)
    # deal_column_cmpu_util(config,"mycolumn","0003","trim(mycolumn2)")
    # get_all_columns_deals(config,conn,1001)
    #----------------------------------------------------
    # get_rowid_tbl_insql(conn,1001, '2019-05-03')
    # get_dupl_rec_sql(conn,1001, '2019-05-03')
    # get_columns_deal_sql_once(config,conn,1001,'2019-05-03')
    # get_pk_dupl_rec_sql(conn,1001, '2019-05-03')
    #----------------------------------------------------
    # get_dbs_and_usage_by_tnmtid(conn,1001)
    # get_dbtbmaps_by_tbid(conn, 1001)
    #-----------------------------------------------------------------
    # get_tb_phys_nm_by_tbid(logger,conn,tbid)
    # get_tnmtid_by_tbid(logger,conn,tbid)
    # get_dbtbmaps_by_tbid(logger,conn,tbid)
    # get_columns_by_tbid(logger,conn, tbid)
    # get_crt_columns_by_tbid(logger,conn, tbid)
    # get_part_by_date(logger,conn, tbid, datadt)
    # get_whpart_by_date(logger,conn, tbid, datadt)
    # get_max_part_date_by_tbid(conn, tbid)
    # get_casewhen_mate(logger,conn, tbid)
    # get_case_when_pkg_util(logger,conf, fld_phys_nm, data_wash_cmpu_cd, end_flag, wash_flag)
    # get_case_when_pkg_by_tbid(logger,config, conn, tbid, '1',ifpro)
    # get_case_when_pkg_by_tbid(logger,config, conn, tbid, '2',ifpro)
    # get_where_or_pkg_by_tbid(logger,config, conn, tbid, 'or',ifpro)
    # get_where_or_pkg_by_tbid(logger,config, conn, tbid, 'and',ifpro)
    # get_where_or_pkg_by_tbid(logger,config, conn, tbid, 'or',ifpro)
    # get_where_or_pkg_by_tbid(logger,config, conn, tbid, 'and',ifpro)
    # get_all_columns_cmpus_pkg(logger,conn, tbid,ifpro)
    # get_all_columns_deals(logger,config, conn, tbid,ifpro)
    # -----------------------------------------------------------------
    # crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,'04','')
    # crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,'03','')
    # crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,'05','_rowid')
    # crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,'05','_deal_dump')
    # crt_tb_by_tbid(logger,conn,jobid,tbid,ifpro,data_dt,'05','_deal_column')
    # get_rowid_tbl_insql(logger,conn, jobid,tbid,'0',data_dt,pect=100)
    # get_dupl_rec_sql(logger,conn,jobid,tbid,'0',data_dt)
    # get_columns_deal_sql_once(logger,config, conn,jobid,tbid,'1', data_dt)
    # get_pk_dupl_rec_sql(logger,conn,jobid,tbid,'1',data_dt)
    # -----------------------------------------------------------------
        # process(hqlfile,logger,config,conn,tbid,mode,ifpro,pect,data_dt)
        # a,b,c = get_hive_process_tab_info(logger,config,hive_conn,conn,jobid,tbid,ifpro,data_dt)
        # update_job_status(logger,config,hive_conn,conn,jobid,tbid,ifpro,data_dt,'3')
        # create_mysql_isu_tab(logger,conn,jobid,tbid)
        # exp_hive_to_msql(logger,hive_conn,conn,jobid,tbid,ifpro,data_dt)
        # get_hive_process_col_info(logger,conn,tbid,ifpro)
        # get_hive_process_col_hql(logger,config,None,conn,jobid,tbid,'0',data_dt)
        # insert_col_static(logger,config,hive_conn,conn,jobid,tbid,'0',data_dt)
        # get_orig_hive_tb_records(logger,hive_conn,conn,tbid,data_dt)
        # chk_orig_tb(logger,hive_conn,conn,tbid,data_dt)

        # get_tb_phys_nm_by_tbid(logger,conn,tbid)
        # get_tnmtid_by_tbid(logger,conn, tbid)
        # get_dbtbmaps_by_tbid(logger,conn,tbid)
        # get_columns_by_tbid(logger,conn, tbid)
        # get_crt_columns_by_tbid(logger,conn, tbid)
        # get_part_by_date(logger,conn, tbid, data_dt)
        # get_whpart_by_date(logger,conn, tbid, data_dt)
        # get_max_part_date_by_tbid(conn, tbid)
        # get_casewhen_mate(logger,conn, tbid)
        # get_all_columns_cmpus_pkg(logger,conn, tbid,ifpro)
        # create_mysql_isu_tab(logger,conn,jobid,tbid)
