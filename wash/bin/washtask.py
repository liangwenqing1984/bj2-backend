#!/bin/env python
# -*- coding: UTF-8 -*-
import sys
import os
import logging
import configparser
import pymysql
import traceback

def help():
    print("参数错误!\n\t示例:python washtask.py FGW_FGW_LWQ_TBL_TEST_WASH_20180101.dir\n"
          "\t参数1:控制文件,如：FGW_FGW_LWQ_TBL_TEST_WASH_20180101.dir\n"
          )

#获得mysql清洗数据库连接
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
        logging.error("获得mysql清洗数据库连接失败 %s"  %err)
        raise err
    return conn

def excute_func(conn,ctlfile):
    # ctlfile = "FGW_FGW_LWQ_TEST_TB_WASH_20190503.dir"
    # get_jobinfo(ctlfile)
    # etl_syscd = "FGW"
    # tbname = "LWQ_TEST_TB"
    # print(get_tbid_by_etljob(conn,etl_syscd,tbname))
    # print(get_jobid_by_tbid_jobcat(conn,1001,1))
    try:
        etl_syscd,tbname,wash_or_mask_flag,data_dt = get_jobinfo(ctlfile)
        tbid = get_tbid_by_etljob(conn,etl_syscd,tbname)
        jobid = get_jobid_by_tbid_jobcat(conn,tbid,wash_or_mask_flag)
        rtcode = None
        if(wash_or_mask_flag == '1'):
            cmd = "python wash.py " + str(jobid) + " " + str(tbid) + " 1 1 100 " + data_dt
            logging.info(cmd)
            rtcode = os.system(cmd)
        else:
            cmd = "mask.sh " + str(jobid) + " " + str(tbid) + " 1 1 100 "  + data_dt
            logging.info(cmd)
            rtcode = os.system(cmd)
        if(rtcode != 0):
            logging.error("作业调起失败:ctlfile=%s"%ctlfile)
            raise Exception("作业调起失败:ctlfile=%s"%ctlfile)
    except Exception as err:
        logging.error("作业调起失败:ctlfile=%s"%ctlfile)
        raise err


def get_jobinfo(ctlfile):
    tmp_ls = ctlfile.split(".")[0].split("_")
    etl_syscd =  tmp_ls[0];
    tbname    = "_".join(x.upper() for x in tmp_ls[2:-2])
    wash_or_mask = tmp_ls[-2]
    data_dt    = "-".join((tmp_ls[-1][0:4],tmp_ls[-1][4:6],tmp_ls[-1][6:8]))
    logging.info("Job Info:etl_syscd=[ %s ],tbname=[ %s ],wash_or_mask=[ %s ],data_dt=[ %s ]\n"%(etl_syscd,tbname,wash_or_mask,data_dt))
    if wash_or_mask == 'WASH':
        wash_or_mask_flag = '1'
    elif wash_or_mask == 'MASK':
        wash_or_mask_flag = '2'
    else:
        logging.error("作业非清洗或脱敏作业,wash_or_mask=%s"%wash_or_mask)
        raise Exception("作业非清洗或脱敏作业,wash_or_mask=%s"%wash_or_mask)
    logging.info("etl_syscd=%s,tbname=%s,data_dt=%s"%(etl_syscd,tbname,data_dt))
    return etl_syscd,tbname,wash_or_mask_flag,data_dt

# 根据系统号和作业名称从清洗库中查找表id
def get_tbid_by_etljob(conn,etl_syscd,tbname):
    try:
        cursor = conn.cursor()
        find_tbid_sql = "select t1.Data_Tblid from data_tbl t1 "\
              "inner join db t2 on t1.Dbid = t2.dbid "\
              "inner join data_part t3 on t2.Partid = t3.partid "\
			  "inner join tnmt t4 on t3.Tnmtid = t4.Tnmtid "\
			  "inner join db_usage t5 on t2.Db_Usageid = t5.db_usageid "\
              "where upper(t1.data_tbl_phys_nm)= '{}' "\
					 "and upper(t4.ETL_SysCd) = '{}' "\
                     "and t5.db_usage_cd = '02';".format(tbname,etl_syscd)
        logging.info(find_tbid_sql)
        cursor.execute(find_tbid_sql)
        result = cursor.fetchone()
        if(result == None or len(result)==0):
            logging.error("未从清洗库找到相应要清洗的表id，系统号：%s,表名称:%s" %(etl_syscd,tbname))
        tbid = result.get("Data_Tblid")
        return tbid
    except Exception as err:
        logging.error("未从清洗库找到相应要清洗的表id，系统号：%s,表名称:%s" %(etl_syscd,tbname))
        raise err
    finally:
        cursor.close()

#根据表id和作业类型查找已发布清洗作业id和作业类型
def get_jobid_by_tbid_jobcat(conn,tbid,jobcat):
    try:
        cursor = conn.cursor()
        sql = "select jobid from prd_data_proc_job where data_tblid = '{}' and job_type='{}';".format(tbid,jobcat)
        cursor.execute(sql)
        result = cursor.fetchone()
        if(result == None or len(result)==0):
            logging.error("根据表id:%s 和作业类型未从清洗库找到相应的作业id %s" %(tbid,jobcat))
            raise Exception()
        else:
            jobid = result.get("jobid")
            return jobid
    except Exception as err:
        logging.error("根据表id:%s 和作业类型未从清洗库找到相应的作业id %s" %(tbid,jobcat))
        raise err
    finally:
        cursor.close()


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
    logging.info("begin to run washtask.py ")
    ETL_HOME = "/home/etl/ETLAuto"
    if len(sys.argv) == 2:
        try:
            os.chdir(os.path.join(ETL_HOME,"wash/data_wash/bin"))
            logging.info(os.system("pwd"))
            config = configparser.ConfigParser()
            config.read('../etc/app.properties')
            conn = get_conn(config)
            excute_func(conn,sys.argv[1])
            logging.info("job finished sucessfull ,programe exit!")
            exit(0)
        except Exception as err:
            logging.error("job finished failed ,programe exit!")
            warning_message = traceback.format_exc()
            logging.error(warning_message)
            sys.exit(1)
    else:
        logging.error("parameter error!")
        sys.exit(1)