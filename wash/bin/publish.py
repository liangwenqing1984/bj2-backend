#!/bin/env python
# -*- coding: UTF-8 -*-
import sys
from publish_meta import *
import configparser
import logging
from get_metadata import *
from publish_meta import *
import traceback

def pub_help():
    print("参数错误!\n\t示例:python publish.py 1001 1 1 D 0\n"
          "\t参数1:表id\n"
          "\t参数2:作业类型(1:清洗作业，2：脱敏作业)\n"
          "\t参数3:作业发布删除标识(1:发布 0:删除)\n"
          "\t参数4:作业周期类型\n"
          "\t参数5:作业运行日期(0、1、2、3...31、-1)\n"
          )


#获得清洗mysql数据库连接
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
        logger.error("获得清洗数据库连接失败 %s"  %err)
        raise err
    return conn

#获得调度mysql数据库连接
def get_etldb_conn(conf):
    try:
        conn = pymysql.connect(host=config.get('etldb', 'dbhost'),
                               user=config.get('etldb', 'dbuser'),
                               password=config.get('etldb', 'dbpasswd'),
                               db=config.get('etldb', 'db'),
                               port=int(config.get('etldb', 'dbport')),
                               charset=config.get('etldb', 'charset'),
                               cursorclass=pymysql.cursors.DictCursor)
    except Exception as err:
        logger.error("获得调度数据库连接失败 %s"  %err)
        raise err
    return conn

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

def process(logger,conf,etl_conn,jobcat,delflag,etl_server,scriptid,scriptfile,etl_system,etl_job,description,job_type,frequency,wash_start_system,wash_start_job,wash_end_system,wash_end_job):
    try:
        if delflag == '1':
            create_etl_job(logger,conf,etl_conn,jobcat,etl_server,scriptid,scriptfile,etl_system,etl_job,description,frequency,jobtype,wash_start_system,wash_start_job,wash_end_system,wash_end_job)
        elif delflag == '0':
            del_job(logger,etl_conn,jobcat,etl_system,etl_job,wash_end_system,wash_end_job)
    except Exception as err:
        logger("处理作业 %s 失败"%etl_job)
        raise err

if __name__ == '__main__':
    if len(sys.argv) != 6 or (sys.argv[2] !='1' and sys.argv[2]!= '2') or (sys.argv[3] !='1' and sys.argv[3]!= '0'):
        pub_help()
        exit(1)

    tbid = sys.argv[1]
    jobcat = sys.argv[2]
    delflag = sys.argv[3]
    jobtype = sys.argv[4]
    frequency = sys.argv[5]

    config = configparser.ConfigParser()
    config.read('../etc/app.properties')
    etl_server = config.get("etlhome","etl_server")
    scriptid = config.get("etlhome","wash_schedule_sript_id")
    wash_conn = get_conn(config)
    etl_conn = get_etldb_conn(config)
    logiflenm = "_".join((tbid,jobcat,delflag)) + ".log"
    publish_log_file = "../log/" + logiflenm
    logger = get_logger(publish_log_file)
    try:
        wash_start_system=config.get("etlhome","wash_start_system")
        wash_start_job=config.get("etlhome","wash_start_job")
        wash_end_system=config.get("etlhome","wash_end_system")
        wash_end_job=config.get("etlhome","wash_end_job")
        etl_system = str(get_etl_system_by_tbid(logger,wash_conn,tbid)).upper()
        tb_phys_nm = str(get_tb_phys_nm_by_tbid(logger,wash_conn,tbid)).upper()
        etl_job = etl_system+"_"+tb_phys_nm+"_WASH"
        scriptfile = str(etl_job).lower()+"0100.sh"
        description = "清洗作业-"+get_tb_cn_nm_by_tbid(logger,wash_conn,tbid)
        if jobcat == '2':
            etl_job = etl_system+"_"+tb_phys_nm+"_MASK"
            scriptfile = str(etl_job).lower()+"0100.sh"
            description = "脱敏作业-"+get_tb_cn_nm_by_tbid(logger,wash_conn,tbid)
            scriptid = config.get("etlhome","mask_schedule_sript_id")
        process(logger,config,etl_conn,jobcat,delflag,etl_server,scriptid,scriptfile,etl_system,etl_job,description,jobtype,frequency,wash_start_system,wash_start_job,wash_end_system,wash_end_job)
    except Exception as err:
        logger.error("作业 %s 发布或删除失败,程序退出!"%etl_job)
        warning_message = traceback.format_exc()
        logging.error(warning_message)
        exit(1)

    logger.info("作业 %s 发布或删除成功,程序退出!"%etl_job)
    exit(0)


    # update_etl_job(logger,etl_conn,etl_server,scriptfile,etl_system,etl_job,description,frequency,jobtype)
    # update_etl_job_dependency(logger,etl_conn,etl_system,etl_job,dependency_system,dependency_job)
    # update_etl_job_stream(logger,etl_conn,etl_system,etl_job,dependency_system,dependency_job)
    # update_etl_job_step(logger,etl_conn,etl_system,etl_job,scriptid,scriptfile)
    # update_etl_job_source(logger,etl_conn,etl_system,etl_job)
    # update_etl_job_timewindow(logger,etl_conn,etl_system,etl_job)