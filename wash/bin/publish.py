#!/bin/env python
# -*- coding: UTF-8 -*-
import sys
from publish_meta import *

def pub_help():
    print("参数错误!\n\t示例:python publish.py 1001 D 0\n"
          "\t参数1:表id\n"
          "\t参数2:调度周期(D:日 M:月)\n"
          "\t参数3:调度日期(0,1,2,3...31)\n"
          )

#获得mysql数据库连接
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
        logger.error("获得etldb数据库连接失败 %s"  %err)
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

def process(logger,etl_conn,etl_server,etl_system,etl_job,description,job_type,frequency,dependency_system,dependency_job):
    pass

if __name__ == '__main__':
    if len(sys.argv) != 2:
        pub_help()
        exit(1)

    tbid = sys.argv[1]
    etl_server = "etl1"
    etl_system = "clsdb"
    etl_job = "clsdb_test1"
    description = "测试调度作业"
    frequency = "0"
    job_type = "D"
    dependency_system = "hisdb"
    dependency_job = "dummpy"

    config = configparser.ConfigParser()
    config.read('../etc/app.properties')
    etl_conn = get_etldb_conn(config)
    logiflenm = "_".join((tbid,job_type,frequency)) + ".log"
    publish_log_file = "../log/" + logiflenm
    logger = get_logger(publish_log_file)




