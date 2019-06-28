#!/bin/env python
# -*- coding: UTF-8 -*-
import pymysql
import sys
import os

#根据表id获取 作业名称、作业系统、作业描述、

#更新 etl_job 表
def update_etl_job(logger,etl_conn,conf,etl_system,etl_job,description,frequency,jobtype):
    etl_server = conf.get("etl_home","etl_server")
    scriptfile = conf.get("etl_home","scriptfile")
    if(jobtype == 'D'):
        frequency = '0'
    try:
        cursor = etl_conn.cursor()
        etl_job_sql =   "replace into etl_job(etl_system,etl_job,etl_server,description,frequency,jobtype,enable,last_jobstatus,runningscript) values( "\
                "'{}','{}','{}','{}','{}','{}','{}')".format(etl_system,etl_job,etl_server,description,frequency,jobtype,'1','Ready',scriptfile)
        logger.info("etl_job_sql====\n" + etl_job_sql)
        cursor.execute(etl_job_sql)
    except Exception as err:
        raise err
    finally:
        cursor.close()


#更新 etl_job_dependency 表
def update_etl_job_dependency(logger,etl_conn,etl_system,etl_job):
    try:
        cursor = etl_conn.cursor()
        etl_job_dependency_sql =   "replace into etl_job_dependency(etl_system,etl_job,dependency_system,dependency_job,enable) values( "\
                "'{}','{}','{}','{}','{}')".format(etl_system,etl_job,'1','dummy','1')
        cursor.execute(etl_job_dependency_sql)
    except Exception as err:
        raise err
    finally:
        cursor.close()


#更新 etl_job_stream 表
def update_etl_job_stream(logger,etl_conn,etl_system,etl_job):
    try:
        cursor = etl_conn.cursor()
        etl_job_stream_sql =   "replace into etl_job_stream(etl_system,etl_job,stream_system,stream_job,enable) values( "\
                "'{}','{}','{}','{}','{}')".format('1','dummy',etl_system,etl_job,'1')
        cursor.execute(etl_job_stream_sql)
    except Exception as err:
        raise err
    finally:
        cursor.close()

#更新 etl_job_step 表
def update_etl_job_step(logger,etl_conn,etl_system,etl_job):
    try:
        scriptfile = "wash0100.sh"
        cursor = etl_conn.cursor()
        etl_job_step_sql =   "replace into etl_job_step(etl_system,etl_job,jobstepid,scriptfile,enable) values( "\
                "'{}','{}','{}','{}','{}')".format(etl_system,etl_job,scriptid,scriptfile,'1')
        cursor.execute(etl_job_step_sql)
    except Exception as err:
        raise err
    finally:
        cursor.close()

#更新 etl_job_source 表
def update_etl_job_source(logger,etl_conn,etl_system,etl_job):
    try:
        cursor = etl_conn.cursor()
        etl_job_source_sql =   "replace into etl_job_source(source,etl_system,etl_job,conv_file_head) values( "\
                "'{}','{}','{}','{}')".format(etl_job,etl_system,etl_job,etl_job)
        cursor.execute(etl_job_source_sql)
    except Exception as err:
        raise err
    finally:
        cursor.close()

#更新 etl_job_timewindow 表
def update_etl_job_timewindow(logger,etl_conn,etl_system,etl_job):
    try:
        cursor = etl_conn.cursor()
        etl_job_timewindow_sql =   "replace into etl_job_timewindow(etl_system,etl_job,allow,beginhour,endhour) values( "\
                "'{}','{}','{}','{}')".format(etl_system,etl_job,'Y','0','23')
        cursor.execute(etl_job_timewindow_sql)
    except Exception as err:
        raise err
    finally:
        cursor.close()

#建立作业软链接
def crt_link(conf,etl_system,etl_job):
    try:
        scriptfile = "wash0100.sh "
        etl_home = conf.get("etlhome","etl_home")
        wash_dir = conf.get("etlhome","wash_dir")
        linkdir = "/".join((etl_home,etl_job,"bin"))
        isExist = os.path.exists(linkdir)
        if not isExist:
            os.makedirs(linkdir)
        linkfile = linkdir+"/wash0100.sh"
        if(not os.path.exists(linkfile)):
            cmd = "ln -s " + wash_dir+ " /wash.sh " + linkdir + scriptfile
            os.system(cmd)
    except Exception as err:
        raise err
    finally:
        pass

#创建作业
def create_job():
    pass