#!/bin/env python
# -*- coding: UTF-8 -*-
import pymysql
import sys
import os



#更新 etl_job 表
def update_etl_job(logger,etl_conn,etl_server,scriptfile,etl_system,etl_job,description,frequency,jobtype):
    try:
        cursor = etl_conn.cursor()
        etl_job_sql =   "replace into etl_job(ETL_System,ETL_Job,ETL_Server,Description,Frequency,JobType,Enable,Last_JobStatus,Last_FileCnt,CubeFlag,CheckFlag,AutoOff,CheckCalendar,RunningScript,JobSessionID,ExpectedRecord,CheckLastStatus,TimeTrigger,Priority) values( "\
                "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(etl_system,etl_job,etl_server,description,frequency,jobtype,'1','Ready','0','N','N','N','N',scriptfile,'0','0','Y','N','0')
        logger.info("etl_job_sql====\n" + etl_job_sql)
        cursor.execute(etl_job_sql)
        # etl_conn.commit()
    except Exception as err:
        logger.error("更新 etl_job 表失败，作业名称：%s "%etl_job)
        raise err
    finally:
        cursor.close()


#更新 etl_job_dependency 表
def update_etl_job_dependency(logger,etl_conn,jobcat,etl_system,etl_job,wash_start_system,wash_start_job,wash_end_system,wash_end_job):
    try:
        cursor = etl_conn.cursor()
        if(jobcat == '1'):
            etl_job_dependency_sql =   "replace into etl_job_dependency(etl_system,etl_job,dependency_system,dependency_job,enable) values( "\
                    "'{}','{}','{}','{}','{}')".format(etl_system,etl_job,wash_start_system,wash_start_job,'1')
            logger.info("etl_job_dependency_sql====\n" + etl_job_dependency_sql)
            cursor.execute(etl_job_dependency_sql)
            wash_end_job_dependency_sql =  "replace into etl_job_dependency(etl_system,etl_job,dependency_system,dependency_job,enable) values( "\
                    "'{}','{}','{}','{}','{}')".format(wash_end_system,wash_end_job,etl_system,etl_job,'1')
            logger.info("wash_end_job_dependency_sql====\n" + wash_end_job_dependency_sql)
            cursor.execute(wash_end_job_dependency_sql)
        elif(jobcat == '2'):
            etl_job_dependency_sql =   "replace into etl_job_dependency(etl_system,etl_job,dependency_system,dependency_job,enable) values( "\
                    "'{}','{}','{}','{}','{}')".format(etl_system,etl_job,wash_end_system,wash_end_job,'1')
            logger.info("etl_job_dependency_sql====\n" + etl_job_dependency_sql)
            cursor.execute(etl_job_dependency_sql)
        # etl_conn.commit()
    except Exception as err:
        logger.error("更新 etl_job_dependency 表失败，作业名称：%s "%etl_job)
        raise err
    finally:
        cursor.close()


#更新 etl_job_stream 表
def update_etl_job_stream(logger,etl_conn,etl_system,etl_job,dependency_system,dependency_job):
    try:
        cursor = etl_conn.cursor()
        etl_job_stream_sql =   "replace into etl_job_stream(etl_system,etl_job,stream_system,stream_job,enable) values( "\
                "'{}','{}','{}','{}','{}')".format(dependency_system,dependency_job,etl_system,etl_job,'1')
        logger.info("etl_job_stream_sql====\n" + etl_job_stream_sql)
        cursor.execute(etl_job_stream_sql)
        # etl_conn.commit()
    except Exception as err:
        logger.error("更新 etl_job_stream 表失败，作业名称：%s "%etl_job)
        raise err
    finally:
        cursor.close()

#更新 etl_job_step 表
def update_etl_job_step(logger,etl_conn,etl_system,etl_job,scriptid,scriptfile):
    try:
        cursor = etl_conn.cursor()
        etl_job_step_sql =   "replace into etl_job_step(etl_system,etl_job,jobstepid,scriptid,scriptfile,enable) values( "\
                "'{}','{}','{}','{}','{}','{}')".format(etl_system,etl_job,'0100',scriptid,scriptfile,'1')
        logger.info("etl_job_step_sql====\n" + etl_job_step_sql)
        cursor.execute(etl_job_step_sql)
        # etl_conn.commit()
    except Exception as err:
        logger.error("更新 etl_job_step 表失败，作业名称：%s "%etl_job)
        raise err
    finally:
        cursor.close()

#更新 etl_job_source 表
def update_etl_job_source(logger,etl_conn,etl_system,etl_job):
    try:
        cursor = etl_conn.cursor()
        etl_job_source_sql =   "replace into etl_job_source(Source,ETL_System,ETL_Job,Conv_File_Head,AutoFilter,Alert,BeforeHour,BeforeMin,OffsetDay) values( "\
                "'{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(etl_job,etl_system,etl_job,etl_job,'0','0','0','0','0')
        logger.info("etl_job_source_sql====\n" + etl_job_source_sql)
        cursor.execute(etl_job_source_sql)
        # etl_conn.commit()
    except Exception as err:
        logger.error("更新 etl_job_source 表失败，作业名称：%s "%etl_job)
        raise err
    finally:
        cursor.close()

#更新 etl_job_timewindow 表
def update_etl_job_timewindow(logger,etl_conn,etl_system,etl_job):
    try:
        cursor = etl_conn.cursor()
        etl_job_timewindow_sql =   "replace into etl_job_timewindow(etl_system,etl_job,allow,beginhour,endhour) values( "\
                "'{}','{}','{}','{}','{}')".format(etl_system,etl_job,'Y','0','23')
        logger.info("etl_job_timewindow_sql====\n" + etl_job_timewindow_sql)
        cursor.execute(etl_job_timewindow_sql)
        # etl_conn.commit()
    except Exception as err:
        logger.error("更新 etl_job_timewindow 表失败，作业名称：%s "%etl_job)
        raise err
    finally:
        cursor.close()

#建立作业软链接
def crt_link(logger,conf,etl_system,etl_job,scriptfile):
    try:
        etl_home = conf.get("etlhome","etl_home")
        srcfile  = os.path.join(etl_home,"app/washtask.sh ")
        logger.info("srcfile====\n" + srcfile)
        linkdir = os.path.join(etl_home,"APP",etl_system,etl_job,"bin")
        logger.info("linkdir====\n" + linkdir)
        isExist = os.path.exists(linkdir)
        if not isExist:
            os.makedirs(linkdir)
        else:
            os.system("rm -f " + linkdir + "/*")
        linkfile = os.path.join(linkdir,scriptfile)
        logger.info("linkfile====\n" + linkfile)
        if(not os.path.exists(linkfile)):
            cmd = "ln -s " + srcfile + linkfile
            rtcode = os.system(cmd)
            if rtcode != 0:
                logger.error("创建软链接失败,linkfile=%s"%linkfile)
                raise Exception("创建软链接失败,linkfile=%s"%linkfile)
    except Exception as err:
        logger.error("创建软链接 %s 失败 "%linkfile)
        raise err
    finally:
        pass


# 根据表id获取租户etl_system
def get_etl_system_by_tbid(logger,conn,tbid):
    try:
        cursor = conn.cursor();
        sql = "select t4.etl_syscd from data_tbl t1 " + \
              "left join db t2 on t1.Dbid = t2.dbid " + \
              "left join data_part t3 on t2.Partid = t3.partid " + \
              "left join tnmt t4 on t3.Tnmtid = t4.Tnmtid " + \
              "where t1.Data_Tblid= {} ".format(tbid)
        logger.info(sql)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result == None  or len(result)==0:
            logger.error("根据表id: %s 未查找到相应的租户etl_syscd!"%tbid)
            raise Exception("[根据表id: %s 未查找到相应的租户etl_syscd!]"%tbid)
        etl_syscd = result.get("etl_syscd")
        logger.info("etl_syscd====\n" + str(etl_syscd))
    except Exception as err:
        logger.error("根据表id %s 查找租户etl_syscd失败:   %s "%(tbid,err))
        raise err
    finally:
        cursor.close()
    return etl_syscd


# 根据表id查找表中文名称
def get_tb_cn_nm_by_tbid(logger,conn,tbid):
    try:
        cursor = conn.cursor();
        sql = "select data_tbl_cn_nm from data_tbl where data_tblid = {} ".format(tbid)
        logger.info(sql)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result == None or len(result)==0:
            logger.error("根据表id: %s 未查找到表中文名称" %tbid)
            raise Exception("[根据表id: %s 未查找到表中文名称]" %tbid)
        data_tbl_cn_nm = result.get("data_tbl_cn_nm")
        logger.info("data_tbl_cn_nm====\n"+data_tbl_cn_nm)
    except Exception as err:
        logger.error("根据表id: %s 查找表中文名称失败%s" %(tbid,err))
        raise err
    finally:
        cursor.close()
    return data_tbl_cn_nm

#创建调度作业
def create_etl_job(logger,conf,etl_conn,jobcat,etl_server,scriptid,scriptfile,etl_system,etl_job,description,frequency,jobtype,wash_start_system,wash_start_job,wash_end_system,wash_end_job):
    try:
        update_etl_job(logger,etl_conn,etl_server,scriptfile,etl_system,etl_job,description,frequency,jobtype)
        update_etl_job_dependency(logger,etl_conn,jobcat,etl_system,etl_job,wash_start_system,wash_start_job,wash_end_system,wash_end_job)
        # update_etl_job_stream(logger,etl_conn,etl_system,etl_job,dependency_system,dependency_job)
        update_etl_job_step(logger,etl_conn,etl_system,etl_job,scriptid,scriptfile)
        update_etl_job_source(logger,etl_conn,etl_system,etl_job)
        update_etl_job_timewindow(logger,etl_conn,etl_system,etl_job)
        crt_link(logger,conf,etl_system,etl_job,scriptfile)
        etl_conn.commit()
    except Exception as err:
        logger.error("创建作业 %s 失败 "%etl_job)
        etl_conn.rollback()
        raise err
    finally:
        pass

# 删除作业(使作业失效)
def  del_job(logger,etl_conn,jobcat,etl_system,etl_job,wash_end_system,wash_end_job):
     try:
        cursor = etl_conn.cursor()
        if(jobcat == '1'):
            unenable_job_sql =   "update etl_job set enable='0' where etl_system='{}' and etl_job='{}';".format(etl_system,etl_job)
            uneanble_end_dependency_sql = "update etl_job_dependency set enable = '0' where etl_system='{}' and etl_job='{}' and dependency_system='{}' and dependency_job='{}'".format(wash_end_system,wash_end_job,etl_system,etl_job)
            logger.info("unenable_job_sql====\n" + unenable_job_sql)
            cursor.execute(unenable_job_sql)
            logger.info("uneanble_end_dependency_sql====\n" + uneanble_end_dependency_sql)
            cursor.execute(uneanble_end_dependency_sql)
        elif(jobcat == '2'):
            unenable_job_sql =   "update etl_job set enable='0' where etl_system='{}' and etl_job='{}';".format(etl_system,etl_job)
            logger.info("unenable_job_sql====\n" + unenable_job_sql)
            cursor.execute(unenable_job_sql)
        etl_conn.commit()
     except Exception as err:
         etl_conn.rollbak()
         logger.error("将作业 %s 置为失效失败" %etl_job)
         raise err
     finally:
         cursor.close()