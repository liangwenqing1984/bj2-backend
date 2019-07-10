MASK_DATABASE_USAGE = '06'

CLS_DATABASE_USAGE = '03'

MASK_IMM_MODE = 2

MASK_FREQ_MODE = 3

JOB_FAILED = 2

JOB_DONE = 1

CLS_PARTITION_KEY = 'data_dt'

JOB_TYPE = '''select job_type from prd_data_proc_job where jobid=%s'''


MASK_TABLE_ID = '''select data_tblid from prd_data_proc_job where jobid=%s'''

MASK_CMPU = '''select Data_Wash_Cmpuid,Data_Wash_Cmpu_Cd from data_wash_cmpu where Data_Wash_Cmpu_Type=1'''

IMM_MASK = '''select t1.Fld_Phys_Nm,t3.Data_Wash_Cmpu_Cd
 from data_fld t1
 join data_fld_wash_proj t2
 on t1.fldid=t2.fldid
 join data_wash_cmpu t3
 on t2.Data_Wash_Cmpuid=t3.Data_Wash_Cmpuid
 where t3.Data_Wash_Cmpu_Type=1 and t1.data_tblid=%s and t1.Del_Dt is null
'''

FREQ_MASK = '''select t1.Fld_Phys_Nm,t3.Data_Wash_Cmpu_Cd
 from data_fld t1
 join prd_data_fld_wash_proj t2
 on t1.fldid=t2.fldid
 join data_wash_cmpu t3
 on t2.Data_Wash_Cmpuid=t3.Data_Wash_Cmpuid
 where t3.Data_Wash_Cmpu_Type=1 and t1.data_tblid=%s and t1.Del_Dt is null
'''

MASK_PROCESS = '''insert overwrite table ${target_database}.${target_table} partition(${partition})  
 select ${field} 
 from ${database}.${table} 
 where ${filter}
'''

CREATE_MASK_TABLE = '''create table IF NOT EXISTS ${database}.${table}( 
  ${content}
  )
 COMMENT '${comment}'
 PARTITIONED BY (${partition}) 
 STORED AS PARQUET
'''

FILTER_SINGLE = '''%s=%s'''

FILTER_BETWEEN = '''%s>='%s' and %s<='%s' '''

MASK_UDF = '''default.mask(coalesce(cast(%s as string),''),'%s')'''

MASK_JOB_START = '''update prd_data_proc_job set Job_Exec_Status=0,Job_Start_Time=now() where Jobid=%s'''

MASK_JOB_STATUS = '''update prd_data_proc_job set Job_Exec_Status=%s,Job_Start_Time=%s,
 Job_Finish_Time=now() where Jobid=%s'''

MASK_FLD_RESULT = 'prd_data_wash_fld_result'

MASK_LABLE = '''select t4.Labelid from db t1 join data_part t2 
 on t1.partid=t2.partid 
 join data_tbl t3
 on t3.dbid = t1.dbid
 join label t4
 on t4.Tnmtid=t2.tnmtid
 where t3.data_tblid=%s and t4.Label_Cd='__L000002__' '''

DATA_TBL_MARK_IDX = 'data_tbl_mark_idx'

TABLE_CHANGE = '''select fldid from data_fld where upd_dt=current_date() and data_tblid=%s'''
