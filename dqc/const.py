CHECK_NULL = 'null'

ROWID_FIELD = 'leiyry'

RAND_FIELD = 'leiyry_rand'

NULL_NVL = '\\N'

DB_TABLE = '''select db_phys_nm,data_tbl_phys_nm 
 from data_tbl t1 join db t2 on t1.dbid=t2.dbid 
 where t1.data_tblid=%s
'''

MAX_PARTITION_DATE = '''select max(dp_dt) as latest from dp where data_tblid=%s'''

PARTITION_PATH = '''select dp_path from dp where data_tblid=%s and dp_dt=%s'''

TABLE_FIELD = '''select fld_phys_nm,fld_data_type,if_pk,if_can_null
 from data_fld where data_tblid=%s and del_dt is null
 order by fld_ord asc
'''

FIELD_CHECK = '''select t1.fld_phys_nm,t3.data_expl_tmplid,t5.chk_proj_cd
 from data_fld t1 join data_fld_expl_proj t2
 on t1.fldid=t2.fldid
 join data_expl_tmpl t3
 on t2.data_expl_tmplid=t3.data_expl_tmplid
 join data_expl_tmpl_proj t4
 on t3.data_expl_tmplid=t4.data_expl_tmplid
 join data_chk_proj t5
 on t4.chk_projid=t5.chk_projid
 where t1.data_tblid=%s and t1.del_dt is null
 order by t1.fld_ord
'''

CHECK_ITEM = '''select Chk_Projid,Chk_Proj_Cd from data_chk_proj'''

FIELD_TABLE = '''select Fldid,Fld_Phys_Nm from data_fld where Data_Tblid=%s and Del_Dt is null'''

TARGET_DATABASE = '''select tt2.db_phys_nm from data_part tt1 
 join db tt2 on tt1.partid=tt2.partid
 join 
 (select t2.tnmtid from db t1 join data_part t2 
 on t1.partid=t2.partid
 where t1.db_phys_nm=%s) tt3
 on tt1.tnmtid=tt3.tnmtid
 join db_usage tt4
 on tt2.Db_Usageid=tt4.Db_Usageid
 where tt4.db_usage_cd='05'
'''

DROP_SQL = '''drop table IF EXISTS %s.%s'''

CREATE_SQL = '''create table ${database}.${table}( 
  ${content}
  )
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\\001'
LINES TERMINATED BY '\\n' 
STORED AS TEXTFILE
'''

BASE_SQL = '''insert overwrite table ${target_database}.${target_table} 
 select ${field},${udf} ${md5},rand() as leiyry_rand from ${database}.${table}
 where ${partition}
 order by leiyry_rand
 ${limit}
'''

MD5_SQL = '''default.md5(concat_ws(',',%s))'''

NVL_SQL = '''coalesce(cast(%s as string),'%s')'''

UDF_SQL = '''default.dqc(coalesce(cast(%s as string),''),'%s')'''

STATS_SQL = '''select ${stats} from ${target_database}.${target_table}
'''

SUM_SQL = '''sum(%s) as %s'''

COUNT_SQL = '''select count(*) as total from ${database}.${table} where ${partition}'''

DUPLICATE_RECORD_SQL = '''select count(*) as duplicate from (select *,
 row_number() over (partition by leiyry) num 
 from ${target_database}.${target_table} ) t where t.num>1
'''

DUPLICATE_KEYS_SQL = '''select count(*) as duplicate from 
 (select ${pk},count(*) 
 from ${target_database}.${target_table} 
 group by ${pk} 
 having count(*)>1) t
'''

FAILED_JOB = '''update data_proc_job set Job_Stus='FAILED',Rfrsh_Tm=now() where Jobid=%s'''

UPDATE_TOTAL_RECORD = '''update data_proc_job set Job_Stus='DONE',Rfrsh_Tm=now(),Proc_Rec_Total_Qty=%s,
 Data_Start_Dt=%s,Data_Terminate_Dt=%s where Jobid=%s'''

TABLE_CHECK_RESULT = '''update data_proc_job set Job_Stus='DONE',Rfrsh_Tm=now(),
 All_Dupl_Rec_Qty=%s,Pk_Dupl_Rec_Qty=%s,Proc_Rec_Total_Qty=%s,Data_Start_Dt=%s,Data_Terminate_Dt=%s
 where Jobid=%s'''


LIMIT_SQL = '''limit %s'''

FLD_EXPL_RESULT = 'fld_expl_result'
