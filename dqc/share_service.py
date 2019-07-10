import config
from mysql import MySQL

SHARE_SQL = '''select concat(t5.Db_Phys_Nm,'_',t2.Data_Tbl_Phys_Nm) as table_en,t2.Data_Tbl_Cn_Nm as table_cn,db_style,host_address,host_port,
 db_name,res_desc,'table' as res_type,
 case when collect_type=1 then 'swap' when collect_type=2 then 'ingest' end as collect_type,
 'jdbc' as share_type,coalesce(t4.Data_Src_Nm,'未知') as source_department,
 cast(t3.Dp_Dt as char) as update_time,'信息资源中心' as process_department 
 from shared_resource t1 left join data_tbl t2 on t1.data_tblid=t2.data_tblid 
 left join (select Data_Tblid,max(Dp_Dt) as Dp_Dt from dp group by Data_Tblid) t3 on t2.Data_Tblid=t3.Data_Tblid 
 left join data_src t4 on t2.Data_Srcid=t4.Data_Srcid
 join db t5
 on t2.dbid = t5.dbid
 where t2.Del_Dt is null
'''

ACCESS_SQL = '''select t2.Db_Cn_Nm as 'database',
 coalesce(t1.Data_Tbl_Cn_Nm,'') as table_cn,Data_Tbl_Phys_Nm as table_en,
 t1.Data_Tbl_UUID as 'uuid','table' as res_type,coalesce(t5.department_cd,'未知') as department_code
 from data_tbl t1 
 join db t2
 on t1.Dbid=t2.Dbid
 join db_usage t3
 on t2.Db_Usageid=t3.Db_Usageid
 left join data_src t4
 on t1.Data_Srcid=t4.Data_Srcid
 left join department t5
 on t5.department_id=t4.Department_Id
 where t3.Db_Usage_Cd in ('02','03','06') and t1.Del_Dt is null
'''


def access():
    db = MySQL(config.dqc_mysql)
    result = db.execute(ACCESS_SQL)
    del db
    return result


def share():
    db = MySQL(config.dqc_mysql)
    result = db.execute(SHARE_SQL)
    del db
    return result
