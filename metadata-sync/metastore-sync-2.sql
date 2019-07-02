drop table if exists tmpdb.data_tbl_upd;
create table tmpdb.data_tbl_upd like tmpdb.data_tbl;

insert into tmpdb.data_tbl_upd 
select 
t.data_tblid,
t.dbid,
t.data_tbl_phys_nm,
t.data_tbl_cn_nm,
t.data_tbl_desc,
t.create_dt,
case when f.data_tblid is not null then current_date else t.upd_dt end,
t.del_dt,
t.incr_or_full,
coalesce(u.data_tbl_uuid, t.data_tbl_uuid),
coalesce(t.data_srcid, s.data_srcid)
from tmpdb.data_tbl t 
left join ${CLEANSE_DB}.data_src s
	on substr(t.data_tbl_phys_nm, 3, 4) = s.Data_Src_Dep_Cd
	and substr(t.data_tbl_phys_nm, 8, 3) = s.Data_Src_Dep_Sys_Cd
left join tmpdb.data_tbl_uuid u
	on t.data_tblid = u.data_tblid
left join (select distinct data_tblid from tmpdb.data_fld where upd_dt = current_date) f
	on  t.data_tblid = f.data_tblid;


drop table if exists tmpdb.dp;
create table tmpdb.dp like ${CLEANSE_DB}.dp;

insert into tmpdb.dp (data_tblid, dp_dt, dp_path, rec_qty)
SELECT t1.data_tblid, p.part_date, p.part_name, p.numRows
FROM
(
select t1.tbl_id, t1.tbl_name, t2.name as db_name, p.PART_NAME, pp.param_value as numRows,
case when INSTR(p.part_name, 'data_dt=') = 1 
	THEN substr(p.part_name, 9)
	else 
		replace(
			replace(
				replace(
					replace(p.part_name, 
									'access_partition_year=', ''),
					'access_partition_month=', ''),
				'access_partition_day=', ''),
			'/', '-')
	end as part_date
from (select * from ${METASTORE_DB}.tbls where ${METASTORE_TABLE_FILTER}) t1 join
(select db_id, name
	from ${METASTORE_DB}.dbs, ${CLEANSE_DB}.db, ${CLEANSE_DB}.data_part p
	where name = db_phys_nm
	and db.partid = p.partid
	and p.tnmtid = ${TENANT_ID}
) t2
on t1.DB_ID = t2.db_id
join ${METASTORE_DB}.partitions p
on t1.TBL_ID = p.tbl_id
left join (select * from ${METASTORE_DB}.partition_params where param_key = 'numRows') pp
on p.part_id = pp.part_id
) p, 
(SELECT d.db_phys_nm, t.data_tbl_phys_nm, t.data_tblid
from tmpdb.data_tbl t, ${CLEANSE_DB}.db d
where t.dbid = d.dbid
) t1
where p.db_name = t1.db_phys_nm
and p.tbl_name = t1.data_tbl_phys_nm 
;

delete from ${CLEANSE_DB}.data_tbl where del_dt is null;
insert into ${CLEANSE_DB}.data_tbl select * from tmpdb.data_tbl_upd;
delete from ${CLEANSE_DB}.data_fld where del_dt is null;
insert into ${CLEANSE_DB}.data_fld select * from tmpdb.data_fld;
delete from ${CLEANSE_DB}.dp; 
insert into ${CLEANSE_DB}.dp select * from tmpdb.dp;



