drop table if exists tmpdb.cts;
create table tmpdb.cts
		select tbl.*, db_phys_nm 
	  from ${CLEANSE_DB}.db db, ${CLEANSE_DB}.data_tbl tbl
    where db.dbid = tbl.dbid  
		and tbl.del_dt is null
;

drop table if exists tmpdb.hts;
create table tmpdb.hts
			select t2.name as db_name, tbl_name, cls_dbid, param_value as cn_name 
			from ${METASTORE_DB}.tbls t1 join
				(select db_id, name, dbid as cls_dbid
					from ${METASTORE_DB}.dbs, ${CLEANSE_DB}.db
					where name = db_phys_nm
				) t2
			on t1.DB_ID = t2.db_id
			left join (select * from ${METASTORE_DB}.table_params where param_key = 'comment') t3
      on t1.tbl_id = t3.tbl_id
;

drop table if exists tmpdb.data_tbl;
create table tmpdb.data_tbl like ${CLEANSE_DB}.data_tbl;
insert into tmpdb.data_tbl (data_tblid) select max(data_tblid) from ${CLEANSE_DB}.data_tbl;
delete from tmpdb.data_tbl;

insert into tmpdb.data_tbl 
select data_tblid, 
			coalesce(dbid, cls_dbid), 
			coalesce(data_tbl_phys_nm, tbl_name), 
			coalesce(data_tbl_cn_nm, cn_name), 
			coalesce(data_tbl_desc, null), 
			coalesce(create_dt, current_date), 
			coalesce(upd_dt, current_date), 
			(case when tbl_name is null then current_date else null end) as del_dt,
			incr_or_full,
			data_tbl_uuid,
			data_srcid
from 
(select *
from tmpdb.cts left join tmpdb.hts
		on concat(cts.db_phys_nm, cts.data_tbl_phys_nm) = concat(hts.db_name, hts.tbl_name)
UNION
select *
from tmpdb.cts right join tmpdb.hts
		on concat(cts.db_phys_nm, cts.data_tbl_phys_nm) = concat(hts.db_name, hts.tbl_name)
) t
;



drop table if exists tmpdb.ccs;
create table tmpdb.ccs
(
  `Fldid` int(11) NOT NULL ,
  `Data_Tblid` int(11) ,
  `Fld_Phys_Nm` varchar(128) ,
  `Fld_Cn_Nm` varchar(128) ,
  `Fld_Data_Type` varchar(30) ,
  `Fld_Desc` varchar(255) ,
  `Fld_Ord` int(11) ,
  `If_Pk` int(11) ,
  `If_Can_Null` int(11),
  `Create_Dt` date,
  `Upd_Dt` date ,
  `Del_Dt` date ,
	`data_tbl_phys_nm` varchar(128),
	`db_phys_nm`	varchar(128),
	`col_full_name_ccs`	varchar(128),
	`tbl_full_name_ccs`	varchar(128)
)
		select f.*, t.Data_Tbl_Phys_Nm, d.Db_Phys_Nm, 
					concat(d.Db_Phys_Nm, '.', t.Data_Tbl_Phys_Nm, '.', f.Fld_Phys_Nm) as col_full_name_ccs,
					concat(d.Db_Phys_Nm, '.', t.Data_Tbl_Phys_Nm) as tbl_full_name_ccs
	  from ${CLEANSE_DB}.db d, ${CLEANSE_DB}.data_tbl t, ${CLEANSE_DB}.data_fld f
    where d.dbid = t.dbid
		and t.data_tblid = f.data_tblid  
		and f.del_dt is null
;

drop table if exists tmpdb.hcs;
create table tmpdb.hcs
(
	cd_id	bigint not null,
	comment varchar(256),
	column_name varchar(767),
	type_name varchar(4000),
	integer_idx int,
	tbl_name varchar(128),
	db_name	varchar(128),
	col_full_name_hcs	varchar(128),
	tbl_full_name_hcs	varchar(128)
)
			select c.*, t.TBL_NAME, d.db_name, 
						concat(d.db_name, '.', t.tbl_name, '.', c.COLUMN_NAME) as col_full_name_hcs,
						concat(d.db_name, '.', t.tbl_name) as tbl_full_name_hcs
			from ${METASTORE_DB}.columns_v2 c, ${METASTORE_DB}.sds, ${METASTORE_DB}.tbls t,
				(select db_id, name as db_name, dbid as cls_dbid
					from ${METASTORE_DB}.dbs, ${CLEANSE_DB}.db
					where name = db_phys_nm
				) d
			where d.DB_ID = t.db_id
			and t.sd_id = sds.sd_id
			and sds.cd_id = c.cd_id
;

create index ccsidx on tmpdb.ccs(col_full_name_ccs);
create index hcsidx on tmpdb.hcs(col_full_name_hcs);

drop table if exists tmpdb.data_fld;
create table tmpdb.data_fld like ${CLEANSE_DB}.data_fld;
insert into tmpdb.data_fld (fldid) select max(fldid) from ${CLEANSE_DB}.data_fld;
delete from tmpdb.data_fld;

insert into tmpdb.data_fld
select c.fldid
,t.data_tblid
,coalesce(c.fld_phys_nm, c.column_name)
,coalesce(c.fld_cn_nm, c.comment)
,coalesce(c.type_name, c.fld_data_type)
,coalesce(c.fld_desc, c.comment)
,coalesce(c.integer_idx, c.fld_ord)
,c.if_pk
,c.if_can_null
,coalesce(c.create_dt, current_date)
,case when c.fld_phys_nm is not NULL
				and c.column_name is not NULL
				and (c.fld_data_type <> c.type_name or c.fld_ord <> c.integer_idx)
			then CURRENT_DATE
			else coalesce(c.upd_dt, current_date) end
,case when c.column_name is null then current_date else null end
from
(select *, coalesce(tbl_full_name_ccs, tbl_full_name_hcs) as tbl_full_name
from tmpdb.ccs left join tmpdb.hcs
		on col_full_name_ccs = hcs.col_full_name_hcs
UNION
select *, coalesce(tbl_full_name_ccs, tbl_full_name_hcs) as tbl_full_name
from tmpdb.ccs right join tmpdb.hcs
		on col_full_name_ccs = hcs.col_full_name_hcs
) c,
(select t.data_tblid, t.data_tbl_phys_nm, db.db_phys_nm, 
	concat(db.Db_Phys_Nm, '.', t.data_tbl_phys_nm) as tbl_full_name
	from tmpdb.data_tbl t, ${CLEANSE_DB}.db
	WHERE t.dbid = db.Dbid
) t
where 
	c.tbl_full_name= t.tbl_full_name
;

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
t.data_tbl_uuid,
s.data_srcid
from tmpdb.data_tbl t left join ${CLEANSE_DB}.data_src s
	on substr(t.data_tbl_phys_nm, 3, 4) = s.Data_Src_Dep_Cd
	and substr(t.data_tbl_phys_nm, 8, 3) = s.Data_Src_Dep_Sys_Cd
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
from ${METASTORE_DB}.tbls t1 join
(select db_id, name
	from ${METASTORE_DB}.dbs, ${CLEANSE_DB}.db
	where name = db_phys_nm
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


