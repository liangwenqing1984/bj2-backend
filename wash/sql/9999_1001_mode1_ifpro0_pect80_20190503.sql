drop table if exists isu_db_3.lwq_test_tb_9999;
create table isu_db_3.lwq_test_tb_9999( 
        rowid string,
        column1 string comment '×Ö¶Î1',
        column2 string comment '×Ö¶Î2',
        column3 double comment '×Ö¶Î3',
        column4 date comment '×Ö¶Î4',
        tags string )
partitioned by (isu_type string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;





drop table if exists cls_db_3.lwq_test_tb_9999;
create table cls_db_3.lwq_test_tb_9999( 
        rowid string,
        column1 string comment '×Ö¶Î1',
        column2 string comment '×Ö¶Î2',
        column3 double comment '×Ö¶Î3',
        column4 date comment '×Ö¶Î4',
        tags string )
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;





drop table if exists tmp_db_3.lwq_test_tb_rowid_9999;
create table tmp_db_3.lwq_test_tb_rowid_9999( 
        rowid string,
        column1 string comment '×Ö¶Î1',
        column2 string comment '×Ö¶Î2',
        column3 double comment '×Ö¶Î3',
        column4 date comment '×Ö¶Î4',
        tags string )
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;





drop table if exists tmp_db_3.lwq_test_tb_deal_dump_9999;
create table tmp_db_3.lwq_test_tb_deal_dump_9999( 
        rowid string,
        column1 string comment '×Ö¶Î1',
        column2 string comment '×Ö¶Î2',
        column3 double comment '×Ö¶Î3',
        column4 date comment '×Ö¶Î4',
        tags string )
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;





drop table if exists tmp_db_3.lwq_test_tb_deal_column_9999;
create table tmp_db_3.lwq_test_tb_deal_column_9999( 
        rowid string,
        column1 string comment '×Ö¶Î1',
        column2 string comment '×Ö¶Î2',
        column3 double comment '×Ö¶Î3',
        column4 date comment '×Ö¶Î4',
        tags string )
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;





insert overwrite table tmp_db_3.lwq_test_tb_rowid_9999
select 
        default.md5(concat_ws(',',coalesce(cast(column1 as string),'\N'),coalesce(cast(column2 as string),'\N'),coalesce(cast(column3 as string),'\N'),coalesce(cast(column4 as string),'\N'))),
        column1,
        column2,
        column3,
        column4,
        ''
from his_db_3.lwq_test_tb TABLESAMPLE(80 PERCENT) 
where year=2019 and month=05 and day=03;




from(
    select
        rowid,
        column1,
        column2,
        column3,
        column4,
        row_number() over(partition by rowid ) as rank_num
    from tmp_db_3.lwq_test_tb_rowid_9999 t ) as tt
insert overwrite table tmp_db_3.lwq_test_tb_deal_dump_9999
    select
        rowid,
        column1,
        column2,
        column3,
        column4,
        ''
     where rank_num=1
insert overwrite table isu_db_3.lwq_test_tb_9999 partition(isu_type='1')
    select
        rowid,
        column1,
        column2,
        column3,
        column4,
        'rowid'
     where rank_num>1;





from (
    select * from tmp_db_3.lwq_test_tb_deal_dump_9999
) t
insert into isu_db_3.lwq_test_tb_9999 partition(isu_type='2')
    select
        rowid,
        column1,
        column2,
        column3,
        column4,
        concat(            case when default.canCleanTrim(column1) = 2 then '<Trim#column1>' else '' end,
            case when default.canCleanTrim(column2) = 2 then '<Trim#column2>' else '' end,
            case when default.canCleanTrim(column2) = 2 then '<Trim#column2>' else '' end,
            case when default.canCleanTrim(column2) = 2 then '<Trim#column2>' else '' end)
                                                                                    as tags
where 
    default.canCleanTrim(column1) = 2
    or default.canCleanTrim(column2) = 2
    or default.canCleanTrim(column2) = 2
    or default.canCleanTrim(column2) = 2
insert  overwrite table tmp_db_3.lwq_test_tb_deal_column_9999
    select
        rowid,
        trim(column1),
        trim(trim(trim(column2))),
        column3,
        column4,
        concat(            case when default.canCleanTrim(column1) = 1 then '<Trim#column1>' else '' end,
            case when default.canCleanTrim(column2) = 1 then '<Trim#column2>' else '' end,
            case when default.canCleanTrim(column2) = 1 then '<Trim#column2>' else '' end,
            case when default.canCleanTrim(column2) = 1 then '<Trim#column2>' else '' end)
                                                                                    as tags
where 
    default.canCleanTrim(column1) != 2
    and default.canCleanTrim(column2) != 2
    and default.canCleanTrim(column2) != 2
    and default.canCleanTrim(column2) != 2
;





from(
    select
        rowid,
        column1,
        column2,
        column3,
        column4,
        tags,
        row_number() over(partition by column1,column2) as rank_num
    from tmp_db_3.lwq_test_tb_deal_column_9999 t ) as tt
insert overwrite table cls_db_3.lwq_test_tb_9999
    select
        rowid,
        column1,
        column2,
        column3,
        column4,
        tags
     where rank_num=1
insert overwrite table isu_db_3.lwq_test_tb_9999 partition(isu_type='3')
    select
        rowid,
        column1,
        column2,
        column3,
        column4,
        tags
     where rank_num>1;





