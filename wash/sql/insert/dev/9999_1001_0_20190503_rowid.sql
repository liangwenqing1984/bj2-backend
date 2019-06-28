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




