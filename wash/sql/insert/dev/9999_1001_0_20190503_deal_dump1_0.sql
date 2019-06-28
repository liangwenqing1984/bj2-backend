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





