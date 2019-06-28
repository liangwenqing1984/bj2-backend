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





