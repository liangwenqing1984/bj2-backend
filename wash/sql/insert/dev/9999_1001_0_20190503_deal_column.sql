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





