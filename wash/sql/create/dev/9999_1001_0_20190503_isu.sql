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





