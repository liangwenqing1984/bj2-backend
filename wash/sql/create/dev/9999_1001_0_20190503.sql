drop table if exists cls_db_3.lwq_test_tb_9999;
create table cls_db_3.lwq_test_tb_9999( 
        rowid string,
        column1 string comment '�ֶ�1',
        column2 string comment '�ֶ�2',
        column3 double comment '�ֶ�3',
        column4 date comment '�ֶ�4',
        tags string )
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;





