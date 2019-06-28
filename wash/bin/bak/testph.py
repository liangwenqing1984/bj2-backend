#!/bin/env python
# -*- coding: UTF-8 -*-


from pyhive import hive

conn = hive.Connection(host='192.168.137.130', port=10000, username='hive', database='default')
cursor = conn.cursor()
sql =   "select sum(Proc_Rec_Total_Qty),sum(All_Dupl_Rec_Qty),sum(Pk_Dupl_Rec_Qty) from ( "\
        "select count(1) as Proc_Rec_Total_Qty ,0 as All_Dupl_Rec_Qty,0 as Pk_Dupl_Rec_Qty from cls_db_3.lwq_test_tb where data_dt='2019-05-03' and tags is not null  "\
        "union "\
        "select 0 as Proc_Rec_Total_Qty ,count(1) as All_Dupl_Rec_Qty,0 as Pk_Dupl_Rec_Qty from isu_db_3.lwq_test_tb where data_dt='2019-05-03' and isu_type='1' "\
        "union "\
        "select 0 as Proc_Rec_Total_Qty,0 as All_Dupl_Rec_Qty,count(1) as Pk_Dupl_Rec_Qty from isu_db_3.lwq_test_tb where data_dt='2019-05-03' and isu_type='3' "\
        ") t"
cursor.execute(sql)
result = cursor.fetchone()
if result != None:
    print(result)
    Proc_Rec_Total_Qty = result[0]
    All_Dupl_Rec_Qty = result[1]
    Pk_Dupl_Rec_Qty = result[2]


