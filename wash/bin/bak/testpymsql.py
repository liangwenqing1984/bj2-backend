#!/bin/env python
# -*- coding: UTF-8 -*-


import pymysql
import configparser

def get_isu_conn(config):
    isu_conn = pymysql.connect(host=config.get('mysql_isu', 'dbhost'),
                           user=config.get('mysql_isu', 'dbuser'),
                           password=config.get('mysql_isu', 'dbpasswd'),
                           db=config.get('mysql_isu', 'db'),
                           port=int(config.get('mysql_isu', 'dbport')),
                           charset=config.get('mysql_isu', 'charset'),
                           cursorclass=pymysql.cursors.DictCursor)
    return isu_conn


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('../../etc/app.properties')
    conn = get_isu_conn(config)
    cursor = conn.cursor()
    tb = "lwq_test_tb"
    columns = "id int comment 'id',name varchar(20) comment '姓名'"
    # sql = "select * from data_tbl where Data_Tbl_Phys_Nm = '" + tb +"';"
    sql = "create table " + tb + "(" +columns+")"
    print(sql)
    cursor.execute(sql)
    result = cursor.fetchall()
    print(result)