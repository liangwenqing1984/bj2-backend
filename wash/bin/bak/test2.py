#!/bin/env python
# -*- coding: UTF-8 -*-


from pyhive import hive
import configparser



if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('./app.properties')
    # conn = hive.Connection(host='192.168.137.130', port=10000, username='hive', database='default')
    hive_conn = hive.Connection(host=config.get('hive', 'host'),
                                port=config.get('hive', 'port'),
                                username=config.get('hive', 'username'),
                                database=config.get('hive', 'database')
                                )

    cursor = hive_conn.cursor()
    cursor.execute("select * from his_db_3.lwq_test_tb")


    result = cursor.fetchall()
    for i in range(len(result)):
        print(result[i][0])

