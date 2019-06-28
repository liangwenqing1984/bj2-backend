#!/bin/env python
# -*- coding: UTF-8 -*-


from pyhive import hive
import configparser



if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('../../etc/app.properties')
    # conn = hive.Connection(host='192.168.137.130', port=10000, username='hive', database='default')
    hive_conn = hive.Connection(host=config.get('hiveCon', 'host'),
                                port=config.get('hiveCon', 'port'),
                                username=config.get('hiveCon', 'username'),
                                database=config.get('hiveCon', 'database')
                                )

    cursor = conn.cursor()
    cursor.execute("select * from his_db_3.lwq_test_tb")


    result = cursor.fetchone()
    if result != None:
        print(result)
        print(result[0])

