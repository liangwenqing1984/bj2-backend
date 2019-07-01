# -*- coding: UTF-8 -*-
#!/bin/env python

import etl_service as es
import logging
import sys

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
if len(sys.argv) < 3 or len(sys.argv) > 4:
    logging.error('参数数量错误，使用方式为: python meta_etl.py dbname yyyy-mm-dd')
    exit(1)
date = sys.argv[2]
ymd = str(date).split('-')
db = sys.argv[1]
y = ymd[0]
m = ymd[1]
d = ymd[2]
if not (len(y) == 4 and len(m) == 2 and len(d) == 2):
    logging.error('日期参数错误，使用方式为: python meta_etl.py yyyy-mm-dd')
    exit(1)

if len(sys.argv) == 4 and sys.argv[3] == 'first':
    logging.info('---开始插入新表')
    es.insertNewT(db,date)
    logging.info('---插入新表结束')

    logging.info('---开始插入新分区')
    es.insertNewP(db,date)
    logging.info('---插入新分区结束')

    logging.info('---开始插入新增字段')
    es.insertNewC(db,date)
    logging.info('---插入新增字段结束')
elif len(sys.argv) == 3:
    logging.info('---开始插入新表')
    es.insertNewT(db,date)
    logging.info('---插入新表结束')

    logging.info('---开始更新修改表')
    es.updateT(db,date)
    logging.info('---更新修改表结束')

    logging.info('---开始删除表')
    es.deleteT(db,date)
    logging.info('---删除表结束')

    logging.info('---开始插入新分区')
    es.insertNewP(db,date)
    logging.info('---插入新分区结束')

    logging.info('---开始插入新增字段')
    es.insertNewC(db,date)
    logging.info('---插入新增字段结束')

    logging.info('---开始更新修改字段')
    es.updateC(db,date)
    logging.info('---更新修改字段结束')

    logging.info('---开始删除字段')
    es.deleteC(db,date)
    logging.info('---删除字段结束')
else:
    print '---- 参数错误！'
