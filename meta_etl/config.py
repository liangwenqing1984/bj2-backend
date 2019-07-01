#!/bin/env python
# -*- coding: UTF-8 -*-
import sys

try:
    reload(sys)
    sys.setdefaultencoding("utf-8")
except AttributeError:
    pass

# hive ={
#     'host':'10.1.3.114',
#     'port':10000,
#     'username':'xiaoy',
#     'passwd':'123456',
#     'charset':'utf8'
# }

hivemeta ={
    'host':'10.1.2.33',
    'port':3306,
    'db':'metastore',
    'user':'cleanse_sys',
    'password':'cleanse_sys',
    'charset':'utf8'
}

washmeta ={
    'host':'10.1.2.33',
    'port':3306,
    'db':'cleanse_db',
    'user':'cleanse_sys',
    'password':'cleanse_sys',
    'charset':'utf8'
}

