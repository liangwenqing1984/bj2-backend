#!/bin/env python
# -*- coding: UTF-8 -*-

import time
import datetime

def list_to_str(list):
    strs = ""
    for i in range(len(list)):
        if i + 1 < len(list):
            strs += "\t\t" + list[i] + "," + "\n"
        else:
            strs += "\t\t" + list[i]
    return strs


def list_to_str2(list):
    strs = ""
    for i in range(len(list)):
        strs += "\t\t" + list[i] + "," + "\n"
    return strs



def list_to_str3(list):
    strs = ""
    for i in range(len(list)):
        if i + 1 < len(list):
            strs += list[i] + ","
        else:
            strs += list[i]
    return strs

def list_to_concat(list):
    strs = ""
    for  i in range(len(list)):
        if i + 1 < len(list):
            strs += "coalesce(cast(" + list[i] +" as string),'\\N'),"
        else:
            strs += "coalesce(cast(" + list[i] +" as string),'\\N')"
    return strs

def write_sql_to_file(file,hql):
    try:
        with open(file,'w') as f:
            f.write(hql)
    except Exception as err:
        raise
    finally:
        f.close()


def get_cur_time():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))


def chg_date_format(datestr):
    return datestr.replace("-",'')


def gen_result_str(columns):
    lens = len(columns)+1
    result_str = []
    kuohao_str = []
    for i in range(lens):
        strs = "hive_result["+str(i)+"]"
        kh = "{}"
        result_str.append(strs)
        kuohao_str.append(kh)
    result_str = list_to_str3(result_str)
    kuohao_str = list_to_str3(kuohao_str)
    return result_str,kuohao_str


def list_to_str_msql_crt(list):
    strs = ""
    for i in range(len(list)):
        line_str = list[i]
        name = line_str.split(' ')[0]
        type = line_str.split(' ')[1]
        if i + 1 < len(list):
            strs +=  "`"+name + "`  " + type+","
        else:
            strs +=  "`"+name + "`  " + type
    return strs

# print(chg_date_format("2019-05-01"))
# columns = ['column1', 'column2', 'column3', 'column4']
# print(list_to_concat(columns))
# strs = list_to_str(columns)
# print(strs)
# strs = list_to_str2(columns)
# print(strs)
# strs = list_to_str3(columns)
# print(strs)
# a = {"column1":3,"column2":4,"column3":None}
# del_column(a)