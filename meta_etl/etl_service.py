#!/bin/env python
# -*- coding: UTF-8 -*-
from mysql import MySQL
import config
import const
import logging
import sys
import uuid
import traceback

#对比插入新增表
def insertNewT(db,date):
    tables1 = gethiveT(db) #传库名
    tables2 = getCT(db) #传库名
    conn = MySQL(config.washmeta)
    tables1 = getDBs(tables1) #传库名
    len2 = len(tables2)
    if len2 == 0:
        for table1 in tables1:
            ud = uuid.uuid1()
            table1['Create_Dt'] = date
            table1['Data_Tbl_UUID'] = str(ud)
            logging.debug('table1:'+ table1['Data_Tbl_Phys_Nm'])
            try:
                conn.insert("data_tbl", table1)
            except Exception :
                logging.error('第一次插入表数据失败,插入数据是：'+str(table1))
                print traceback.format_exc()
                sys.exit(1)
    else:
        for table1 in tables1:
            flag = True
            for table2 in tables2:
                if (table1.get('Data_Tbl_Phys_Nm') == table2.get('Data_Tbl_Phys_Nm')):
                    flag = False
                    break
            if flag:
                table1['Create_Dt'] = date
                table1['Data_Tbl_UUID'] = str(uuid.uuid1())
                logging.debug('table1:' + table1['Data_Tbl_Phys_Nm'])
                try:
                    # conn.insert("data_tbl", table1)
                    logging.debug("插入表字段")
                    insertCByT(db,table1)
                except Exception as e:
                    logging.error('插入新增表失败,插入数据是：'+str(table1))
                    print e
                    print traceback.format_exc()
                    sys.exit(1)
    del conn

#对比修改表
def updateT(db,date):
    tables1 = gethiveT(db) #传库名
    tables2 = getCT(db) #传库名
    conn = MySQL(config.washmeta)
    tables1 = getDBs(tables1) #传库名
    len2 = len(tables2)
    if len2 == 0:
        for table1 in tables1:
            table1['Create_Dt'] = date
            table1['Data_Tbl_UUID'] = uuid.uuid1()
            logging.debug('table1:'+ table1['Data_Tbl_Phys_Nm'])
            try:
                conn.insert("data_tbl", table1)
            except Exception:
                logging.error('第一次插入表数据失败，插入数据是：'+str(table1))
                print traceback.format_exc()
                sys.exit(1)
    for table1 in tables1:
        for table2 in tables2:
            if table1.get('Data_Tbl_Phys_Nm') == table2.get('Data_Tbl_Phys_Nm'):
                # print 'hive 表：',table1.get('Data_Tbl_Phys_Nm')
                # print 'clean 表：',table2.get('Data_Tbl_Phys_Nm')
                # if (not compareP(table1, table2) or not compareC(table1, table2)):
                if (compareP(db,table1, table2) or compareC(db,table1, table2)):
                    logging.debug('对比表table1:'+ table1['Data_Tbl_Phys_Nm'])
                    try:
                        conn.execute("update data_tbl set Upd_Dt='{}' where Data_Tbl_Phys_Nm='{}'".format(date, table1.get(
                        'Data_Tbl_Phys_Nm')))
                    except Exception:
                        logging.error('更新表元数据失败，数据为：'+str(table1))
                        print traceback.format_exc()
                        sys.exit(1)
    del conn





#对比删除表
def deleteT(db,date):
    tables1 = gethiveT(db) #传库名
    tables2 = getCT(db) #传库名
    conn = MySQL(config.washmeta)
    tables1 = getDBs(tables1) #传库名
    len2 = len(tables2)
    flag = True
    if len2 == 0:
        for table1 in tables1:
            table1['Create_Dt'] = date
            table1['Data_Tbl_UUID'] = uuid.uuid1()
            logging.debug('table1:'+ table1['Data_Tbl_Phys_Nm'])
            conn.insert("data_tbl", table1)
    for table2 in tables2:
        for table1 in tables1:
            if table2.get('Data_Tbl_Phys_Nm') == table1.get('Data_Tbl_Phys_Nm'):
                flag = False
                break
        if flag:
            logging.debug('table1:'+ table1['Data_Tbl_Phys_Nm'])
            try:
                conn.execute("update data_tbl set Del_Dt='{}' where Data_Tbl_Phys_Nm='{}'".format(date, table2.get(
                        'Data_Tbl_Phys_Nm')))
            except Exception:
                logging.error('删除表元数据失败，数据为：'+str(table2))
                print traceback.format_exc()
                sys.exit(1)
        flag = True
    del conn

#对比同步分区信息
def insertNewP(db,date):
    partitions1 = gethiveP(db)  #传库名
    partitions2 = getCP(db)  #传库名
    conn = MySQL(config.washmeta)
    partitions1 = getTableID(partitions1)
    len2 = len(partitions2)
    if len2 == 0:
        for p1 in partitions1:
            logging.debug('partition:'+str( p1['Data_Tblid'])+':'+p1['Dp_Path'])
            try:
                conn.insert("dp", p1)
            except Exception:
                logging.error('第一次插入分区数据失败，数据为：'+str(p1))
                print traceback.format_exc()
                sys.exit(1)
    else:
        for p1 in partitions1:
            flag = True
            for p2 in partitions2:
                if p1.get('Data_Tblid') == p2.get('Data_Tblid') and p1.get('Dp_Path') == p2.get('Dp_Path'):
                    flag = False
                    break
            if flag:
                logging.debug('插入分区partition:' + str(p1['Data_Tblid']) + ':' + p1['Dp_Path'])
                try:
                    conn.insert("dp", p1)
                except Exception:
                    logging.error('插入分区数据失败，数据为：' + str(p1))
                    print traceback.format_exc()
                    sys.exit(1)
        for p2 in partitions2:
            flag = True
            for p1 in partitions1:
                if p2.get('Data_Tblid') == p1.get('Data_Tblid') and p2.get('Dp_Path') == p1.get('Dp_Path'):
                    flag = False
                    break
            if flag:
                logging.debug('删除分区partition:' + str(p2['Data_Tblid']) + ':' + p2['Dp_Path'])
                try:
                    conn.execute("delete from dp where Data_Tblid='{}' and Dp_Path='{}'".format(p2.get('Data_Tblid'),p2.get('Dp_Path')))
                except Exception:
                    logging.error('删除分区数据失败，数据为：' + str(p1))
                    print traceback.format_exc()
                    sys.exit(1)


    del conn

#对比插入新增字段
def insertNewC(db,date):
    columns1 = gethiveC(db)  #传库名
    columns2 = getCC(db)  #传库名
    conn = MySQL(config.washmeta)
    columns1 = getTableID(columns1)  #传库名
    len2 = len(columns2)
    flag = True
    if len2 == 0:
        for c1 in columns1:
            c1['Create_Dt'] = date
            logging.debug('column:'+str(c1['Data_Tblid'])+':'+c1['Fld_Phys_Nm'])
            try:
                conn.insert("data_fld", c1)
            except Exception:
                logging.error('第一次插入字段数据失败，失败数据为：'+str(c1))
                print traceback.format_exc()
                sys.exit(1)
    else:
        for c1 in columns1:
            flag =True
            for c2 in columns2:
                if c1.get('Data_Tblid') == c2.get('Data_Tblid') and c1.get('Fld_Phys_Nm') == c2.get('Fld_Phys_Nm'):
                    flag = False
                    break
            if flag:
                c1['Create_Dt'] = date
                logging.debug('column:' + str(c1['Data_Tblid']) + ':' + c1['Fld_Phys_Nm'])
                try:
                    conn.insert('data_fld',c1)
                except Exception:
                    logging.error('插入新增字段数据失败，数据为：'+str(c1))
                    print traceback.format_exc()
                    sys.exit(1)
    del conn


#对比更新修改字段
def updateC(db,date):
    columns1 = gethiveC(db) #传库名
    columns2 = getCC(db) #传库名
    conn = MySQL(config.washmeta)
    columns1 = getTableID(columns1)
    len2 = len(columns2)
    flag = True
    if len2 == 0:
        for c1 in columns1:
            c1['Create_Dt'] = date
            logging.debug('column:' + str(c1['Data_Tblid']) + ':' + c1['Fld_Phys_Nm'])
            try:
                conn.insert("data_fld", c1)
            except Exception:
                logging.error('第一次插入字段数据失败，失败数据为：' + str(c1))
                print traceback.format_exc()
                sys.exit(1)
    else:
        for c1 in columns1:
            flag =True
            for c2 in columns2:
                if c1.get('Data_Tblid') == c2.get('Data_Tblid') and c1.get('Fld_Phys_Nm') == c2.get('Fld_Phys_Nm') and c1.get('Fld_Cn_Nm') == c2.get('Fld_Cn_Nm') \
                        and c1.get('Fld_Data_Type') == c2.get('Fld_Data_Type') and c1.get('Fld_Ord') == c2.get('Fld_Ord'):
                    flag = False
                    break
            if flag:
                c1['Upd_Dt'] = date
                logging.debug('column:' + str(c1['Data_Tblid']) + ':' + c1['Fld_Phys_Nm'])
                try:
                    conn.execute("update data_fld set Fld_Cn_Nm='{}',Fld_Data_Type='{}',Fld_Ord='{}',Upd_Dt='{}' where Data_Tblid='{}' and Fld_Phys_Nm='{}'"
                             .format(c1['Fld_Cn_Nm'],c1['Fld_Data_Type'],c1['Fld_Ord'],c1['Upd_Dt'],c1['Data_Tblid'],c1['Fld_Phys_Nm']))
                except Exception:
                    logging.error('插入新增字段数据失败，数据为：' + str(c1))
                    print traceback.format_exc()
                    sys.exit(1)
    del conn

#对比删除字段
def deleteC(db,date):
    columns1 = gethiveC(db) #传库名
    columns2 = getCC(db) #传库名
    conn = MySQL(config.washmeta)
    columns1 = getTableID(columns1) #传库名
    len2 = len(columns2)
    flag = True
    if len2 == 0:
        for c1 in columns1:
            c1['Create_Dt'] = date
            logging.debug('column:' + str(c1['Data_Tblid']) + ':' + c1['Fld_Phys_Nm'])
            try:
                conn.insert("data_fld", c1)
            except Exception:
                logging.error('第一次插入字段数据失败，失败数据为：' + str(c1))
                print traceback.format_exc()
                sys.exit(1)
    else:
        for c2 in columns2:
            flag = True
            for c1 in columns1:
                if c1.get('Data_Tblid') == c2.get('Data_Tblid') and c1.get('Fld_Phys_Nm') == c2.get(
                        'Fld_Phys_Nm'):
                    flag = False
                    break
            if flag:
                c2['Del_Dt'] = date
                logging.debug('column:' + str(c2['Data_Tblid']) + ':' + c2['Fld_Phys_Nm'])
                try:
                    conn.execute(
                    "update   data_fld set Del_Dt='{}' where Data_Tblid='{}' and Fld_Phys_Nm='{}'"
                    .format(c2['Del_Dt'],c2['Data_Tblid'], c2['Fld_Phys_Nm']))
                except Exception:
                    logging.error('删除字段数据失败，失败数据为：'+str(c2))
                    print traceback.format_exc()
                    sys.exit(1)
    del conn

def gethiveT(db):
    try:
        conn = MySQL(config.hivemeta)
        tables = conn.execute(const.getTSql.format(db))
    except Exception:
        logging.error('获取hive 表元数据失败！')
        print traceback.format_exc()
        sys.exit(1)
    del conn
    return tables


def gethiveP(db):
    try:
        conn = MySQL(config.hivemeta)
        partitions = conn.execute(const.getPSql.format(db))
    except Exception:
        logging.error('获取hive 分区元数据失败！')
        print traceback.format_exc()
        sys.exit(1)
    del conn
    return partitions

def gethiveC(db):
    try:
        conn = MySQL(config.hivemeta)
        columns = conn.execute(const.getCSql.format(db))
    except Exception:
        logging.error('获取hive 字段元数据失败')
        print traceback.format_exc()
        sys.exit(1)
    del conn
    return columns

def getCT(db):
    try:
        conn = MySQL(config.washmeta)
        tables = conn.execute(const.getCTSql.format(db))
    except Exception:
        logging.error('获取清洗库 表元数据 失败！')
        print traceback.format_exc()
        sys.exit(1)
    del conn
    return tables

def getCP(db):
    try:
        conn = MySQL(config.washmeta)
        partitions = conn.execute(const.getCPSql.format(db))
    except Exception:
        logging.error('获取清洗库 分区数据 失败！')
        print traceback.format_exc()
        sys.exit(1)
    del conn
    return partitions

def getCC(db):
    try:
        conn = MySQL(config.washmeta)
        columns = conn.execute(const.getCCSql.format(db))
    except Exception:
        logging.error('获取清洗库 字段数据 失败！')
        print traceback.format_exc()
        sys.exit(1)
    del conn
    return columns
def getDBs(tables):
    try:
        conn = MySQL(config.washmeta)
        dbs = conn.execute(const.getDBs)
    except Exception:
        logging.error('获取库ID 失败！')
        print traceback.format_exc()
        sys.exit(1)
    for table in tables:
        for db in dbs:
            if table.get('Dbid') == db.get('Db_Phys_Nm'):
                table['Dbid'] = db['Dbid']
    return tables
def getTableID(tables1):
    try:
        conn = MySQL(config.washmeta)
        tables2 = conn.execute('select Data_Tblid,Data_Tbl_Phys_Nm from data_tbl')
    except Exception:
        logging.error('获取表ID 失败！')
        print traceback.format_exc()
        sys.exit(1)
    for t1 in tables1:
        for t2 in tables2:
            if t1.get('Data_Tblid') == t2.get('Data_Tbl_Phys_Nm'):
                t1['Data_Tblid'] = t2.get('Data_Tblid')
    return tables1
def compareP(db,table1,table2):
    conn1 = MySQL(config.hivemeta)
    conn2 = MySQL(config.washmeta)
    ps1 = conn1.execute(const.getTPSql.format(db,table1.get('Data_Tbl_Phys_Nm')))
    ps2 = conn2.execute(const.getCTPSql.format(db,table2.get('Data_Tbl_Phys_Nm')))
    del conn1
    del conn2
    flag = False
    for p1 in ps1:
        flag = True
        for p2 in ps2:
            if p1 == p2:
                flag = False
                ps2.remove(p2)
                break
        if flag == True:
            break
    return flag

def compareC(db,table1,table2):
    conn1 = MySQL(config.hivemeta)
    conn2 = MySQL(config.washmeta)
    c1 = conn1.execute(const.getTCSql.format(db,table1.get('Data_Tbl_Phys_Nm')))
    c2 = conn2.execute(const.getCTCSql.format(db,table2.get('Data_Tbl_Phys_Nm')))
    del conn1
    del conn2
    c1.sort()
    c2.sort()
    if cmp(c1,c2) != 0:
        # print 'c1:',c1
        # print 'c2:',c2
        return True
    else:
        return False
#新增加的表字段插入
def insertCByT(db,tb):
    conn1 = MySQL(config.hivemeta)
    conn2 = MySQL(config.washmeta)
    hivesql = """select 
'{}' AS Data_Tblid,
t1.COLUMN_NAME as Fld_Phys_Nm,
t1.COMMENT as Fld_Cn_Nm,
t1.TYPE_NAME as Fld_Data_Type,
t1.INTEGER_IDX as Fld_Ord
from columns_v2 t1
left join sds t2
on t1.cd_id = t2.cd_id
left join tbls t3
on t2.sd_id = t3.sd_id
left join dbs t4
on t3.db_id=t4.db_id
where t4.name='{}' and t3.tbl_name='{}'"""
    cs1 = conn1.execute(hivesql.format(tb.get('Data_Tbl_Phys_Nm'),db,tb.get('Data_Tbl_Phys_Nm')))
    cs1 = getTableID(cs1)
    for c in cs1:
        c['Create_Dt'] = tb['Create_Dt']
        try:
            logging.debug("插入新增表字段："+str(c['Fld_Phys_Nm']))
            conn2.insert('data_fld', c)
        except Exception as e:
            logging.error("插入新增表字段失败："+str(c['Fld_Phys_Nm']))
            print traceback.format_exc()
    del conn1
    del conn2


