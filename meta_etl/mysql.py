#!/bin/env python
# -*- coding: UTF-8 -*-
import pymysql


class MySQL:
    def __init__(self, db_dict):
        self.dbhost = db_dict.get("host")
        self.port = db_dict.get("port")
        self.dbuser = db_dict.get("user")
        self.dbpwd = db_dict.get("password")
        self.dbname = db_dict.get("db")
        self.dbcharset = db_dict.get("charset")
        self.connection = self.connect()

    def connect(self):
        connection = pymysql.connect(
            host=self.dbhost,
            port=self.port,
            user=self.dbuser,
            password=self.dbpwd,
            db=self.dbname,
            charset=self.dbcharset,
            cursorclass=pymysql.cursors.DictCursor)
        return connection

    def insert(self, table, data, ignore=False):
        with self.connection.cursor() as cursor:
            params = join_field_value(data)
            if ignore:
                sql = "INSERT IGNORE INTO {table} SET {params}".format(table=table, params=params)
            else:
                sql = "INSERT INTO {table} SET {params}".format(table=table, params=params)
            result = cursor.execute(sql, tuple(data.values()))
            self.connection.commit()
            return result

    def execute(self, sql, data={}):
        with self.connection.cursor() as cursor:
            if not sql:
                return
            result = cursor.execute(sql, data)
            self.connection.commit()
            if str(sql).upper().startswith('SELECT'):
                return cursor.fetchall()
            else:
                return result

    def close(self):
        if self.connection:
            return self.connection.close()

    def __del__(self):
        self.close()


def join_field_value(data):
    sql = comma = ''
    for key in data.keys():
        sql += "{}`{}` = %s".format(comma, key)
        comma = ', '
    return sql