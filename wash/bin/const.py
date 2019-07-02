#!/bin/env python
# -*- coding: UTF-8 -*-

Formats = "ROW FORMAT DELIMITED \n" +\
          "FIELDS TERMINATED BY '\\001'\n" +\
          "LINES TERMINATED BY '\\n' \n" +\
          "STORED AS TEXTFILE;\n\n\n\n\n\n"
PartitionbyStr = "partitioned by (data_dt string)\n"
PartitionbyStrIsu = "partitioned by (data_dt string,isu_type string)\n"
PartitionbyStrIsuDev = "partitioned by (isu_type string)\n"
DropStr = "drop table if exists "
CreateStr = "create table "
CreateIfStr = "create table if not exists "
CrtDevDir = "../sql/create/dev/"
CrtProDir = "../sql/create/pro/"
RowidStr = "\t\trowidlwq string,\n"
TagsStr = "\t\ttagslwq string )\n"

InsDevDir = "../sql/insert/dev/"
InsProDir = "../sql/insert/pro/"
AsTagsStr = "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tas tags\n"

InsOvrStr = "insert overwrite table "
InsPartDtStr = " partition(data_dt='"
