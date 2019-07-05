#!/bin/bash

echo "use ${TMP_DB_NAME};"
echo "drop table if exists data_tbl_uuid;"
echo "create table data_tbl_uuid(data_tblid int, data_tbl_uuid varchar(60), primary key(data_tblid));"

first_record=1
while read tblid
do
	if [ $first_record -eq 1 ]; then
		echo -n "insert into data_tbl_uuid values "
		first_record=0
	else
		echo -n ","
	fi
	echo -n "($tblid, '"
	echo -n `cat /proc/sys/kernel/random/uuid`
	echo -n "')"
done
echo ";"

