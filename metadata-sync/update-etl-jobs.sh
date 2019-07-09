#!/bin/bash

TENANT_ID=$1
CUR_DIR=$(dirname $(realpath $0))
source $CUR_DIR/set-env.sh

JOB_CONTROLLER=/usr/share/etl/wash/data_wash/bin/publish.sh

mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS  -D $CLEANSE_DB_NAME -N -e ' 
select j.jobid, j.data_tblid, j.job_type 
from prd_data_proc_job j, data_tbl t, db d, data_part p 
where t.dbid = d.dbid
and d.partid = p.partid
and t.data_tblid = j.data_tblid 
and t.del_dt = CURRENT_DATE
and j.job_type in (1,3) 
and j.job_expire_date is null' | while read JOB_ID TBL_ID JOB_TYPE
do
	case $JOB_TYPE in
		1)
			JOB_TYPE_NAME="清洗作业"
			;;
		3)
			JOB_TYPE_NAME="脱敏作业"
			;;
	esac

	echo "Disabling $JOB_TYPE_NAME(id=$JOB_ID) for table(id=$TBL_ID) ..."
	echo "$JOB_CONTROLLER $TBL_ID $JOB_TYPE 0 D 0"
	$JOB_CONTROLLER $TBL_ID $JOB_TYPE 0 D 0
	if [ $? -eq 0 ]; then
		echo "Done"
		mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS  -D $CLEANSE_DB_NAME -e "
			update prd_data_proc_job set job_expire_date = CURRENT_DATE where jobid=$JOB_ID;
		"
	else
		echo "Failed"
	fi
done

echo "ETL Jobs updated."

