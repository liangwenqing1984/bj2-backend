#!/bin/bash


CUR_DATE=`date +%Y-%m-%d`
CUR_TIME=`date +%Y-%m-%dT%H:%M:%S`

if [ $# -lt 2 ]; then
    echo "Usage: $0 <job-id> <tenant-id>"
    exit -1
fi

JOB_ID=$1
TENANT_ID=$2

CUR_DIR=$(dirname $(realpath $0))
WORKDIR=$CUR_DIR/work.$TENANT_ID

if [ ! -d $WORKDIR ]; then
	rm -f $WORKDIR
	mkdir $WORKDIR
fi
if [ ! -d $WORKDIR/backup ]; then
	rm -f $WORKDIR/backup
	mkdir $WORKDIR/backup
fi

source $CUR_DIR/set-env.sh

export CLEANSE_METADB_NAME=${CLEANSE_METADB_NAME}_$TENANT_ID
export TMP_DB_NAME=tmpdb_$TENANT_ID

METASTORE_TABLE_FILTER="tbl_name not like 'h_bj18_frk%' and tbl_name not like 'h_bj53_web%' and tbl_name not like 'h_bj18_db%' and tbl_name not like 'h_bj30_stat%'"

echo "Begin executing job $JOB_ID for synchronizing metadata for tenant $TENANT_ID ..."

echo "Backing up cleanse database ..."
date
mysqldump -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS $CLEANSE_DB_NAME > $WORKDIR/backup/${CLEANSE_DB_NAME}.dump.$CUR_TIME

echo "Dumping metadata from metastore source ..."
date
mysqldump -h $METASTORE_DB_HOST -P $METASTORE_DB_PORT -u$METASTORE_DB_USER -p$METASTORE_DB_PASS \
$METASTORE_DB_NAME columns_v2 tbls dbs partitions partition_params sds table_params > $WORKDIR/metastore.sql

cat $CUR_DIR/metastore-sync-1.sql|sed "s/\${METASTORE_DB}/$CLEANSE_METADB_NAME/g;s/\${CLEANSE_DB}/$CLEANSE_DB_NAME/g;s/\${TENANT_ID}/$TENANT_ID/g;s/\${TMP_DB_NAME}/$TMP_DB_NAME/g;s/\${METASTORE_TABLE_FILTER}/$METASTORE_TABLE_FILTER/g" > $WORKDIR/metastore-sync-1.sql.run
cat $CUR_DIR/metastore-sync-2.sql|sed "s/\${METASTORE_DB}/$CLEANSE_METADB_NAME/g;s/\${CLEANSE_DB}/$CLEANSE_DB_NAME/g;s/\${TENANT_ID}/$TENANT_ID/g;s/\${TMP_DB_NAME}/$TMP_DB_NAME/g;s/\${METASTORE_TABLE_FILTER}/$METASTORE_TABLE_FILTER/g" > $WORKDIR/metastore-sync-2.sql.run

echo "Loading metadata to cleanse database ..."
date
mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
use $CLEANSE_METADB_NAME;
source $WORKDIR/metastore.sql;
END

echo "Syncronizing metadata phase 1 ..."
date
mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
source $WORKDIR/metastore-sync-1.sql.run
END

echo "Calculating Table UUID ..."
mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS  -D $TMP_DB_NAME -N -e 'select data_tblid from data_tbl where data_tbl_uuid is null' | $CUR_DIR/gen-uuid-script.sh > $WORKDIR/load-tbl-uuid.sql
mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS  < $WORKDIR/load-tbl-uuid.sql

echo "Syncronizing metadata phase 2 ..."
date
mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
source $WORKDIR/metastore-sync-2.sql.run
END

echo "Remove invalidated data due to metadata synchronization ..."
date
mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
use $CLEANSE_DB_NAME;

delete from data_tbl_wash_proj where data_tblid in (select data_tblid from data_tbl where del_dt = CURRENT_DATE);

delete from data_tbl_expl_proj where data_tblid in (select data_tblid from data_tbl where del_dt = CURRENT_DATE);

delete from data_fld_wash_proj where fldid in (select fldid from data_fld where del_dt = CURRENT_DATE);

delete from data_fld_expl_proj where fldid in (select fldid from data_fld where del_dt = CURRENT_DATE);

delete from prd_data_fld_wash_proj where fldid in (select fldid from data_fld where del_dt = CURRENT_DATE);

update prd_data_proc_job set job_expire_date = CURRENT_DATE where data_tblid in (select data_tblid from data_tbl where del_dt = CURRENT_DATE);

END

echo "Update Automation job status ..."
$CUR_DIR/update-etl-jobs.sh

echo "Updating Metadata-Sync job status ..."
CUR_TIME=`date "+%Y-%m-%d %H:%M:%S"`
mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
use $CLEANSE_DB_NAME;

update data_proc_job set rfrsh_tm = "$CUR_TIME", job_stus='DONE' where jobid = $JOB_ID;

END

mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
use $CLEANSE_DB_NAME;

select count(*) as TABLE_ADDED from data_tbl where create_dt = "$CUR_DATE";
select count(*) as TABLE_UPDATED from data_tbl where upd_dt = "$CUR_DATE" and create_dt <> "$CUR_DATE";
select count(*) as TABLE_DELETED from data_tbl where del_dt = "$CUR_DATE";

select count(*) as COLUMN_ADDED from data_fld where create_dt = "$CUR_DATE";
select count(*) as COLUMN_UPDATED from data_fld where upd_dt = "$CUR_DATE" and create_dt <> "$CUR_DATE";
select count(*) as COLUMN_DELETED from data_fld where del_dt = "$CUR_DATE";

END


echo "Finished."
date

