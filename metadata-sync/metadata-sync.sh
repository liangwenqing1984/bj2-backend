#!/bin/bash


CUR_DATE=`date +%Y-%m-%d`

if [ $# -lt 2 ]; then
    echo "Usage: $0 <job-id> <tenant-id>"
    exit -1
fi

WORKDIR=$(dirname $(realpath $0))
source $WORKDIR/set-env.sh

JOB_ID=$1
TENANT_ID=$2


echo "Begin executing job $JOB_ID for synchronizing metadata for tenant $TENANT_ID ..."

echo "Dumping metadata from metastore source ..."
mysqldump -h $METASTORE_DB_HOST -P $METASTORE_DB_PORT -u$METASTORE_DB_USER -p$METASTORE_DB_PASS \
$METASTORE_DB_NAME columns_v2 tbls dbs partitions partition_params sds table_params > $WORKDIR/metastore.sql

cat $WORKDIR/metastore-sync-1.sql|sed "s/\${METASTORE_DB}/$CLEANSE_METADB_NAME/g;s/\${CLEANSE_DB}/$CLEANSE_DB_NAME/g;s/\${TENANT_ID}/$TENANT_ID/g" > $WORKDIR/metastore-sync-1.sql.run
cat $WORKDIR/metastore-sync-2.sql|sed "s/\${METASTORE_DB}/$CLEANSE_METADB_NAME/g;s/\${CLEANSE_DB}/$CLEANSE_DB_NAME/g;s/\${TENANT_ID}/$TENANT_ID/g" > $WORKDIR/metastore-sync-2.sql.run

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
mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS  -D tmpdb -N -e 'select data_tblid from data_tbl where data_tbl_uuid is null' | $WORKDIR/gen-uuid-script.sh > $WORKDIR/load-tbl-uuid.sql
mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS  < $WORKDIR/load-tbl-uuid.sql

echo "Syncronizing metadata phase 2 ..."
date
mysql -h $CLEANSE_DB_HOST -P $CLEANSE_DB_PORT -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
source $WORKDIR/metastore-sync-2.sql.run
END

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

