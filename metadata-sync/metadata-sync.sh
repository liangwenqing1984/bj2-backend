#!/bin/bash

METASTORE_DB_HOST=localhost
METASTORE_DB_USER=cleanse_sys
METASTORE_DB_PASS=cleanse_sys
METASTORE_DB_NAME=metastore

CLEANSE_DB_HOST=localhost
CLEANSE_DB_USER=cleanse_sys
CLEANSE_DB_PASS=cleanse_sys
CLEANSE_METADB_NAME=metastore_sync;
CLEANSE_DB_NAME=cleanse_db;

CUR_DATE=`date +%Y-%m-%d`

echo "Begin processing ..."

echo "Dumping metadata from metastore source ..."
mysqldump -h $METASTORE_DB_HOST -u$METASTORE_DB_USER -p$METASTORE_DB_PASS \
$METASTORE_DB_NAME columns_v2 tbls dbs partitions partition_params sds table_params > metastore.sql

cat metastore-sync-1.sql|sed "s/\${METASTORE_DB}/$CLEANSE_METADB_NAME/g;s/\${CLEANSE_DB}/$CLEANSE_DB_NAME/g" > metastore-sync-1.sql.run
cat metastore-sync-2.sql|sed "s/\${METASTORE_DB}/$CLEANSE_METADB_NAME/g;s/\${CLEANSE_DB}/$CLEANSE_DB_NAME/g" > metastore-sync-2.sql.run

echo "Loading metadata to cleanse database ..."
date
mysql -h $CLEANSE_DB_HOST -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
use $CLEANSE_METADB_NAME;
source metastore.sql;
END

echo "Syncronizing metadata phase 1 ..."
date
mysql -h $CLEANSE_DB_HOST -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
source metastore-sync-1.sql.run
END

echo "Calculating Table UUID ..."
mysql -h $CLEANSE_DB_HOST -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS  -D tmpdb -N -e 'select data_tblid from data_tbl where data_tbl_uuid is null' | ./gen-uuid-script.sh > load-tbl-uuid.sql
mysql -h $CLEANSE_DB_HOST -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS  < load-tbl-uuid.sql

echo "Syncronizing metadata phase 2 ..."
date
mysql -h $CLEANSE_DB_HOST -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
source metastore-sync-2.sql.run
END

mysql -h $CLEANSE_DB_HOST -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
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

