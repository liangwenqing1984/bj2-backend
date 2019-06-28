#!/bin/bash

METASTORE_DB_HOST=10.1.1.175
METASTORE_DB_USER=root
METASTORE_DB_PASS=root
METASTORE_DB_NAME=metastore

CLEANSE_DB_HOST=localhost
CLEANSE_DB_USER=cleanse_sys
CLEANSE_DB_PASS=cleanse_sys
CLEANSE_METADB_NAME=metastore;
CLEANSE_DB_NAME=cleanse_db;

CUR_DATE=`date +%Y-%m-%d`

echo "Begin processing ..."

mysqldump -h $METASTORE_DB_HOST -u$METASTORE_DB_USER -p$METASTORE_DB_PASS \
$METASTORE_DB_NAME columns_v2 tbls dbs partitions partition_params sds table_params > metastore.sql

cat metastore-sync.sql|sed "s/\${METASTORE_DB}/$CLEANSE_METADB_NAME/g;s/\${CLEANSE_DB}/$CLEANSE_DB_NAME/g" > metastore-sync.sql.run

echo "Loading metadata ..."
date
mysql -h $CLEANSE_DB_HOST -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
use $CLEANSE_METADB_NAME;
source metastore.sql;
END

echo "Syncronizing metadata ..."
date
mysql -h $CLEANSE_DB_HOST -u$CLEANSE_DB_USER -p$CLEANSE_DB_PASS << END
source metastore-sync.sql.run

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

