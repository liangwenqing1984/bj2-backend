import config
from mysql import MySQL
import exception
import const
import collections


# 根据表ID获取库名、表名
def get_database_table(table_id):
    db = MySQL(config.dqc_mysql)
    result = db.execute(const.DB_TABLE, (table_id,))
    if len(result) == 0:
        raise exception.DQCException("database table is non-exist. [table_id:%s]" % table_id)
    del db
    return result[0]["db_phys_nm"], result[0]["data_tbl_phys_nm"]


# 获取表最新数据分区
def get_partition_latest(table_id):
    db = MySQL(config.dqc_mysql)
    result = db.execute(const.MAX_PARTITION_DATE, (table_id,))
    del db
    if result[0]["latest"] is None:
        raise exception.DQCException("table partition is non-exist. [table_id:%s]" % table_id)
    else:
        return result[0]["latest"]


# 根据分区日期获得分区路径
def get_partition_path(table_id, partition_date):
    db = MySQL(config.dqc_mysql)
    result = db.execute(const.PARTITION_PATH, (table_id, partition_date))
    del db
    if len(result) == 0:
        return None
    else:
        return result[0]["dp_path"]


# 获取字段信息,获取主键信息,获取不可为空信息
def get_metadata_field(table_id):
    db = MySQL(config.dqc_mysql)
    result = db.execute(const.TABLE_FIELD, (table_id,))
    del db
    field = []
    datatype = []
    pk = []
    null = []
    for rs in result:
        field.append(rs["fld_phys_nm"])
        datatype.append(rs["fld_data_type"])
        if rs["if_pk"] == 1:
            pk.append(rs["fld_phys_nm"])
        if rs["if_can_null"] == 0:
            null.append(rs["fld_phys_nm"])
    return field, datatype, pk, null


# 获取字段级质量探查规则
def get_field_check(table_id):
    """
    数据结构说明:
    {
        "字段1": [规则1,规则2, ...],
        "字段2": [规则1],
        ...
    }
    """
    db = MySQL(config.dqc_mysql)
    result = db.execute(const.FIELD_CHECK, (table_id,))
    del db
    check = collections.OrderedDict()
    items = []
    for rs in result:
        if check.get(rs["fld_phys_nm"], None) is None:
            items.clear()
        if rs["chk_proj_cd"] in items:
            continue
        else:
            items.append(rs["chk_proj_cd"])
            check[rs["fld_phys_nm"]] = items.copy()
    return check


# 获取数据库信息
def get_metadata(table_id):
    database, table = get_database_table(table_id)
    return database, table


# 获取分区日期
def get_partition_date(table_id, mode, date):
    if mode == 1:
        max_date = get_partition_latest(table_id)
        date = max_date
        partition = get_partition_path(table_id, max_date)
    else:
        partition = get_partition_path(table_id, date)
    return partition, date


# 获取目标数据库
def get_target_database(database):
    db = MySQL(config.dqc_mysql)
    result = db.execute(const.TARGET_DATABASE, (database,))
    if len(result) == 0:
        raise exception.DQCException("target database is non-exist. [database:%s]" % database)
    del db
    return result[0]["db_phys_nm"]


# 合并规则
def merge_check(check, null):
    for item in null:
        if check.get(item, None) is None:
            check[item] = [const.CHECK_NULL]
        else:
            tmp = check.get(item)
            if const.CHECK_NULL in tmp:
                continue
            else:
                tmp.append(const.CHECK_NULL)
                check[item] = tmp.copy()
                tmp.clear()
    return check


# 数据检查项目
def get_check_item():
    db = MySQL(config.dqc_mysql)
    result = db.execute(const.CHECK_ITEM)
    item = {}
    for rs in result:
        item[rs['Chk_Proj_Cd']] = rs['Chk_Projid']
    del db
    return item


# 字段信息
def get_field(table_id):
    db = MySQL(config.dqc_mysql)
    result = db.execute(const.FIELD_TABLE, (table_id,))
    item = {}
    for rs in result:
        item[rs['Fld_Phys_Nm']] = rs['Fldid']
    del db
    return item
