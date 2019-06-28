from pyhive import hive
import config
import assemble


# 表级默认规则
def table_default_rule(target_database, target_table, pk):
    """
    解析表级规则:
        表级规则使用SQL语句模板进行定义.
    规则说明:
        1)默认判断表分区是否为空.
        2)默认判断表分区记录是否重复.
    """
    duplicate_record = table_duplicate_record(target_database, target_table)
    if len(pk) == 0:
        duplicate_pk = duplicate_record
    else:
        duplicate_pk = table_duplicate_pk(target_database, target_table, pk)
    return duplicate_record, duplicate_pk


# 主键重复
def table_duplicate_pk(target_database, target_table, pk):
    cursor = hive.connect(host=config.hiveserver2, username=config.hive_user, port=config.hive_port).cursor()
    cursor.execute(assemble.duplicate_key_sql(target_database, target_table, pk))
    result = cursor.fetchall()
    cursor.close()
    return result[0][0]


# 记录重复
def table_duplicate_record(target_database, target_table):
    cursor = hive.connect(host=config.hiveserver2, username=config.hive_user, port=config.hive_port).cursor()
    cursor.execute(assemble.duplicate_record_sql(target_database, target_table))
    result = cursor.fetchall()
    cursor.close()
    return result[0][0]


# 统计记录数
def total_record(database, table, partition):
    cursor = hive.connect(host=config.hiveserver2, username=config.hive_user, port=config.hive_port).cursor()
    cursor.execute(assemble.count_sql(database, table, partition))
    result = cursor.fetchall()
    cursor.close()
    return result[0][0]
