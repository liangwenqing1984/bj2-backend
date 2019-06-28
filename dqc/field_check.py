from pyhive import hive
import assemble
import config


# 创建目标表
def create_target_table(target_database, target_table, field, datatype, fld_check):
    sql = assemble.create_table_sql(target_database, target_table, field, datatype, fld_check)
    cursor = hive.connect(host=config.hiveserver2, username=config.hive_user, port=config.hive_port).cursor()
    cursor.execute(assemble.drop_table_sql(target_database, target_table))
    cursor.execute(sql)
    cursor.close()


# 加工基表
def handle_base_table(database, table, partition, target_database, target_table, field, fld_check, sample, limit_size):
    sql = assemble.handle_base_table_sql(database, table, partition, target_database, target_table, field, fld_check,
                                         sample, limit_size)
    cursor = hive.connect(host=config.hiveserver2, username=config.hive_user, port=config.hive_port).cursor()
    cursor.execute(sql)
    cursor.close()


# 统计结果
def stats_field_check(target_database, target_table, fld_check):
    sql, fld_as = assemble.stats_field_check(target_database, target_table, fld_check)
    cursor = hive.connect(host=config.hiveserver2, username=config.hive_user, port=config.hive_port).cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result[0], fld_as
