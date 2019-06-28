from string import Template
import const


# 创建表SQL
def create_table_sql(target_database, target_table, field, datatype, fld_check):
    content = ""
    i = 0
    for item in field:
        if i == 0:
            content += item + " " + datatype[i]
        else:
            content += "," + item + " " + datatype[i]
        i += 1
    for k, v in fld_check.items():
        for check in v:
            fld_as = k + "_" + check
            content += "," + fld_as + " int"
    content += "," + const.ROWID_FIELD + " string"
    content += "," + const.RAND_FIELD + " string"
    args = {"database": target_database,
            "table": target_table,
            "content": content
            }
    template = Template(const.CREATE_SQL)
    return template.substitute(args)


# 中间表
def handle_base_table_sql(database, table, partition, target_database, target_table, field, fld_check, sample,
                          limit_size):
    """
    解析字段级规则:
        字段规则使用UDF函数进行定义.
    """
    fields = ",".join(field)
    if sample == 100:
        limit = ''
    else:
        limit = const.LIMIT_SQL % limit_size
    args = {"database": database,
            "table": table,
            "partition": parse_partition(partition),
            "target_database": target_database,
            "target_table": target_table,
            "field": fields,
            "md5": md5_sql(field),
            "udf": udf_sql(fld_check),
            "limit": limit}
    template = Template(const.BASE_SQL)
    return template.substitute(args)


# rowid计算每行的MD5值
def md5_sql(field):
    exp = [const.NVL_SQL % (f, const.NULL_NVL) for f in field]
    return const.MD5_SQL % ",".join(exp)


# 数据检查
def udf_sql(fld_check):
    udf = ""
    for k, v in fld_check.items():
        for check in v:
            udf += const.UDF_SQL % (k, check) + ","
    return udf


# 统计数据质量
def stats_field_check(target_database, target_table, fld_check):
    fld_as = []
    for k, v in fld_check.items():
        for check in v:
            fld_as.append(k + "_" + check)
    sum_fld = [const.SUM_SQL % (fld, fld) for fld in fld_as]
    args = {"stats": ",".join(sum_fld),
            "target_database": target_database,
            "target_table": target_table}
    template = Template(const.STATS_SQL)
    return template.substitute(args), fld_as


# 解析分区
def parse_partition(partition):
    where = []
    parameters = partition.split('/')
    for parameter in parameters:
        param = parameter.split('=')
        where.append(param[0] + "='" + param[1] + "'")
    return " and ".join(where)


# drop table
def drop_table_sql(target_database, target_table):
    return const.DROP_SQL % (target_database, target_table)


# count
def count_sql(database, table, partition):
    args = {"database": database,
            "table": table,
            "partition": parse_partition(partition)}
    template = Template(const.COUNT_SQL)
    return template.substitute(args)


# 重复记录
def duplicate_record_sql(target_database, target_table):
    template = Template(const.DUPLICATE_RECORD_SQL)
    args = {"target_database": target_database,
            "target_table": target_table}
    return template.substitute(args)


# 重复主键
def duplicate_key_sql(target_database, target_table, pk):
    template = Template(const.DUPLICATE_KEYS_SQL)
    args = {"target_database": target_database,
            "target_table": target_table,
            "pk": ",".join(pk)}
    return template.substitute(args)
