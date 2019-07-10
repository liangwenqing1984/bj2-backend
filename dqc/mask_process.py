import config
import exception
from mysql import MySQL
import mask_const
import metadata
from string import Template
from pyhive import hive
import datetime
import const


# 查询脱敏作业类型
def mask_job_type(jobid):
    """
    作业类型 Job_Type
          2 立即执行数据脱敏
          3 周期执行数据脱敏
    """
    db = MySQL(config.dqc_mysql)
    result = db.execute(mask_const.JOB_TYPE, (jobid,))
    del db
    if len(result) == 0:
        raise exception.MaskException("jobid is non-exist. [jobid:%s]" % jobid)
    job_type = result[0]["job_type"]
    if job_type != mask_const.MASK_IMM_MODE and job_type != mask_const.MASK_FREQ_MODE:
        raise exception.MaskException("job type is invalid. [jobid:%s]" % jobid)
    return job_type


# 获取脱敏作业表ID
def mask_table_id(jobid):
    db = MySQL(config.dqc_mysql)
    result = db.execute(mask_const.MASK_TABLE_ID, (jobid,))
    if len(result) == 0:
        raise exception.MaskException("jobid is non-exist. [jobid:%s]" % jobid)
    return result[0]["data_tblid"]


# 获取脱敏作业元数据
def get_mask_metadata(table_id):
    origin_database, table = metadata.get_database_table(table_id)
    database = metadata.get_target_database(origin_database, mask_const.CLS_DATABASE_USAGE)
    target_database = metadata.get_target_database(database, mask_const.MASK_DATABASE_USAGE)
    field, datatype, _, _ = metadata.get_metadata_field(table_id)
    return database, table, target_database, field, datatype


# 数据脱敏
def mask_process(rule, start, end, database, table, target_database, target_table, field, partition_key, job_type):
    sql = mask_sql(database, table, target_database, target_table, field, rule, start, end, partition_key, job_type)
    cursor = hive.connect(host=config.hiveserver2, username=config.hive_user, port=config.hive_port).cursor()
    cursor.execute(sql)
    cursor.close()


# 脱敏SQL
def mask_sql(database, table, target_database, target_table, field, rule, start, end, partition_key, job_type):
    template = Template(mask_const.MASK_PROCESS)
    filters = ''
    if job_type == mask_const.MASK_IMM_MODE:
        filters = mask_const.FILTER_BETWEEN % (partition_key,
                                               datetime.datetime.strptime(str(start), "%Y%m%d").strftime("%Y-%m-%d"),
                                               partition_key,
                                               datetime.datetime.strptime(str(end), "%Y%m%d").strftime("%Y-%m-%d"))
    elif job_type == mask_const.MASK_FREQ_MODE:
        filters = mask_const.FILTER_SINGLE % (partition_key,
                                              datetime.datetime.strptime(str(start), "%Y%m%d").strftime("%Y-%m-%d"))
    content = ""
    i = 0
    for fld in field:
        if rule.get(fld, None) is not None:
            fld = mask_const.MASK_UDF % (fld, rule.get(fld))
        if i == 0:
            content += fld
        else:
            content += "," + fld
        i += 1
    content += "," + partition_key
    args = {"database": database,
            "table": table,
            "target_database": target_database,
            "target_table": target_table,
            "filter": filters,
            "field": content,
            "partition": partition_key
            }
    return template.substitute(args)


# 创建脱敏表
def create_mask_table(target_database, target_table, field, datatype, partition_key, table_comment, field_comment):
    content = ""
    i = 0
    """
    野蛮粗暴用string类型，避免数据类型冲突问题。
    (例如：bigint类型的身份证号，脱敏之后存储到bigint字段，会造成查询只有null）
    """
    for item in field:
        if i == 0:
            content += item + " string COMMENT '" + field_comment[i] + "'"
        else:
            content += "," + item + " string COMMENT '" + field_comment[i] + "'"
        i += 1
    partition = partition_key + " string"
    args = {"database": target_database,
            "table": target_table,
            "content": content,
            "partition": partition,
            "comment": table_comment
            }
    template = Template(mask_const.CREATE_MASK_TABLE)
    cursor = hive.connect(host=config.hiveserver2, username=config.hive_user, port=config.hive_port).cursor()
    cursor.execute(template.substitute(args))
    cursor.close()


# 表结构调整处理
def get_change_handle(table_id, table, target_database):
    if metadata.get_change_ddl(table_id):
        cursor = hive.connect(host=config.hiveserver2, username=config.hive_user, port=config.hive_port).cursor()
        cursor.execute(const.DROP_SQL % (target_database, table))
        cursor.close()


# 获取标签id
def get_label_id(table_id):
    db = MySQL(config.dqc_mysql)
    result = db.execute(mask_const.MASK_LABLE, (table_id,))
    del db
    if len(result) == 0:
        raise exception.MaskException("table lable is non-exist. [table_id:%s]" % table_id)
    return result[0]["Labelid"]
