import logging
import traceback
import sys
import metadata
import field_check
import job
import table_check

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')


def process(job_id, table_id, mode, sample, date):
    # 获取元数据
    database, table = metadata.get_metadata(table_id)
    partition, partition_date = metadata.get_partition_date(table_id, mode, date)
    if partition is None:
        job.total_record(job_id, 0, partition_date)
        sys.exit(0)
    field, data_type, pk, null = metadata.get_metadata_field(table_id)
    check = metadata.get_field_check(table_id)
    fld_check = metadata.merge_check(check, null)
    check_item = metadata.get_check_item()
    field_item = metadata.get_field(table_id)
    target_database = metadata.get_target_database(database)
    target_table = table + '_' + str(job_id)
    # 统计总记录数
    total = table_check.total_record(database, table, partition)
    if total == 0:
        job.total_record(job_id, total, partition_date)
        sys.exit(0)
    limit_size = int(total * sample / 100)
    # 字段级检查
    field_check.create_target_table(target_database, target_table, field, data_type, fld_check)
    field_check.handle_base_table(database, table, partition, target_database, target_table, field, fld_check, sample, limit_size)
    if len(fld_check) > 0:
        stats_filed_check, fld_as = field_check.stats_field_check(target_database, target_table, fld_check)
        job.fld_expl_result(job_id, stats_filed_check, check_item, field_item, fld_as)
    # 表级检查
    duplicate_record, duplicate_pk = table_check.table_default_rule(target_database, target_table, pk)
    job.table_check_result(job_id, duplicate_record, duplicate_pk, limit_size, partition_date)


def parse_argv():
    try:
        arg_job_id = int(sys.argv[1])
    except Exception:
        logging.error('job id invalid.')
        sys.exit(-1)

    try:
        if len(sys.argv) == 6:
            arg_table_id = int(sys.argv[2])
            arg_mode = int(sys.argv[3])
            arg_sample = int(sys.argv[4])
            arg_date = int(sys.argv[5])
        elif len(sys.argv) == 5:
            arg_table_id = int(sys.argv[2])
            arg_mode = int(sys.argv[3])
            arg_sample = int(sys.argv[4])
            arg_date = None
        else:
            logging.error("parameter error.")
            sys.exit(-1)
    except Exception:
        logging.error("parameter error.")
        warning_msg = traceback.format_exc()
        logging.error(warning_msg)
        job.failed_job(arg_job_id)
        sys.exit(-1)
    return arg_job_id, arg_table_id, arg_mode, arg_sample, arg_date


if __name__ == '__main__':
    """
    输入参数: 作业ID、表ID、模式、分区范围
        1批量探查
        0指定范围探查
    """
    """
    mock: 
        作业ID  123
        表ID  1
        模式  0 
        抽样百分比  50  代表50%抽样  全表100
        指定日期(可选)  20190101
    """
    main_job_id = None
    try:
        main_job_id, main_table_id, main_mode, main_sample, main_date = parse_argv()
        logging.info(
            "parameter [%s] [%s] [%s] [%s] [%s]" % (main_job_id, main_table_id, main_mode, main_sample, main_date))
        process(main_job_id, main_table_id, main_mode, main_sample, main_date)
    except Exception:
        warning_message = traceback.format_exc()
        logging.error(warning_message)
        job.failed_job(main_job_id)
        sys.exit(-1)
