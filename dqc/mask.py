import traceback
import job
import sys
import logging
import mask_process
import mask_const
import metadata
import datetime

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')


def parse_argv(start_time):
    try:
        arg_job_id = int(sys.argv[1])
    except Exception:
        logging.error('job id invalid.')
        sys.exit(-1)

    try:
        if len(sys.argv) == 4:
            arg_start = int(sys.argv[2])
            arg_end = int(sys.argv[3])
        else:
            logging.error("parameter error.")
            sys.exit(-1)
    except Exception:
        logging.error("parameter error.")
        warning_msg = traceback.format_exc()
        logging.error(warning_msg)
        job.mask_job_status(arg_job_id, mask_const.JOB_FAILED, start_time)
        sys.exit(-1)
    return arg_job_id, arg_start, arg_end


def process(jobid, start, end, start_time):
    """
    作业类型 Job_Type
          2 立即执行数据脱敏
          3 周期执行数据脱敏
    """
    job.start_mask_job(jobid)
    job_type = mask_process.mask_job_type(jobid)
    table_id = mask_process.mask_table_id(jobid)
    database, table, target_database, field, datatype = mask_process.get_mask_metadata(table_id)
    partition_key = mask_const.CLS_PARTITION_KEY
    field_items = metadata.get_field(table_id)
    mask_cmpu = metadata.get_mask_cmpu()
    table_comment, field_comment = metadata.get_comment(table_id)
    mask_lable_id = mask_process.get_label_id(table_id)
    mask_process.get_change_handle(table_id, table, target_database)
    mask_process.create_mask_table(target_database, table, field, datatype, partition_key, table_comment, field_comment)
    rule = []
    if job_type == mask_const.MASK_IMM_MODE:
        rule = metadata.immediate_mask_rule(table_id)
    elif job_type == mask_const.MASK_FREQ_MODE:
        rule = metadata.frequent_mask_rule(table_id)
    if len(rule) == 0:
        logging.error("mask rule is empty.")
        job.mask_job_status(jobid, mask_const.JOB_FAILED, start_time)
        sys.exit(-1)
    mask_process.mask_process(rule, start, end, database, table, target_database, table, field, partition_key, job_type)
    job.fld_mask_result(jobid, rule, field_items, mask_cmpu)
    job.mask_job_status(jobid, mask_const.JOB_DONE, start_time)
    job.mask_label(table_id, mask_lable_id)


if __name__ == '__main__':
    """
    输入参数: 作业ID、开始日期、结束日期
    
    mock: 
        作业ID  65
        开始日期 20190601
        结束日期 20190630
    """
    starttime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    main_job_id = None
    try:
        main_job_id, main_start, main_end = parse_argv(starttime)
        logging.info("parameter [%s] [%s] [%s]" % (main_job_id, main_start, main_end))
        process(main_job_id, main_start, main_end, starttime)
    except Exception:
        warning_message = traceback.format_exc()
        logging.error(warning_message)
        job.mask_job_status(main_job_id, mask_const.JOB_FAILED, starttime)
        sys.exit(-1)
