import logging
import sys
import traceback
import mask_const
import job
import datetime
import mask

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
            arg_tableid = sys.argv[2]
            arg_date = int(datetime.datetime.strptime(sys.argv[3], '%Y-%m-%d').strftime('%Y%m%d'))
        else:
            logging.error("parameter error.")
            sys.exit(-1)
    except Exception:
        logging.error("parameter error.")
        warning_msg = traceback.format_exc()
        logging.error(warning_msg)
        job.mask_job_status(arg_job_id, mask_const.JOB_FAILED, start_time)
        sys.exit(-1)
    return arg_job_id, arg_tableid, arg_date


if __name__ == '__main__':
    """
    作业参数：作业ID、历史表ID、日期(YYYY-MM-DD)
    """
    starttime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    main_job_id = None
    try:
        main_job_id, main_tableid, main_date = parse_argv(starttime)
        logging.info("parameter [%s] [%s] [%s]" % (main_job_id, main_tableid, main_date))
        mask.process(main_job_id, main_date, main_date, starttime)
    except Exception:
        warning_message = traceback.format_exc()
        logging.error(warning_message)
        job.mask_job_status(main_job_id, mask_const.JOB_FAILED, starttime)
        sys.exit(-1)
