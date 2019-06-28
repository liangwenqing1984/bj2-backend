from mysql import MySQL
import config
import const


# 作业失败
def failed_job(job_id):
    """
    作业类型 Job_Type
        0：数据探查作业
        1：数据清洗规则验证作业
    """
    db = MySQL(config.dqc_mysql)
    db.execute(const.FAILED_JOB, (job_id,))
    del db


# 更新总记录条数
def total_record(job_id, total, date):
    db = MySQL(config.dqc_mysql)
    db.execute(const.UPDATE_TOTAL_RECORD, (total, date, date, job_id))
    del db


# 更新作业状态
def table_check_result(job_id, duplicate_record, duplicate_pk, total, date):
    db = MySQL(config.dqc_mysql)
    db.execute(const.TABLE_CHECK_RESULT, (duplicate_record, duplicate_pk, total, date, date, job_id))
    del db


# 同步字段级质量检查结果
def fld_expl_result(job_id, stats_filed_check, check_item, field_item, fld_as):
    db = MySQL(config.dqc_mysql)
    i = 0
    for val in stats_filed_check:
        key = fld_as[i]
        field = key[:key.rindex('_')]
        check = key[key.rindex('_') + 1:]
        fld_id = field_item[field]
        chk_id = check_item[check]
        value = val
        param = {"Jobid": job_id,
                 "Fldid": fld_id,
                 "Chk_Projid": chk_id,
                 "Isu_Rec_Qty": value}
        db.insert(const.FLD_EXPL_RESULT, param)
        param.clear()
        i += 1
    del db
