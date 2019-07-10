from mysql import MySQL
import config
import const
import mask_const
import datetime


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
        field = key[:key.rindex('_leiyry_')]
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


# 脱敏作业执行状态
def mask_job_status(job_id, status, start_time):
    """
    作业执行状态:
        0 正在执行
        1 执行完成
        2 执行失败
    """
    db = MySQL(config.dqc_mysql)
    db.execute(mask_const.MASK_JOB_STATUS, (status, start_time, job_id))
    del db


# 脱敏作业开始
def start_mask_job(job_id):
    """
    作业执行状态:
        0 正在执行
        1 执行完成
        2 执行失败
    """
    db = MySQL(config.dqc_mysql)
    db.execute(mask_const.MASK_JOB_START, (job_id,))
    del db


# 字段脱敏结果
def fld_mask_result(job_id, rule, field_items, mask_cmpu):
    db = MySQL(config.dqc_mysql)
    for k, v in rule.items():
        param = {"Jobid": job_id,
                 "Job_Exec_Date": datetime.datetime.now().strftime("%Y-%m-%d"),
                 "Fldid": field_items[k],
                 "Data_Wash_Cmpuid": mask_cmpu[v],
                 "Job_Exec_Time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        db.insert(mask_const.MASK_FLD_RESULT, param)
    del db


# 打标签
def mask_label(table_id, mask_lable_id):
    db = MySQL(config.dqc_mysql)
    parm = {"Data_Tblid": table_id,
            "Labelid": mask_lable_id,
            "Mark_Idx_Type": 0}
    db.insert(mask_const.DATA_TBL_MARK_IDX, parm)
    del db
