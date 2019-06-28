#!/bin/env python
# -*- coding: UTF-8 -*-
import logging
import sys

#向日志和标准输出写日志
def get_logger(logfile):
    fh = logging.FileHandler(logfile,encoding='utf-8',mode='w')
    sh     = logging.StreamHandler()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fm = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    logger.addHandler(fh)
    logger.addHandler(sh)
    fh.setFormatter(fm)
    sh.setFormatter(fm)
    return logger

def funa(logger):
    try:
        logger.info("funca--------------------")
        # a= 1/0
        a = 0/1
        if a == 0:
            raise Exception
    except Exception as err:
        logger.error("funca 错误：%s"  %err)
        raise err
        exit(1)
    return a
def funb(a):
    b = a + 1
    logger.info("funcb--------------------")
    return b
def func(b):
    c = b+1
    logger.info("funcc--------------------")
    return c

logger = get_logger("D:\\a.xt")
try:
    a = funa(logger)
    b = funb(a)
    c = func(b)
except Exception as err:
    # logger.exception(sys.exc_info())
    pass
