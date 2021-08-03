#!/usr/bin/env python
# encoding: utf-8

from core.exception.related_exception import FactoryException
import datetime

def db_cost(start_time):
    """
    记录数据库操作耗时
    """
    stop_time = datetime.datetime.now()
    ms = (stop_time - start_time)
    print(str(ms.total_seconds())+'s')

def factory():
    pass

    raise FactoryException('该数据库暂暂不支持，数据库：{0}'.format(""))