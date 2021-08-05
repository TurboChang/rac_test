#!/usr/bin/env python
# encoding: utf-8

from core.exception.related_exception import FactoryException
from core.logics.db_param import GetPlan as Plan
from core.dbs.db_oracle import InsertOracleDB as OraDB
import datetime

def db_cost(start_time):
    """
    记录数据库操作耗时
    """
    stop_time = datetime.datetime.now()
    ms = (stop_time - start_time)
    print(str(ms.total_seconds())+'s')

def create_increment_data(db, tab_name, batch):
    plan = Plan()
    if plan:
        res = plan.get_testplan(db)[0]
        insert_data = OraDB(res, tab_name, round(int(batch)/5))

        if insert_data:
            start_time = datetime.datetime.now()
            insert_data.threading()
            db_cost(start_time)
    else:
        raise FactoryException('该数据库暂暂不支持，数据库：{0}'.format(db))

def truncate_data(db, tab_name, batch):
    plan = Plan()
    if plan:
        res = plan.get_testplan(db)[0]
        insert_data = OraDB(res, tab_name, round(int(batch)/5))

        if insert_data:
            start_time = datetime.datetime.now()
            insert_data.truncate_table()
            db_cost(start_time)
    else:
        raise FactoryException('该数据库暂暂不支持，数据库：{0}'.format(db))

def update_data(db, tab_name, batch):
    plan = Plan()
    if plan:
        res = plan.get_testplan(db)[0]
        insert_data = OraDB(res, tab_name, int(batch))

        if insert_data:
            start_time = datetime.datetime.now()
            insert_data.update_table(start_time)
            db_cost(start_time)
    else:
        raise FactoryException('该数据库暂暂不支持，数据库：{0}'.format(db))
