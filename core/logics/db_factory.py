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

def factory():
    plan = Plan()
    if plan:
        res = plan.get_testplan("Oracle")[0]
        insert_data = OraDB(res, "rac_test", 100)

        if insert_data:
            start_time = datetime.datetime.now()
            insert_data.insert_table()
            db_cost(start_time)
    else:
        raise FactoryException('该数据库暂暂不支持，数据库：{0}'.format("Oracle"))

if __name__ == '__main__':
    factory()