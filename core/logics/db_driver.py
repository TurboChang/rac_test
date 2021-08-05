# encoding: utf-8
# author TurboChang

import datetime

DB_LOGGING_START = '-' * 10 + 'DB-START' + '-' * 10
DB_LOGGING_END = '-' * 10 + 'DB-END' + '-' * 10


def db_call(func):
    """
    数据库调用装饰器，用于打印数据库行为
    """

    def inner_wrapper(*args, **kwargs):
        print(DB_LOGGING_START)
        start_time = datetime.datetime.now()
        has_error = None
        ret = None
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            has_error = e
        stop_time = datetime.datetime.now()
        ms = (stop_time - start_time).microseconds / 1000
        print('Query time: {0}ms'.format(str(ms)))
        print(DB_LOGGING_END)
        if has_error:
            raise has_error
        return ret

    return inner_wrapper


def db_step(step_name):
    """
    数据库操作步骤装饰器，用于打印数据库行为
    """

    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            print('[DB]: ' + step_name)
            return func(*args, **kwargs)

        return inner_wrapper

    return wrapper

def logging():
    pass
