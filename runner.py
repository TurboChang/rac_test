#!/usr/bin/env python
# encoding: utf-8

import os
from core.logics.db_factory import *
from core.exception.related_exception import MainException
import argparse

USAGE = """
DataPipeline Runner 用法
通过指定以下参数执行自动化增量数据写入
"""
DB_USAGE = """param 1: 执行指定测试数据库. """
TB_USAGE = """param 2: 执行指定的测试表. """
BT_USAGE = """param 3: 执行指定BATCH大小."""
OP_USAGE = """param 4: 执行指定操作(写入/清空数据). """


class TestRunner:

    def __init__(self):
        self.parser = self._prepare_cli()

    def _prepare_cli(self):
        parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument("--ops", choices={"insert", "trunc"}, nargs="?", const="CONST", help=OP_USAGE, type=str)
        parser.add_argument("--db", "-d", default="Oracle", help=DB_USAGE)
        parser.add_argument("--tab", "-t", default="rac_test", help=TB_USAGE)
        parser.add_argument("--batch", "-b", default=2000, help=BT_USAGE)
        return parser

    def _parse_arguments(self):
        args = self.parser.parse_args()
        if args.ops:
            ops = args.ops
            if ops == "insert":
                return create_increment_data(args.db, args.tab, args.batch)
            elif ops == "trunc":
                return truncate_data(args.db, args.tab, args.batch)
            else:
                raise MainException("方法: {0} 执行报错".format(args.db))

    def run(self):
        self._parse_arguments()


if __name__ == '__main__':
    TestRunner().run()
