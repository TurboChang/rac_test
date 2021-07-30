#!/usr/bin/env python
# encoding: utf-8


class RegressionException(Exception):
    """回归测试级别的异常基类"""

    def __init__(self, msg='', logger=None):
        self.message = msg
        if logger:
            logger.error(msg)

        Exception.__init__(self, msg)

    def __repr__(self):
        return self.message

    __str__ = __repr__