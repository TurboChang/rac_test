#!/usr/bin/env python
# encoding: utf-8
# author TurboChang

class RegressionException(Exception):
    """异常基类"""

    def __init__(self, msg='', logger=None):
        self.message = msg
        if logger:
            logger.error(msg)

        Exception.__init__(self, msg)

    def __repr__(self):
        return self.message

    __str__ = __repr__

class FactoryException(RegressionException):
    """FACTORY路径为找到"""
    pass

class GetPlanException(RegressionException):
    """FACTORY路径为找到"""
    pass

class MainException(RegressionException):
    """FACTORY路径为找到"""
    pass

class CompareException(RegressionException):
    """FACTORY路径为找到"""
    pass

class EmailException(RegressionException):
    """FACTORY路径为找到"""
    pass