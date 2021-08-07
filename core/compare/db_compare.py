# encoding: utf-8
# author TurboChang

import os
import time
import warnings
import zipfile

import cx_Oracle

from core.compare.utilities import compare
from core.conf.sql_config import *

warnings.filterwarnings("ignore")
begin_time = time.time()
key = "compareKey"

matchFile = r"core/report/matchfilename.txt"
unMatchFile = r"core/report/unmatchfilename.txt"
# if os.path.exists(matchFile):
#     os.remove(matchFile)
# if os.path.exists(unMatchFile):
#     os.remove(unMatchFile)

records_processed = 0
row_limit = 50000


class DbCompare:
    ALIAS = "Compare"

    def __init__(self, tabName):
        self.tab_name = tabName
        self.max_date = self._get_max_date()[0].strftime("%Y-%m-%d %H:%M:%S")
        self.file_date = self._get_max_date()[0].strftime("%Y-%m-%d-%H%M%S")

    def __zip_file(self):
        try:
            zip_file = r"core/report/" + str(self.file_date) + "-" + self.tab_name + ".zip"
            if not os.path.exists(zip_file):
                with zipfile.ZipFile(zip_file, mode="w") as f:
                    f.write(matchFile)
                    f.close()
                os.remove(matchFile)
                os.remove(unMatchFile)
        except Exception as e:
            print("异常对象的类型是:%s" % type(e))
            print("异常对象的内容是:%s" % e)

    def __connect(self, db_conn):
        # print('连接Oracle数据库: {0}'.format(db_conn))
        db = cx_Oracle.connect(db_conn)
        db.ping()
        cursor = db.cursor()
        return cursor

    def _get_max_date(self):
        cursor = self.__connect(SOURCE_DSN_DICT)
        query = sourceTabMaxDate.format(self.tab_name)
        cursor.execute(query)
        result = cursor.fetchone()
        return result

    def get_col_list(self):
        cursor = self.__connect(SOURCE_DSN_DICT)
        cursor.execute(columnQuery, (self.tab_name, SOURCE_DATA_BASE_NAME))
        column_list = []

        while True:
            column_result = cursor.fetchall()
            if not column_result:
                break
            for _, column in enumerate(column_result):
                column_list.append(column[0])
        return column_list

    def get_key_list(self):
        cursor = self.__connect(SOURCE_DSN_DICT)
        key_result = cursor.execute(keyQuery, (self.tab_name, SOURCE_DATA_BASE_NAME))
        key_list = []

        for _, key_row in enumerate(key_result):
            key_list.append(key_row[0])
        return key_list

    def get_source_data(self):
        source_row_object_list = []
        cursor = self.__connect(SOURCE_DSN_DICT)
        sql = sourceRowQueryString.format(self.tab_name, self.max_date)
        # print(type(self.max_date[0]))
        # print("======SQL is: {0}======\n".format(sql))
        cursor.execute(sql)
        col_list = self.get_col_list()
        key_list = self.get_key_list()

        while True:
            source_row_result = cursor.fetchmany(row_limit)
            # print(source_row_result)
            if not source_row_result:
                break

            for i, sourceRow in enumerate(source_row_result):
                sourceKeyString = ""
                sourceRowObject = {}
                for j, sourceRowValue in enumerate(sourceRow):
                    sourceRowObject[col_list[j]] = sourceRowValue
                    if col_list[j] in key_list:
                        sourceKeyString = sourceKeyString + str(sourceRowValue)
                sourceRowObject["compareKey"] = sourceKeyString
                source_row_object_list.append(sourceRowObject)
        return source_row_object_list

    def get_target_data(self):
        target_row_object_list = []
        cursor = self.__connect(TARGET_DSN_DICT)
        sql = sourceRowQueryString.format(self.tab_name, self.max_date)
        # print("======SQL is: {0}======\n".format(sql))
        cursor.execute(sql)
        col_list = self.get_col_list()
        key_list = self.get_key_list()

        while True:
            target_row_result = cursor.fetchmany(row_limit)
            # print(target_row_result)
            if not target_row_result:
                break

            for m, targetRow in enumerate(target_row_result):
                targetKeyString = ""
                targetRowObject = {}
                for n, targetRowValue in enumerate(targetRow):
                    targetRowObject[col_list[n]] = targetRowValue
                    if col_list[n] in key_list:
                        targetKeyString = targetKeyString + str(targetRowValue)
                targetRowObject["compareKey"] = targetKeyString
                target_row_object_list.append(targetRowObject)
        # print(target_row_object_list)
        return target_row_object_list

    def db_row_compare(self):
        begin_time = time.time()
        source = self.get_source_data()
        target = self.get_target_data()
        tabName = SOURCE_DATA_BASE_NAME + "." + self.tab_name
        compare(source, target, matchFile, unMatchFile, key, tabName)
        self.__zip_file()
        recordsProcessed = records_processed + len(target)
        print(str(recordsProcessed) + " rows compared")
        print("The table " + self.tab_name + " batch compare time for " + str(len(target)) + " rows, cost: " + str(
            time.time() - begin_time))
