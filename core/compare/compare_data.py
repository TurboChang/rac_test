# encoding: utf-8
# author TurboChang

import csv
import datetime
import cx_Oracle
from csv_diff import load_csv, compare
from core.conf.sql_config import *
from core.exception.related_exception import CompareException


class CompareData:

    def __init__(self, tabName):
        self.tab_name = tabName
        self.query = sourceTabMaxDate.format(self.tab_name)
        self.max_date = self.__call_sql(SOURCE_DSN_DICT, self.query)[0][0].strftime("%Y-%m-%d %H:%M:%S")
        self.current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def __connect(self, db_conn):
        # print('连接Oracle数据库: {0}'.format(db_conn))
        db = cx_Oracle.connect(db_conn)
        db.ping()
        cursor = db.cursor()
        return cursor

    def __call_sql(self, db, sql):
        cursor = self.__connect(db)
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

    def data_csv(self, db, csv_name):
        cols_list = []
        csv_file = open(compare_path + csv_name, "w")
        writer = csv.writer(csv_file, delimiter=",", lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)

        if db == SOURCE_DSN_DICT:
            colSql = columnQuery.format(self.tab_name, SOURCE_DATA_BASE_NAME)
            cols_cur = self.__call_sql(SOURCE_DSN_DICT, colSql)
            for i in cols_cur:
                cols_list.append(i[0])
            writer.writerow(cols_list)
        elif db == TARGET_DSN_DICT:
            colSql = columnQuery.format(self.tab_name, TARGET_DATA_BASE_NAME)
            cols_cur = self.__call_sql(TARGET_DSN_DICT, colSql)
            for i in cols_cur:
                cols_list.append(i[0])
            writer.writerow(cols_list)

        dataSql = sourceRowQueryString.format(self.tab_name, self.max_date)
        datas_cur = self.__call_sql(db, dataSql)
        for row in datas_cur:
            writer.writerow(row)
        csv_file.close()

    def _load_csv(self):
        self.data_csv(SOURCE_DSN_DICT, "source.csv")
        self.data_csv(TARGET_DSN_DICT, "target.csv")

    def _write_report(self, file, content, ops):
        fo = open(file, ops)
        fo.write(content)
        fo.close()

    def compare(self):
        source_csv = open(compare_path + "source.csv", "r")
        target_csv = open(compare_path + "target.csv", "r")
        source = load_csv(source_csv, key="ID")
        target = load_csv(target_csv, key="ID")
        if target != {}:
            print("TARGET IS NOT NULL.")
            diff = compare(source, target)
            diff_str = "{'added': [], 'removed': [], 'changed': [], 'columns_added': [], 'columns_removed': []}"
            if str(diff) != diff_str:
                content = "{0}-table \"{1}\" diff is: ".format(self.current_time, self.tab_name) + str(diff) + "\n"
                self._write_report(report_file, content, "a")
        source_csv.close()
        target_csv.close()

    def report(self):
        try:
            print("table: {1} max_date: {0}".format(self.max_date, self.tab_name))
            self._load_csv()
            self.compare()
        except Exception as e:
            raise CompareException('对比报告失败，原因：{0}'.format(e))
