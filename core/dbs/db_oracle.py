# encoding: utf-8
# author TurboChang

import threading
import cx_Oracle
from core.logics.db_driver import *
from core.logics.db_param import PtDataGen as PD


class InsertOracleDB(object):
    ALIAS = "ORACLE"

    def __init__(self, db_config: list, table_name, num):
        """
        :param db_config: db_info
        :param table_name:
        :param num: batch number
        :param thr: threading number
        """
        self.host = db_config[1]
        self.port = db_config[4]
        self.user = db_config[2]
        self.password = db_config[3]
        self.database = db_config[5]
        self.table_name = table_name
        self.insert_sql = "insert into {0} (col1,  col2, col3, col4, col5, col6, col7)"
        self.values_sql = " values (:1, :2, :3, :4, :5, :6, :7)"
        self.pd = PD(num)
        self.datas = self.pd.get_datas()
        self.db = self.__connect()
        # self.thr = thr

    def __del__(self):
        try:
            self.db.close()
        except cx_Oracle.Error as e:
            print(e)

    def __connect(self):
        print("连接Oracle数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}".format(
            self.host, self.port, self.user, self.password, self.database))
        db_dsn = self.host + ":" + str(self.port) + "/" + self.database
        print("db_dsn: {0}".format(db_dsn))
        db = cx_Oracle.SessionPool(self.user, str(self.password), db_dsn, min=5, max=15, increment=1, threaded=True,
                                   encoding="UTF-8", getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT)
        return db

    @db_call
    def __execute(self, sql, para):
        conn = self.db.acquire()
        cursor = conn.cursor()
        print("SQL: {0}".format(sql))
        cursor.executemany(sql, para)
        conn.commit()
        cursor.close()

    def __truncate(self):
        conn = self.db.acquire()
        cursor = conn.cursor()
        sql = "truncate table {0}".format(self.table_name)
        cursor.execute(sql)
        conn.commit()
        cursor.close()

    @db_step("Oracle Insert Batch")
    def insert_table(self):
        print("表名: {0}".format(self.table_name))
        sql = self.insert_sql.format(self.table_name) + self.values_sql
        print("SQL: {0}".format(sql))
        self.__execute(sql, self.datas)

    def threading(self):
        # numberOfThreads = self.thr
        threadArray = []

        for i in range(5):
            thread = threading.Thread(name="#" + str(i), target=self.insert_table)
            print("Thread", threading.current_thread().name)
            threadArray.append(thread)
            thread.start()
        for t in threadArray:
            t.join()
        print("All done!")

    @db_step("删除Oracle表")
    def truncate_table(self):
        print("表名: {0}".format(self.table_name))
        results = self.__truncate()
        return results


class OraCompare(InsertOracleDB):
    ALIAS = "OracleCompare"

    def __del__(self):
        try:
            self.db.close()
        except cx_Oracle.Error as e:
            print(e)

    def __connect(self):
        print("连接Oracle数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}".format(
            self.host, self.port, self.user, self.password, self.database))
        db_dsn = self.host + ":" + str(self.port) + "/" + self.database
        print("db_dsn: {0}".format(db_dsn))
        db = cx_Oracle.SessionPool(self.user, str(self.password), db_dsn, min=5, max=15, increment=1, threaded=True,
                                   encoding="UTF-8", getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT)
        return db

    @db_call
    def __query(self, sql):
        rowLimit = 5000
        conn = self.db.acquire()
        cursor = conn.cursor()
        print("SQL: {0}".format(sql))
        cursor.execute(sql)
        row_result = cursor.fetmany(rowLimit)

        while True:
            if not row_result:
                break

            row_result_list = []

            for i, row in enumerate(row_result):
                sourceKeyString = '';
                sourceRowObject = {}

                for j, sourceRowValue in enumerate(row):
                    sourceRowObject[]

        cursor.close()

