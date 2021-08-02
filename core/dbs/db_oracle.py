# encoding: utf-8
# author TurboChang

from core.logics.db_driver import *
from core.logics.db_param import PtDataGen as PD
import cx_Oracle
import threading

class InsertOracleDB:
    ALIAS = 'ORACLE'

    def __init__(self, db_config: list, table_name, num, thr):
        self.host = db_config[2]
        self.port = db_config[5]
        self.user = db_config[3]
        self.password = db_config[4]
        self.database = db_config[6]
        self.table_name = table_name
        self.con = self.__connect()
        self.insert_sql = "insert into {0} (col1,  col2, col3, col4, col5, col6, col7)"
        self.values_sql = " values (:1, :2, :3, :4, :5, :6, :7)"
        self.pd = PD(num)
        self.datas = self.pd.get_datas()
        self.thr = thr

    def __del__(self):
        try:
            self.con.close()
        except cx_Oracle.Error as e:
            print(e)

    def __connect(self):
        print("连接Oracle数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}".format(
            self.host, self.port, self.user, self.password, self.database))
        db = cx_Oracle.SessionPool(self.user, self.password, "{0}:{1}/{2}".format(self.host, self.port, self.database),
                                   min=self.thr, max=self.thr+5, increment=1, threaded=True, getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT)
        con = db.acquire()
        return con

    @db_call
    def __execute(self, sql, para):
        cursor = self.con.cursor()
        print("SQL: {0}".format(sql))
        cursor.executemany(sql, para)
        cursor.close()
        self.con.commit()

    def __threading(self):
        numberOfThreads = self.thr
        threadArray = []

        for i in range(numberOfThreads):
            thread = threading.Thread(name="#" + str(i), target=self.insert_table)
            print("Thread", threading.current_thread().name)
            threadArray.append(thread)
            thread.start()
        for t in threadArray:
            t.join()
        print("All done!")

    @db_step("Oracle Insert Batch")
    def insert_table(self):
        print("表名: {0}".format(self.table_name))
        print(self.datas)
        sql = self.insert_sql.format(self.table_name) + self.values_sql
        self.__execute(self.table_name, sql, self.datas)

    @db_step("删除Oracle表")
    def truncate_table(self):
        print("表名: {0}".format(self.table_name))
        results = self.__execute(self.sql("truncate_table").format(self.table_name), False)
        return results
