# encoding: utf-8
# author TurboChang

from core.logics.db_driver import *
from core.logics.db_param import PtDataGen as PD
import cx_Oracle
import threading

class InsertOracleDB:
    ALIAS = 'ORACLE'

    def __init__(self, db_config: list, table_name, num):
        self.host = db_config[2]
        self.port = db_config[5]
        self.user = db_config[3]
        self.password = db_config[4]
        self.database = db_config[6]
        self.table_name = table_name
        self.db = self.__connect()
        self.insert_sql = "insert into {0} (customer_id, first_name, last_name, phone, email, status, birdsday, addr)"
        self.values_sql = " values (:1, :2, :3, :4, :5, :6, :7, :8)"
        self.db_data_path = "../../assets/testData/"
        self.db_data_file = "TestData.csv"
        self.pd = PD(num)
        self.datas = self.pd.get_datas()

    def __del__(self):
        try:
            self.db.close()
        except cx_Oracle.Error as e:
            print(e)

    def __connect(self):
        print('连接Oracle数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}'.format(
            self.host, self.port, self.user, self.password, self.database))
        # db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(
        #     self.user, self.password, self.host, self.port, self.database))
        db = cx_Oracle.SessionPool(self.user, self.password, "{0}:{1}/{2}".format(self.host, self.port, self.database),
                                   min=2, max=5, increment=1, threaded=True, getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT)
        con = db.acquire()
        return con

    @db_call
    def __execute(self, table_name, sql, para):
        cursor = self.con.cursor()
        cursor.execute("truncate table {0}".format(table_name))
        print('SQL: {0}'.format(sql))
        cursor.executemany(sql, para)
        cursor.close()
        self.db.commit()
        print("Thread", threading.current_thread().name)

    def __threading(self):
        numberOfThreads = 2
        threadArray = []
        exec = self.__execute

        for i in range(numberOfThreads):
            thread = threading.Thread(name='#' + str(i), target=exec)
            threadArray.append(thread)
            thread.start()
        for t in threadArray:
            t.join()
        print("All done!")


    @db_step("Oracle Insert Batch")
    def insert(self):
        print('表名: {0}'.format(self.table_name))
        print(self.datas)
        sql = self.insert_sql.format(self.table_name) + self.values_sql
        self.__execute(self.table_name, sql, self.datas)
