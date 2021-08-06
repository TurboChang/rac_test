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
        self.num = num
        self.pd = PD(self.num)
        self.datas = self.pd.get_datas()
        self.db = self.__connect()
        # self.thr = thr

    def __del__(self):
        try:
            self.db.close()
        except cx_Oracle.Error as e:
            print(e)

    def __connect(self):
        # print("连接Oracle数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}".format(
        #     self.host, self.port, self.user, self.password, self.database))
        db_dsn = self.host + ":" + str(self.port) + "/" + self.database
        # print("db_dsn: {0}".format(db_dsn))
        db = cx_Oracle.SessionPool(self.user, str(self.password), db_dsn, min=5, max=15, increment=1, threaded=True,
                                   encoding="UTF-8", getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT)
        return db

    @db_call
    def __executemany(self, sql, para):
        conn = self.db.acquire()
        cursor = conn.cursor()
        # print("SQL: {0}".format(sql))
        cursor.executemany(sql, para)
        conn.commit()
        cursor.close()

    @db_call
    def __execute(self, sql):
        conn = self.db.acquire()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()

    @db_call
    def __fetchall(self, sql):
        res_list = []
        conn = self.db.acquire()
        cursor = conn.cursor()
        cursor.execute(sql)
        while True:
            column_result = cursor.fetchall()
            if not column_result:
                break
            for _, column in enumerate(column_result):
                res_list.append(column[0])
        return res_list

    @db_step("Oracle Insert Batch")
    def insert_table(self):
        # print("表名: {0}".format(self.table_name))
        sql = self.insert_sql.format(self.table_name) + self.values_sql
        # print("SQL: {0}".format(sql))
        self.__executemany(sql, self.datas)

    def threading(self):
        # numberOfThreads = self.thr
        threadArray = []

        for i in range(5):
            thread = threading.Thread(name="#" + str(i), target=self.insert_table)
            # print("Thread", threading.current_thread().name)
            threadArray.append(thread)
            thread.start()
        for t in threadArray:
            t.join()
        print("All done!")

    @db_step("删除Oracle表")
    def truncate_table(self):
        sql = "truncate table {0}".format(self.table_name)
        # print("表名: {0}".format(self.table_name))
        results = self.__execute(sql)
        return results

    @db_step("更新Oracle表")
    def update_table(self, now):
        query = "select id from " \
                "(select id, rownum as rn from {0} order by rn desc) " \
                "where rownum <= {1}".format(self.table_name, str(self.num))
        res_id = self.__fetchall(query)
        update = "update {0} set col1 = '{1}', col2 = '{2}', col3 = '{3}', col4 = '{4}', " \
                 "col5 = '{5}', col6 = '{6}', col7 = '{7}', col_date = sysdate where id = {8}"

        for id in res_id:
            print(id)
            sql = update.format(self.table_name,
                                self.datas[0][0],
                                self.datas[0][1],
                                self.datas[0][2],
                                self.datas[0][3],
                                self.datas[0][4],
                                self.datas[0][5],
                                str(now),
                                id)
            self.__execute(sql)
