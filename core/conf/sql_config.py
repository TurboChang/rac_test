# encoding: utf-8
# author TurboChang

# Oracle
SOURCE_DSN_DICT = "dp_test/123456@39.105.17.117:1521/orcl"
SOURCE_DATA_BASE_NAME = "dp_test"
SOURCE_TABLE_NAME = ["t1", "t2"]

TARGET_DSN_DICT = "dp_sink/123456@39.105.17.117:1521/orcl"
TARGET_DATA_BASE_NAME = "dp_sink"
# TARGET_TABLE_NAME = "t2"

columnQuery = """SELECT COLUMN_NAME 
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = UPPER('{0}')
    AND OWNER = UPPER('{1}')
    ORDER BY COLUMN_ID"""

keyQuery = """SELECT COLUMN_NAME
FROM ALL_CONSTRAINTS TC
         INNER JOIN ALL_CONS_COLUMNS KU
                    ON TC.CONSTRAINT_TYPE = 'P'
                        AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
                        AND KU.TABLE_NAME = UPPER(:1)
                        AND KU.OWNER = UPPER(:2)"""

sourceTabMaxDate = "select max(col_date) from " + SOURCE_DATA_BASE_NAME + ".{0}"
sourceRowQueryString = "select * from " + "{0} where col_date >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss')"
# targetRowQueryString = "SELECT * FROM " + TARGET_DATA_BASE_NAME + "." + TARGET_TABLE_NAME



