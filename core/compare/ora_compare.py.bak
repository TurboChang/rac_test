# encoding: utf-8
# author TurboChang

import os
import time
import warnings

import cx_Oracle

from core.compare.utilities import compare

warnings.filterwarnings("ignore")
beginTime = time.time()

matchFile = r"matchfilename.txt"
unMatchFile = r"unmatchfilename.txt"
if os.path.exists(matchFile):
    os.remove(matchFile)
if os.path.exists(unMatchFile):
    os.remove(unMatchFile)

try:
    key = "compareKey"
    recordsProcessed = 0
    rowLimit = 500

    SOURCE_DSN_DICT = "dp_test/123456@39.105.17.117:1521/orcl"
    SOURCE_DATA_BASE_NAME = "dp_test"
    SOURCE_TABLE_NAME = "t1"

    # TARGET_DSN_DICT = "39.105.17.117/1521@dp_sink:123456/orcl"
    TARGET_DSN_DICT = "dp_sink/123456@39.105.17.117:1521/orcl"
    TARGET_DATA_BASE_NAME = "dp_sink"
    TARGET_TABLE_NAME = "t1"

    columnConnection = cx_Oracle.connect(SOURCE_DSN_DICT)
    columnCursor = columnConnection.cursor()
    columnQuery = """SELECT COLUMN_NAME 
    FROM ALL_TAB_COLUMNS 
    WHERE TABLE_NAME = UPPER('{0}')
    AND OWNER = UPPER('{1}')
    ORDER BY COLUMN_ID"""

    colSql = columnQuery.format(SOURCE_TABLE_NAME, SOURCE_DATA_BASE_NAME)
    columnCursor.execute(colSql)
    columnList = []
    while True:
        columnResult = columnCursor.fetchall()
        if not columnResult:
            break
        for index, column in enumerate(columnResult):
            columnList.append(column[0])

    columnCursor.close()

    keyConnection = cx_Oracle.connect(SOURCE_DSN_DICT)
    keyCursor = keyConnection.cursor()
    keyQuery = """SELECT COLUMN_NAME
FROM USER_CONSTRAINTS TC
         INNER JOIN USER_CONS_COLUMNS KU
                    ON TC.CONSTRAINT_TYPE = 'P'
                        AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
                        AND KU.TABLE_NAME = UPPER(:1)
                        AND KU.OWNER = UPPER(:2)"""

    keySql = keyQuery.format(SOURCE_TABLE_NAME, SOURCE_DATA_BASE_NAME)
    # keyQuery = keyCursor.execute(keySql)
    keyResult = keyCursor.execute(keySql, (SOURCE_TABLE_NAME, SOURCE_DATA_BASE_NAME))
    keyList = []

    for keyIndex, keyRow in enumerate(keyResult):
        keyList.append(keyRow[0])

    keyCursor.close()

    sourceRowQueryString = "SELECT * FROM " + SOURCE_DATA_BASE_NAME + "." + SOURCE_TABLE_NAME
    sourceRowConnection = cx_Oracle.connect(SOURCE_DSN_DICT)
    sourceRowCursor = sourceRowConnection.cursor()
    sourceRowCursor.execute(sourceRowQueryString)

    targetRowQueryString = "SELECT * FROM " + TARGET_DATA_BASE_NAME + "." + TARGET_TABLE_NAME
    targetRowConnection = cx_Oracle.connect(TARGET_DSN_DICT)
    targetRowCursor = targetRowConnection.cursor()
    targetRowCursor.execute(sourceRowQueryString)

    while True:
        batchTime = time.time()
        sourceRowResult = sourceRowCursor.fetchmany(rowLimit)
        print(sourceRowResult)
        if not sourceRowResult:
            break
        targetRowResult = targetRowCursor.fetchmany(rowLimit)
        print(targetRowResult)
        if not targetRowResult:
            break

        sourceRowObjectList = []
        for i, sourceRow in enumerate(sourceRowResult):
            sourceKeyString = ""
            sourceRowObject = {}
            for j, sourceRowValue in enumerate(sourceRow):
                sourceRowObject[columnList[j]] = sourceRowValue
                if columnList[j] in keyList:
                    sourceKeyString = sourceKeyString + str(sourceRowValue)
            sourceRowObject["compareKey"] = sourceKeyString
            sourceRowObjectList.append(sourceRowObject)

        targetRowObjectList = []
        for m, targetRow in enumerate(targetRowResult):
            targetKeyString = ""
            targetRowObject = {}
            for n, targetRowValue in enumerate(targetRow):
                targetRowObject[columnList[n]] = targetRowValue
                if columnList[n] in keyList:
                    targetKeyString = targetKeyString + str(targetRowValue)
            targetRowObject["compareKey"] = targetKeyString
            targetRowObjectList.append(targetRowObject)

        compare(sourceRowObjectList, targetRowObjectList, matchFile, unMatchFile, key)
        recordsProcessed = recordsProcessed + len(targetRowObjectList)
        print(str(recordsProcessed) + " rows compared")
        print("Batch compare time for " + str(len(targetRowObjectList)) + " rows: " + str(time.time() - batchTime))

    sourceRowCursor.close()
    targetRowCursor.close()
    print("Total Time:" + str(time.time() - beginTime))

except Exception as e:
    print(e)
