# encoding: utf-8
# author TurboChang

from faker import Faker
from core.exception.related_exception import GetPlanException
import openpyxl
import os

fake = Faker('zh_CN')

class PtDataGen():

    def __init__(self, num):
        self.num = num
        self.data_list = []
        self.array = []

    def fake_data(self):
        pt_profile = fake.profile()
        col1 = f"{fake.pyint(15000,17000):05}"
        col2 = f"{fake.pyint(200,350):05}"
        col3 = pt_profile['name']
        col4 = pt_profile['sex']
        col5 = f"{fake.pyint(15,50)}"
        col6 = fake.street_address()
        col7 = fake.domain_name()
        return col1, col2, col3, col4, col5, col6, col7

    def get_datas(self):
        lis = []
        for i in range(self.num):
            lis.append(self.fake_data())
        return lis

class GetPlan:
    alias = "EXCEL"

    def __init__(self):
        self.excel_file = "core/conf/TestPlan.xlsx"

    def get_testplan(self, sheet_name=None):
        cls = []
        excel_path = os.path.abspath(os.path.join(self.excel_file))
        wb = openpyxl.load_workbook(excel_path)
        sheet = wb[sheet_name]
        rows = sheet.rows

        for i, row in enumerate(rows):
            if i == 0:
                continue
            columns = [cell.value for cell in row]
            cls.append(columns)
            if row is False:
                raise GetPlanException('sheet页{0}中并无数据'.format(sheet_name))
        return cls




