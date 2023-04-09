# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import pandas as pd
import numpy as np

############################################################################

class ReadExcel():

    def __init__(self, path) :
        self.path = path

    def readExcelSheet(self, sheet_name=None, skiprows=None, header=None):

        # 1
        if sheet_name != None and skiprows != None and header != None:
            sheet = pd.read_excel(self.path, sheet_name=sheet_name, header=header, skipfooter=skiprows)
            return sheet
        # 2
        elif sheet_name == None and skiprows == None and header == None:
            sheet = pd.read_excel(self.path)
            return sheet
        # 3
        elif sheet_name != None and skiprows != None and header == None:
            sheet = pd.read_excel(self.path, sheet_name=sheet_name, skipfooter=skiprows)
            return sheet
        # 4
        elif sheet_name != None and skiprows == None and header != None:
            sheet = pd.read_excel(self.path, sheet_name=sheet_name, header=header)
            return sheet
        # 5
        elif sheet_name == None and skiprows != None and header != None:
            sheet = pd.read_excel(self.path, header=header, skipfooter=skiprows)
            return sheet
        # 6
        elif sheet_name == None and skiprows == None and header != None:
            sheet = pd.read_excel(self.path, header=header)
            return sheet
        # 7
        elif sheet_name == None and skiprows != None and header == None:
            sheet = pd.read_excel(self.path, skipfooter=skiprows)
            return sheet
        # 8
        elif sheet_name != None and skiprows == None and header == None:
            sheet = pd.read_excel(self.path, header=header, skipfooter=skiprows)
            return sheet


    def sheet_name_data_array(self):
        np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)

        # Create empty numpy 2D array for [sheet_title, data_title]
        fnlist = np.empty(shape=[0, 2])

        ls1 = self.excelSheet
        ls2 = []

        for anyItem in self.readExcel():
            ls2.append(anyItem)

        i = 0
        for anything in ls1:
            fnlist = np.append(fnlist, [[ls1[i], ls2[i]]], axis=0)
            i += 1

        return fnlist



class ReadCSV():

    def __init__(self, path) :
        self.path = path

    def readCsvSheet(self, use_columns=None):
        if use_columns != None:
            sheetName = pd.read_csv(self.path, usecols= use_columns)
            return sheetName
        else:
            sheetName = pd.read_csv(self.path)
            return sheetName


############################################################################

if __name__ == "__main__":

    print("EXCEL")
    path = ".\input\EDL_vs_BIB_Reconciliation.xlsx"
    readExcelObj = ReadExcel(path)
    # skiprows & header must be lists
    file_sources = readExcelObj.readExcelSheet("File Sources", None, [0,1])
    print(file_sources)
    print(file_sources.columns)

    print("\n\nCSV")
    path = ".\input\TZ_VW_EDL_EDW_FILE_RECON20221026.csv"
    readCsvObj = ReadCSV(path)
    print(readCsvObj.readCsvSheet(['EDL_FEED_NAME', 'EDL_COUNT', 'EDW_COUNT']))