# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import pandas as pd
import numpy as np
import script_modules.subscript.line.CreateLine as line
from script_modules.subscript.read_file.read_file import ReadExcel
from script_modules.subscript.read_file.read_file import ReadCSV
from script_modules.subscript.merge_file.mergedf import MergeDataFrame
from script_modules.subscript.null_check.nullcheck import NullCheck
from script_modules.subscript.check_file_exist.CheckFile import CheckFile
from script_modules.subscript.convert_file.convertfile import ConvertFile


############################################################################

class MERGE_NEW_FEEDS:

    def __init__(self, dateObj, file_source) :
        self.dateObj = dateObj
        self.file_source = file_source

    def newEDL_isexist(self):
        newFileExistObj = CheckFile(self.dateObj)
        path = f".\input\\new_edl_file_feeds_{self.dateObj.year_file}{self.dateObj.month_file}{self.dateObj.day_file}.csv"
        newFileExistObj.is_file_exist(path)
        return newFileExistObj.path

    def newEDW_isexist(self):
        newFileExistObj = CheckFile(self.dateObj)
        path = f".\input\\new_edw_file_feeds_{self.dateObj.year_file}{self.dateObj.month_file}{self.dateObj.day_file}.xlsx"
        newFileExistObj.is_file_exist(path)
        return newFileExistObj.path

    def read_new_edl_edw_files(self, path_edl, path_edw):

        newEdlObj = ReadCSV(path_edl)
        newEdl = newEdlObj.readCsvSheet()

        newEdwObj = ReadExcel(path_edw)
        newEdw = newEdwObj.readExcelSheet()

        print("  The new EDL & EDW files were read successfully .\n")

        return newEdl, newEdw

    def edl_null_feeds_df(self):
        edlNullObj = NullCheck(self.file_source, 'file')
        edlNullDf = edlNullObj.null_edl_check()
        return edlNullDf

    def convert_new_files_to_vertical(self, newEdl, newEdw, null_list_df):
        convertObj = ConvertFile()
        # EDL
        edl_columns= ['EDL_FEED_NAME', 'NEW_EDL_RECORD_COUNT']
        edlfeed = convertObj.convert_horizontal_to_vertical_newEDL_recon(edl_columns, newEdl)

        # EDW
        edw_columns= ['EDL_FEED_NAME', 'NEW_EDW_RECORD_COUNT']
        edwfeed = convertObj.convert_horizontal_to_vertical_newEDW_recon(edw_columns, newEdw, null_list_df)

        return edlfeed, edwfeed

    def merge_vertical_with_template(self ,newEdl_v, newEdw_v):

        # EDL
        mergeEDL = MergeDataFrame(self.file_source, newEdl_v, self.dateObj)
        df = mergeEDL.merge_multiIndex()
        df = mergeEDL.replace(df, df.columns[2], df.columns[7])
        

        # EDW
        mergeEDW = MergeDataFrame(df, newEdw_v, self.dateObj)
        df = mergeEDW.merge_multiIndex()
        df = mergeEDW.replace(df, df.columns[3], df.columns[7])

        print("  The merging of new EDL & EDW files were done.\n")

        return df



    def executive(self):
            line.title_between_line("MERGE new EDL & EDW Files Feeds")

            info = "  File Names : "
            info += "\n"
            info += f"  new_edl_file_feeds_{self.dateObj.year_file}{self.dateObj.month_file}{self.dateObj.day_file}.csv"
            info += "\n"
            info += f"  new_edw_file_feeds_{self.dateObj.year_file}{self.dateObj.month_file}{self.dateObj.day_file}.xlsx"
            info += "\n"
            print(info)

            path_edl = self.newEDL_isexist()
            print("\n")
            path_edw = self.newEDW_isexist()

            print("\n")
            readNewFile = self.read_new_edl_edw_files(path_edl, path_edw)
            newEdl = readNewFile[0]
            newEdw = readNewFile[1]

            null_list = self.edl_null_feeds_df()

            readNewFile_to_vertical = self.convert_new_files_to_vertical(newEdl, newEdw, null_list)
            newEdl_v = readNewFile_to_vertical[0]
            newEdw_v = readNewFile_to_vertical[1]

            file_source = self.merge_vertical_with_template(newEdl_v, newEdw_v)


            return file_source

            