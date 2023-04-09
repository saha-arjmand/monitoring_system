# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import pandas as pd
import script_modules.subscript.line.CreateLine as line
from script_modules.subscript.read_file.read_file import ReadExcel
from script_modules.subscript.read_file.read_file import ReadCSV
from script_modules.subscript.merge_file.mergedf import MergeDataFrame
from script_modules.subscript.null_check.nullcheck import NullCheck
from script_modules.subscript.check_file_exist.CheckFile import CheckFile
from script_modules.subscript.convert_file.convertfile import ConvertFile


############################################################################

class MERGE_RECHECK_EDL_FEEDS:

    def __init__(self, dateObj, db_source) :
        self.dateObj = dateObj
        self.db_source = db_source

    def recheckEDL_isexist(self):
        recheckFileExistObj = CheckFile(self.dateObj)
        path = f".\input\\recheck_edl_db_feeds_{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}.csv"
        recheckFileExistObj.is_file_exist(path)
        return recheckFileExistObj.path

    def read_recheck_edl_files(self, path_edl):

        newEdlObj = ReadCSV(path_edl)
        newEdl = newEdlObj.readCsvSheet()

        print("  The recheck EDL files were read successfully .\n")

        return newEdl

    def convert_recheck_files_to_vertical(self, recheckEdl):

        convertObj = ConvertFile()
        # EDL
        edl_columns= ['EDL_FEED_NAME', 'NEW_EDL_RECORD_COUNT']
        edlfeed = convertObj.convert_horizontal_to_vertical_newEDL_recon(edl_columns, recheckEdl)

        return edlfeed

    def merge_vertical_with_template(self ,newEdl_v):

        # EDL
        mergeEDL = MergeDataFrame(self.db_source, newEdl_v, self.dateObj)
        df = mergeEDL.merge_multiIndex("db")
        df = mergeEDL.replace(df, df.columns[3], df.columns[7])

        print("  The merging of recheck EDL files was done.\n")

        return df

    def executive(self):
            line.title_between_line("MERGE recheck EDL Files Feeds")

            info = "  File Name : "
            info += "\n"
            info += f"  recheck_edl_db_feeds_{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}.csv"
            info += "\n"
            print(info)

            path_edl = self.recheckEDL_isexist()

            print("\n")
            readNewFile = self.read_recheck_edl_files(path_edl)

            readNewFile_to_vertical = self.convert_recheck_files_to_vertical(readNewFile)

            db_source = self.merge_vertical_with_template(readNewFile_to_vertical )

            return db_source



class MERGE_RECHECK_EDW_FEEDS:

    def __init__(self, dateObj, db_source) :
        self.dateObj = dateObj
        self.db_source = db_source

    def recheckEDW_isexist(self):
        recheckFileExistObj = CheckFile(self.dateObj)
        path = f".\input\\recheck_edw_db_feeds_{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}.xlsx"
        recheckFileExistObj.is_file_exist(path)
        return recheckFileExistObj.path

    def read_recheck_edw_files(self, path_edw):

        newEdwObj = ReadExcel(path_edw)
        newEdl = newEdwObj.readExcelSheet()

        print("  The recheck EDW files was read successfully .\n")

        return newEdl
    
    def feed_list(self):

        edl_feed_list = self.db_source.to_numpy()
        feedList = []
        for anyfeed in edl_feed_list:
            feedList.append(anyfeed[1])
        
        feedlist_pd = pd.DataFrame(feedList, columns=['EDL_FEED_NAME'])
        return feedlist_pd

    def convert_recheck_files_to_vertical(self, recheckEdw, edl_feed_list_df):

        convertObj = ConvertFile()
        # EDW
        edw_columns= ['EDL_FEED_NAME', 'NEW_EDW_RECORD_COUNT']
        edwfeed = convertObj.convert_horizontal_to_vertical_newEDW_recon(edw_columns, recheckEdw, edl_feed_list_df)

        return edwfeed

    def merge_vertical_with_template(self ,newEdw_v):

        # EDW
        mergeEDW = MergeDataFrame(self.db_source, newEdw_v, self.dateObj)
        df = mergeEDW.merge_multiIndex("db")
        df = mergeEDW.replace(df, df.columns[4], df.columns[7])

        print("  The merging of recheck EDW files was done.\n")

        return df

    def executive(self):
            line.title_between_line("MERGE recheck EDW DB Feeds")

            info = "  File Name : "
            info += "\n"
            info += f"  recheck_edw_db_feeds_{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}.xlsx"
            info += "\n"
            print(info)

            path_edw = self.recheckEDW_isexist()

            print("\n")
            readNewFile = self.read_recheck_edw_files(path_edw)

            feed_list = self.feed_list()

            readNewFile_to_vertical = self.convert_recheck_files_to_vertical(readNewFile, feed_list)

            db_source = self.merge_vertical_with_template(readNewFile_to_vertical )

            return db_source