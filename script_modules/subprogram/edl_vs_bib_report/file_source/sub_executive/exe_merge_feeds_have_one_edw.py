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
from script_modules.subscript.db.tables.tables import TABLES
from script_modules.subscript.cleanup_file.cleanup import DataCleaner

############################################################################

class MERGE_FEEDS_HAVE_ONE_EDW:

    def __init__(self, dateObj, file_source) :
        self.dateObj = dateObj
        self.file_source = file_source

    def read_feeds_have_one_feed_on_edw_tbl(self):
        tblObj = TABLES()
        feeds_have_one_feed_on_edw_df = tblObj.FEEDS_HAVE_ONE_FEED_ON_EDW()

        # change column name
        df_np = feeds_have_one_feed_on_edw_df.to_numpy()
        df = pd.DataFrame(df_np, columns=['id', 'EDL_FEED_NAME', 'edw_feed_name', 'type'])

        return df

    def file_source_to_df(self):

        df_np = self.file_source.to_numpy()

        list = []

        for anyItem in df_np:
            list.append([anyItem[1], anyItem[2], anyItem[3]])


        list_np = np.array(list)

        df = pd.DataFrame(list_np, columns=['EDL_FEED_NAME', 'edl_record_count', 'edw_record_count'])

        return df

    def add_edw_count_np(self, df_fileSource, dftbl):
        mergeObj = MergeDataFrame(df_fileSource, dftbl, self.dateObj)

        df = mergeObj.merge()
        df = df.drop('id', axis=1)
        
        df_np = df.to_numpy()

        list = []
        
        for anyItem in df_np:
            if len(str(anyItem[3])) > 3:
                list.append(anyItem)

        list_np = np.array(list)
        
        return list_np

    def sum_edl_count(self, df):

        for anyItem1 in df:
            for anyItem2 in df:
                # edlname of feed dont equal  and edw name of feed is equal
                if (anyItem1[0] != anyItem2[0]) and (anyItem1[3] == anyItem2[3]) and (anyItem1[1] != anyItem2[1]):

                    if float(anyItem1[1]) == 0.0 or float(anyItem2[1]) == 0.0:
                        newEdlCount = float(anyItem1[1]) + float(anyItem2[1])

                    elif  float(anyItem1[1]) != float(anyItem2[1]):
                        newEdlCount = float(anyItem1[1]) + float(anyItem2[1])

                    
                    anyItem1[1] = float(newEdlCount)
                    anyItem2[1] = float(newEdlCount)

        return df
        
    def update_file_source(self, df):
        
        df_np = self.file_source.to_numpy()
        
        for anyItem1 in df_np:
            for anyItem2 in df:
                if anyItem1[1] == anyItem2[0]:
                    anyItem1[2] = float(anyItem2[1])

        df = pd.DataFrame(df_np)

        return df

    def convert_to_pd_and_update_header(self, df):
        headerObj = DataCleaner(self.dateObj, df, 'file')
        file_source = headerObj.update_header()

        return file_source

    def executive(self):

        line.title_between_line("Merge feeds that have one feed on the edw side")

        # read tbl
        feeds_have_one_feed_on_edw_df = self.read_feeds_have_one_feed_on_edw_tbl()

        # edw_count_df
        edw_count_df = self.file_source_to_df()

        # add edw count to tbl
        df = self.add_edw_count_np(edw_count_df, feeds_have_one_feed_on_edw_df)

        df2 = self.sum_edl_count(df)

        df3 = self.update_file_source(df2)

        file_source = self.convert_to_pd_and_update_header(df3)

        print("  The feeds have been merged.\n")

        return file_source
