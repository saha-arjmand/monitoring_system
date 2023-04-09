# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import pandas as pd
from script_modules.subscript.cleanup_file.cleanup import DataCleaner
from script_modules.subscript.merge_file.mergedf import MergeDataFrame


############################################################################


class MathCalc():

    def __init__(self, df, dateObj, type):
        self.df = df
        self.dateObj = dateObj
        self.type = type

    @property
    def get_df(self):
        return self.df

    @property
    def get_feedlist(self):

        df = pd.DataFrame(self.df)
        # get feedName
        feedName = df.columns[0].to_numpy()

        feedlist = []
        for anyFeedName in feedName:
            feedlist.append(anyFeedName)
        
        return feedlist

    def subtraction(self, value1, value2):

        if self.type.lower() == 'file':
            diff = (self.df.loc[:, pd.IndexSlice[:, value1]] - (self.df.loc[:,pd.IndexSlice[:,value2]]).values).rename(columns={value1 : 'newDiff'})
            diff = pd.concat([self.df, diff], axis=1)

            # merge diff with original df
            mergeObj = MergeDataFrame(self.df, diff, self.dateObj)
            mergeDiff = mergeObj.replace(diff, diff.columns[4], diff.columns[7])

            return mergeDiff

        elif self.type.lower() == 'db':
            diff = (self.df.loc[:, pd.IndexSlice[:, value1]] - self.df.loc[:,pd.IndexSlice[:,value2]].values).rename(columns={value1 : 'newDiff'})
            diff = pd.concat([self.df, diff], axis=1)

            # merge diff with original df
            mergeObj = MergeDataFrame(self.df, diff, self.dateObj)
            mergeDiff = mergeObj.replace(diff, diff.columns[5], diff.columns[7])

            return mergeDiff
 
    def percentage(self):
        
        df_np = self.df.to_numpy()
        
        if self.type.lower() == 'file':
            for anyItem in df_np :
                if int(anyItem[3]) == 0:
                    anyItem[5] = 0.0
                elif int(anyItem[3]) != 0:
                    anyItem[5] = abs(anyItem[4]/anyItem[3])

            df = pd.DataFrame(df_np)

            headerObj = DataCleaner(self.dateObj, df, 'file')
            file_source = headerObj.update_header()

            return file_source
        
        elif self.type.lower() == 'db':
            for anyItem in df_np :
                if int(anyItem[4]) == 0:
                    anyItem[6] = 0.0
                elif int(anyItem[4]) != 0:
                    anyItem[6] = abs(anyItem[5]/anyItem[4])

            df = pd.DataFrame(df_np)

            headerObj = DataCleaner(self.dateObj, df, 'db')
            db_source = headerObj.update_header()

            return db_source

    def total_percentage(self):

        df_np = self.df.to_numpy()

        if self.type.lower() == 'file':

            sum_percent = 0
            for anyItem in df_np :
                sum_percent += float(anyItem[5])

            total_percent = abs(sum_percent/len(df_np) - 1)

            df = pd.DataFrame(df_np)

            headerObj = DataCleaner(self.dateObj, df, 'file')
            file_source = headerObj.update_header(total_percent)

            return file_source

        
        elif self.type.lower() == 'db':

            sum_percent = 0
            for anyItem in df_np :
                sum_percent += float(anyItem[6])

            total_percent = abs(sum_percent/len(df_np) - 1)

            df = pd.DataFrame(df_np)

            headerObj = DataCleaner(self.dateObj, df, 'db')
            db_source = headerObj.update_header(total_percent)

            return db_source