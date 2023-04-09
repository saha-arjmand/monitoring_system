# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import pandas as pd

############################################################################

class MergeDataFrame():

    def __init__(self, df1, df2, dateObj) :
        self.df1 = df1
        self.df2 = df2
        self.dateObj = dateObj
        # self.column = column

    def merge(self):
        df = self.df1.merge(self.df2, on = 'EDL_FEED_NAME', how="left")
        return df

    def merge_unname_columns(self, left, right):
        df = self.df1.merge(self.df2, left_on=left, right_on=right, how="left")
        return df


    def merge_multiIndex(self, type = 'file'):

        if type.lower() == 'file':
            title = f'EDL_BIB_FILE_RECON_{self.dateObj.file_date}'
            # add a level to 1D df
            self.df2.columns = pd.MultiIndex.from_product([[title], self.df2.columns])

            df = self.df1.merge(self.df2 ,on=[self.df2.columns[0]], how='left')
            return df
            
        elif type.lower() == 'db':
            title = f'EDL_BIB_DB_RECON_{self.dateObj.date}'
            # add a level to 1D df
            self.df2.columns = pd.MultiIndex.from_product([[title], self.df2.columns])

            df = self.df1.merge(self.df2 ,on=[self.df2.columns[0]], how='left')
            return df


    def replace(self, targetDF, column_table1, column_table2):
        df = pd.DataFrame(targetDF)
        df[column_table1] = df.pop(column_table2).fillna(df[column_table1])
        return df

############################################################################

if __name__ == "__main__":
    pass