# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import pandas as pd

############################################################################

class DataCleaner:

    def __init__(self ,dateObj ,df, df_type):
        self.dateObj = dateObj
        self.df = df
        self.df_type = df_type

    def clean_exelsheet(self):

        if self.df_type == "file":
            x = 0
            #  shape[0] get us the len of our df
            while x < self.df.shape[0]:
                #  iloc[row,column]
                self.df.iloc[x,0] = f"{self.dateObj.year_file}{self.dateObj.month_file}{self.dateObj.day_file}"
                self.df.iloc[x, 2] = ''
                self.df.iloc[x, 3] = ''
                self.df.iloc[x, 4] = ''
                self.df.iloc[x, 5] = ''
                x += 1
            return self.df
            
        elif self.df_type == "db":
            x = 0
            #  shape[0] get us the len of our df
            while x < self.df.shape[0]:
                #  iloc[row,column]
                self.df.iloc[x,0] = f"{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}"
                self.df.iloc[x, 3] = ''
                self.df.iloc[x, 4] = ''
                self.df.iloc[x, 5] = ''
                self.df.iloc[x, 6] = ''
                x += 1

            return self.df

    def update_header(self, total_percent = 0):
        if self.df_type == "file":
            title = f'EDL_BIB_FILE_RECON_{self.dateObj.file_date}'
            multi_index_level0 = [title, title, title, title, title, total_percent, " "]
            multi_index_level1 = ['DATE_KEY', 'EDL_FEED_NAME', 'EDL_RECORD_COUNT', 'EDW_RECORD_COUNT', 'DIFF', 'Diff %', 'Comments']
            multi_index = [multi_index_level0, multi_index_level1]
            self.df.columns = pd.MultiIndex.from_arrays(multi_index)
            return self.df
        
        elif self.df_type == 'db':
            title = f'EDL_BIB_DB_RECON_{self.dateObj.date}'
            multi_index_level0 = [title, title, title, title, title, title, total_percent]
            multi_index_level1 = ['DATE_KEY', 'EDL_FEED_NAME', 'BIB_FEED_NAME', 'EDL_RECORD_COUNT', 'BIB_RECORD_COUNT', 'DIFF', 'Diff %']
            multi_index = [multi_index_level0, multi_index_level1]
            self.df.columns = pd.MultiIndex.from_arrays(multi_index)
            return self.df


############################################################################

if __name__ == "__main__":
    pass
    # self.df.columns = pd.MultiIndex.from_arrays(multi_index)



