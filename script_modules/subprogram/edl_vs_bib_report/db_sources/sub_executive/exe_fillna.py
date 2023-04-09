# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import pandas as pd
from script_modules.subscript.cleanup_file.cleanup import DataCleaner
############################################################################

class FILLNA:

    def __init__(self, df, dateObj, type) :
        self.df = df
        self.dateObj = dateObj
        self.type = type


    def fill_na(self):

        df_np = self.df.to_numpy()

        if self.type == 'db':
            for anyItem in df_np:
                if anyItem[3] == '':
                    anyItem[3] = 0
                elif anyItem[4] == '':
                    anyItem[4] = 0

            df = pd.DataFrame(df_np)

            return df

        elif self.type == 'file':
            for anyItem in df_np:
                if anyItem[2] == '':
                    anyItem[2] = 0
                elif anyItem[3] == '':
                    anyItem[3] = 0

            df = pd.DataFrame(df_np)

            return df



    def convert_to_pd_and_update_header(self, df):

        if self.type == 'db':
            headerObj = DataCleaner(self.dateObj, df, 'db')
            df = headerObj.update_header()
            return df

        elif self.type == 'file':
            headerObj = DataCleaner(self.dateObj, df, 'file')
            df = headerObj.update_header()
            return df

    def executive(self):

        df_tmp = self.fill_na()

        df = self.convert_to_pd_and_update_header(df_tmp)

        return df