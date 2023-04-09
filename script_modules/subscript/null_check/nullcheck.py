import numpy as np
import pandas as pd

############################################################################

class NullCheck():

    def __init__(self, df, df_type) :
        self.df = df
        self.df_type = df_type

    def null_edl_check(self):

        if self.df_type == 'file':
            
            edl_feed_list = []
            
            n = 0
            for anyItem in self.df.to_numpy():
                if str(self.df.to_numpy()[n][2]) == '':
                    edl_feed_list.append(self.df.to_numpy()[n][1])
                n += 1

            file_feed_list_df = pd.DataFrame({'EDL_FEED_NAME': edl_feed_list})

            return file_feed_list_df
        
        elif self.df_type =='db':
            #  calculate feeds dont have count in EDL side
            n = 0
            edl_feed_list = []

            for anyItem in self.df.to_numpy():
                if str(self.df.to_numpy()[n][3]) == '':
                    edl_feed_list.append(self.df.to_numpy()[n][1])
                n += 1

            file_feed_list_df = pd.DataFrame({'EDL_FEED_NAME': edl_feed_list})

            return file_feed_list_df

    def null_edw_check(self):

        if self.df_type == 'file':
            
            edw_feed_list = []
            
            n = 0
            for anyItem in self.df.to_numpy():
                if str(self.df.to_numpy()[n][3]) == '':
                    edw_feed_list.append(self.df.to_numpy()[n][1])
                n += 1

            file_feed_list_df = pd.DataFrame({'EDL_FEED_NAME': edw_feed_list})

            return file_feed_list_df
        
        elif self.df_type =='db':
            #  calculate feeds dont have count in EDL side
            n = 0
            edw_feed_list = []

            for anyItem in self.df.to_numpy():
                if str(self.df.to_numpy()[n][4]) == '':
                    edw_feed_list.append(self.df.to_numpy()[n][1])
                n += 1

            file_feed_list_df = pd.DataFrame({'EDL_FEED_NAME': edw_feed_list})

            return file_feed_list_df
############################################################################

if __name__ == "__main__":
    pass