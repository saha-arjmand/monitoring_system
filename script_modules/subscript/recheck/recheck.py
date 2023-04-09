import numpy as np
import pandas as pd

############################################################################

class RECheck():

    def __init__(self, df, df_type) :
        self.df = df
        self.df_type = df_type

    def recheck_list(self):

        if self.df_type == 'file':
            
            edl_feed_list = []
            
            n = 0
            for anyItem in self.df.to_numpy():
                if float(self.df.to_numpy()[n][5]) > 0.0000000000:
                    edl_feed_list.append(self.df.to_numpy()[n][1])
                n += 1

            file_feed_list_df = pd.DataFrame({'EDL_FEED_NAME': edl_feed_list})

            return file_feed_list_df
        
        elif self.df_type =='db':
            #  calculate feeds dont have count in EDL side
            n = 0
            edl_feed_list = []

            for anyItem in self.df.to_numpy():
                if float(self.df.to_numpy()[n][6]) > 0.01000000000:
                    edl_feed_list.append(self.df.to_numpy()[n][1])
                n += 1

            file_feed_list_df = pd.DataFrame({'EDL_FEED_NAME': edl_feed_list})

            return file_feed_list_df

  
############################################################################

if __name__ == "__main__":
    pass