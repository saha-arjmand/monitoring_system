# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import pandas as pd

############################################################################

class ConvertFile():

    def convert_horizontal_to_vertical_newEDL_recon(self, column_names_list, df):
        # Create empty df for convert horizontal to vertical
        newdf = pd.DataFrame(columns=column_names_list)

        # Feed name
        i = 0
        for anyFeedName in df:

            # feed_name
            feed_name = anyFeedName

            # count
            count = df.iloc[0, i]

            # add feed_name & count to df
            newdf.loc[i] = (feed_name, count)
            i += 1


        return newdf


    def convert_horizontal_to_vertical_newEDW_recon(self, column_names_list, df, original_null_list):

        df = pd.DataFrame(df)
        np_original_null_lis = pd.DataFrame(original_null_list).to_numpy()

        newdf = pd.DataFrame(columns=column_names_list)

        # Feed name
        i = 0
        for anyFeedName in df:

            # feed_name
            feed_name_tmp = anyFeedName
            #  In edw Our feed names are not complete due to Oracle label restrictions
            #  Therefore, it is better to complete the name of the feed

            for anyName in np_original_null_lis:
                if feed_name_tmp.upper() in (anyName[0]).upper():
                    feed_name = anyName[0]

            # count
            count = df.iloc[0, i]

            # add feed_name & count to df
            newdf.loc[i] = (feed_name, count)
            i += 1

        return newdf
