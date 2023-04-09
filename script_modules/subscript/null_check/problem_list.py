# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

from script_modules.subscript.db.tables.tables import TABLES
from script_modules.subscript.cleanup_file.cleanup import DataCleaner
import pandas as pd


############################################################################

def feeds_list_minus_problem_feeds(feeds_list, edl_or_edw, type):

    tableObj = TABLES()

    if type.lower() == 'file':
        # null feeds list
        edl_null_feeds_list = []
        for anyItem in feeds_list.to_numpy():
            edl_null_feeds_list.append(anyItem[0])

        if edl_or_edw.lower() == "edl":

            # problem feeds list
            edl_problem_feeds_numpy = tableObj.edl_problem_feeds_file.to_numpy()


            # remove problem list from null list
            if len(edl_problem_feeds_numpy) > 0:
                for anyItem in edl_problem_feeds_numpy:
                    if (anyItem[5]).lower() == 'inactive':
                        edl_null_feeds_list.remove(anyItem[1])

                edl_null_feeds = pd.DataFrame(edl_null_feeds_list,columns=['EDL_FEED_NAME'])

                return edl_null_feeds

        

        elif edl_or_edw.lower() == "edw":

            # problem feeds list
            edw_problem_feeds_numpy = tableObj.edw_problem_feeds_file.to_numpy()


            # remove problem list from null list
            if len(edw_problem_feeds_numpy) > 0:
                for anyItem in edw_problem_feeds_numpy:
                    if (anyItem[6]).upper() == 'NO':
                        edl_null_feeds_list.remove(anyItem[1])

                edl_null_feeds = pd.DataFrame(edl_null_feeds_list,columns=['EDL_FEED_NAME'])

                return edl_null_feeds
   
    elif type.lower() == 'db':
        # null feeds list

        edl_null_feeds_list = []
        for anyItem in feeds_list.to_numpy():
            edl_null_feeds_list.append(anyItem[0])


        if edl_or_edw.lower() == "edl":

            # problem feeds list
            edl_problem_feeds_numpy = tableObj.edl_problem_feeds_db.to_numpy()


            # remove problem list from null list
            if len(edl_problem_feeds_numpy) > 0:
                for anyItem in edl_problem_feeds_numpy:
                    if (anyItem[5]).lower() == 'inactive':
                        edl_null_feeds_list.remove(anyItem[1])

            edl_null_feeds = pd.DataFrame(edl_null_feeds_list,columns=['EDL_FEED_NAME'])

            return edl_null_feeds

        

        elif edl_or_edw.lower() == "edw":

            # problem feeds list
            edw_problem_feeds_numpy = tableObj.edw_problem_feeds_db.to_numpy()

            # remove problem list from null list
            if len(edw_problem_feeds_numpy) > 0:
                for anyItem in edw_problem_feeds_numpy:
                    if (anyItem[6]).upper() == 'NO':
                        if anyItem[1] in edl_null_feeds_list:
                            edl_null_feeds_list.remove(anyItem[1])

            edl_null_feeds = pd.DataFrame(edl_null_feeds_list,columns=['EDL_FEED_NAME'])

            return edl_null_feeds


def replacing_problematic_values(df, dateObj, edl_or_edw, level, type):

    tableObj = TABLES()

    # read problem tables
    edl_problem_feeds_numpy = tableObj.edl_problem_feeds().to_numpy()
    edw_problem_feeds_numpy = tableObj.edw_problem_feeds().to_numpy()

    # read file source
    df_np = df.to_numpy()

    if type.lower() == 'file':
        if edl_or_edw.lower() == "edl":
            for anyProblem in edl_problem_feeds_numpy:
                for anyfeeds in df_np:
                    if anyProblem[1] == anyfeeds[1]:
                        # problem solved item
                        # In the first step, we set a series of data equal to 0
                        if level == "0":
                            if anyProblem[7] == '0':
                                anyfeeds[2] = 0

           
        elif edl_or_edw.lower() == "edw":
            for anyProblem in edw_problem_feeds_numpy:
                for anyfeeds in df_np:
                    if anyProblem[1] == anyfeeds[1]:
                        # problem solved item
                        # In the first step, we set a series of data equal to 0
                        if level == "0":
                            if anyProblem[8] == '0':
                                anyfeeds[3] = 0

        # convert np array to pd df
        fileSource = pd.DataFrame(df_np,columns=['DATE_KEY', 'EDL_FEED_NAME', 'EDL_RECORD_COUNT', 'EDW_RECORD_COUNT', 'DIFF', 'Diff %', 'Comments'])
        
        # update index to multi index
        updateObj = DataCleaner(dateObj, fileSource, 'file') 
        updatedf = updateObj.update_header()
        return updatedf


    elif type.lower() == 'db':
        if edl_or_edw.lower() == "edl":
            for anyProblem in edl_problem_feeds_numpy:
                for anyfeeds in df_np:
                    if anyProblem[1] == anyfeeds[1]:
                        # problem solved item
                        # In the first step, we set a series of data equal to 0
                        if level == "0":
                            if anyProblem[7] == '0':
                                anyfeeds[3] = 0
                        else:
                            pass
                    
        elif edl_or_edw.lower() == "edw":
            for anyProblem in edw_problem_feeds_numpy:
                for anyfeeds in df_np:
                    if anyProblem[1] == anyfeeds[1]:
                        # problem solved item
                        # In the first step, we set a series of data equal to 0
                        if level == "0":
                            if anyProblem[8] == '0':
                                anyfeeds[4] = 0
                        else:
                            pass

        # convert np array to pd df
        dbSource = pd.DataFrame(df_np,columns=['DATE_KEY', 'EDL_FEED_NAME', 'BIB_FEED_NAME', 'EDL_RECORD_COUNT', 'BIB_RECORD_COUNT', 'DIFF', 'Diff %'])

        # update index to multi index
        updateObj = DataCleaner(dateObj, dbSource, 'db') 
        updatedf = updateObj.update_header()
        return updatedf


