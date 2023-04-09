# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import script_modules.subscript.line.CreateLine as line
from script_modules.subscript.merge_file.mergedf import MergeDataFrame
from script_modules.subscript.db.tables.tables import TABLES
from script_modules.subscript.db.sql_mode.sql_mode import SQLMode
from script_modules.subscript.recheck.recheck import RECheck
from script_modules.subscript.query_creator.querymaker import EDW_QueryMaker



############################################################################

class RECHECK_EDW:

    def __init__(self, db_source, dateObj) :
        self.db_source = db_source
        self.dateObj = dateObj

    def recheck_feeds_df(self):
        recheckObj = RECheck(self.db_source, 'db')
        recheck = recheckObj.recheck_list()
        return recheck


    def recheck_feeds_list(self, recheck_feeds_df):
        recheck_feeds_np = recheck_feeds_df.to_numpy()
        
        recheck_feeds_list = []

        for anyItem in recheck_feeds_np :
            recheck_feeds_list.append(anyItem[0])

        return recheck_feeds_list
    
    @property
    def edl_feeds_list_in_edl_table_with_edw_task(self):

        tableObj = TABLES()
        edl_table_with_edw_tbl = tableObj.EDL_TABLE_WITH_EDW_TASK()

        edl_table_with_edw_np = edl_table_with_edw_tbl.to_numpy()

        edl_table_with_edw_list = []

        for anyItem in edl_table_with_edw_np:
            edl_table_with_edw_list.append(anyItem[1])

        return edl_table_with_edw_list

    
    def feeds_not_in_edl_table_with_edw_task(self, recheck_feeds_list, edl_feeds_list_in_edl_table_with_edw_task):

        feeds_not_in_edl_table_with_edw_list = []

       
        for anyItem in recheck_feeds_list:
            if anyItem not in edl_feeds_list_in_edl_table_with_edw_task:
                feeds_not_in_edl_table_with_edw_list.append(anyItem)

        if len(feeds_not_in_edl_table_with_edw_list) > 0 :
            print("\n  These feeds do not exist in the EDL_TABLE_WITH_EDW_TASK and their status must be specified .\n")
            print("\n")
            x = 1
            for anyItem in feeds_not_in_edl_table_with_edw_list:
                print(f"  {x}. {anyItem}")
                x += 1
        else:
            print("\n  The database in EDL_DATE_FORMAT Table is complete.\n")

        return feeds_not_in_edl_table_with_edw_list


    def complete_db_edl_table_with_edw_task_hint(self, feeds_not_in_edl_table_with_edw_task_table):
        if len(feeds_not_in_edl_table_with_edw_task_table) > 0 :
            print("\n")
            print("  Hint for inserting data: ")
            print("  ------------------------"),

            # we need mapping table
            for anyItem in feeds_not_in_edl_table_with_edw_task_table:
                print(f'  INSERT INTO EDL_TABLE_WITH_EDW_TASK (edl_feed_name, dw_task_name) VALUES ("{anyItem}", "dw_task_name");\n')


    def complete_db_edl_table_with_edw_task(self, feeds_not_in_edl_table_with_edw_task_table):
        
        if len(feeds_not_in_edl_table_with_edw_task_table) > 0 :
            sqlModeObj = SQLMode()
            sqlModeObj.sqlMode()
        else:
            pass


    def merge_EdwRecheckFeed_EdlEdwTask(self, edw_recheck_df, edl_edw_task):
        mergeObj = MergeDataFrame(edw_recheck_df, edl_edw_task, self.dateObj)
        df = mergeObj.merge_unname_columns("EDL_FEED_NAME", "edl_feed_name")

        # remove additional column
        df = df.drop('id', axis=1)
        df = df.drop('edl_feed_name', axis=1)

        print("  The merging of db sources and edl with edw task was done.\n")
        return df


    def edl_with_edw_task_query_information(self):
        line.title_between_line("Query Recheck (EDW)")
        print("  We use the DW_TASK table for Richak DB.")
        print("  These feeds need to be rechecked on the edw side, get count for them through the following query : \n") 
        print(f"  Place (recheck_edw_db_feeds_{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}.xlsx) file in the inpout folder\n") 


    def edl_with_edw_task_count_query(self, recheck_feeds_df):
        line.title_between_line("EDW RECHECK DB COUNT QOUERY")
        
        fileEdwQueryObj = EDW_QueryMaker(self.dateObj, recheck_feeds_df)
        fileEdwQueryObj.db_edw_recheck_query()

    

    def executive(self):
        line.title_between_line("Recheck EDW DATA")

        # recheck feeds
        recheck_feeds_df = self.recheck_feeds_df()
        recheck_feeds_list = self.recheck_feeds_list(recheck_feeds_df)

        # DB
        edl_feeds_list_in_edl_table_with_edw_task = self.edl_feeds_list_in_edl_table_with_edw_task
        feeds_not_in_edl_table_with_edw_task_table = self.feeds_not_in_edl_table_with_edw_task(recheck_feeds_list, edl_feeds_list_in_edl_table_with_edw_task)
        
        self.complete_db_edl_table_with_edw_task_hint(feeds_not_in_edl_table_with_edw_task_table)
        self.complete_db_edl_table_with_edw_task(feeds_not_in_edl_table_with_edw_task_table)
        
        # Complete DB
        len_feeds_not_in_edl_table_with_edw_task_table = len(feeds_not_in_edl_table_with_edw_task_table)
        while  len_feeds_not_in_edl_table_with_edw_task_table > 0:
            line.create_simple_line()
            edl_feeds_list_in_edl_table_with_edw_task = self.edl_feeds_list_in_edl_table_with_edw_task
            feeds_not_in_edl_table_with_edw_task_table = self.feeds_not_in_edl_table_with_edw_task(recheck_feeds_list, edl_feeds_list_in_edl_table_with_edw_task)
        
            self.complete_db_edl_table_with_edw_task_hint(feeds_not_in_edl_table_with_edw_task_table)
            self.complete_db_edl_table_with_edw_task(feeds_not_in_edl_table_with_edw_task_table)

        
            # recheck len of table
            len_feeds_not_in_edl_table_with_edw_task_table = len(feeds_not_in_edl_table_with_edw_task_table)
            if len_feeds_not_in_edl_table_with_edw_task_table == 0:
                break
        
        tableObj = TABLES()
        edw_recheck_feeds = self.merge_EdwRecheckFeed_EdlEdwTask(recheck_feeds_df, tableObj.EDL_TABLE_WITH_EDW_TASK())
        self.edl_with_edw_task_query_information()

        self.edl_with_edw_task_count_query(edw_recheck_feeds)

