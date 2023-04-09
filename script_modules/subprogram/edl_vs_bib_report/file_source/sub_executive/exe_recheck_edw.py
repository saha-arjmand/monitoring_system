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

    def __init__(self, file_source, dateObj) :
        self.file_source = file_source
        self.dateObj = dateObj

    def recheck_feeds_df(self):
        recheckObj = RECheck(self.file_source, 'file')
        recheck = recheckObj.recheck_list()
        return recheck


    def recheck_feeds_list(self, recheck_feeds_df):
        recheck_feeds_np = recheck_feeds_df.to_numpy()
        
        recheck_feeds_list = []

        for anyItem in recheck_feeds_np :
            recheck_feeds_list.append(anyItem[0])

        return recheck_feeds_list
    
    @property
    def edw_feeds_list_in_date_format_db_table(self):

        tableObj = TABLES()
        edl_date_format_tbl = tableObj.edw_date_format()

        edl_date_format_np = edl_date_format_tbl.to_numpy()

        edl_date_format_list = []

        for anyItem in edl_date_format_np:
            edl_date_format_list.append(anyItem[1])

        return edl_date_format_list

    
    def feeds_not_in_date_format_db_table(self, recheck_feeds_list, edw_feeds_list_in_date_format_db_table):

        feeds_not_in_date_format_db_table_list = []

       
        for anyItem in recheck_feeds_list:
            if anyItem not in edw_feeds_list_in_date_format_db_table:
                feeds_not_in_date_format_db_table_list.append(anyItem)

        if len(feeds_not_in_date_format_db_table_list) > 0 :
            print("\n  These feeds do not exist in the EDW date format table and their status must be specified .\n")
            print("\n")
            x = 1
            for anyItem in feeds_not_in_date_format_db_table_list:
                print(f"  {x}. {anyItem}")
                x += 1
        else:
            print("\n  The database in EDL_DATE_FORMAT Table is complete.\n")

        return feeds_not_in_date_format_db_table_list


    def complete_db_edw_date_format_hint(self, feeds_not_in_date_format_db_table):
        if len(feeds_not_in_date_format_db_table) > 0 :
            print("\n")
            print("  Hint for inserting data: ")
            print("  ------------------------")

            # we need mapping table
            tblObj = TABLES()
            mapping_table = tblObj.FILE_FEEDS_EDL_EDW_DRL_MAPPING()
            mapping_table_np = mapping_table.to_numpy()

            for anyItem in feeds_not_in_date_format_db_table:
                #INSERT INTO people (first_name, last_name) VALUES ("John", "Smith");
                for anyFeed in mapping_table_np:
                    if anyItem == anyFeed[1] :
                        print(f'  INSERT INTO EDW_DATE_FORMAT (edl_feed_name, edw_table_name, partition, partition_format, type) VALUES ("{anyItem}", "{anyFeed[4]}", "YES", "YYYYMMDD", "file");\n')
                
            print(f"\n  If the table has a problem, query is like this : ")
            print("  ------------------------")

            for anyItem2 in feeds_not_in_date_format_db_table:
                for anyFeed2 in mapping_table_np:
                    if anyItem2 == anyFeed2[1]:
                        print(f'  INSERT INTO EDW_PROBLEM_FEEDS (edl_feed_name, edw_table_name ,first_date, last_date, details, status, last_follow_up, action_on_report) VALUES ("{anyItem2}", "{anyFeed2[4]}" , "20220101", "{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}", "details", "inactive", "follow up from x", "0");\n')
            
            print(f'\n  Type "commit" when finished')
        else:
            pass


    def complete_db_edw_date_format(self, feeds_not_in_date_format_db_table):
        
        if len(feeds_not_in_date_format_db_table) > 0 :
            sqlModeObj = SQLMode()
            sqlModeObj.sqlMode()
        else:
            pass


    def merge_EdwRecheckFeed_EdwDateFormat(self, edw_recheck_df, edw_date_format):
        mergeObj = MergeDataFrame(edw_recheck_df, edw_date_format, self.dateObj)
        df = mergeObj.merge_unname_columns("EDL_FEED_NAME", "edl_feed_name")

        # remove additional column
        df = df.drop('id', axis=1)
        df = df.drop('edl_feed_name', axis=1)
        df = df.drop('type', axis=1)

        print("  The merging of file sources and edw date format was done.\n")
        return df


    def edw_new_feed_query_information(self):
        line.title_between_line("Query Recheck (EDW)")
        print("  These feeds need to be rechecked on the edw side, get count for them through the following query : \n") 
        print(f"  Place (recheck_edw_file_feeds_{self.dateObj.year_file}{self.dateObj.month_file}{self.dateObj.day_file}.csv) file in the inpout folder\n") 


    def edw_file_count_query(self, edw_recheck_feeds):
        line.title_between_line("EDW RECHECK FILE COUNT QOUERY")
        
        fileEdwQueryObj = EDW_QueryMaker(self.dateObj, edw_recheck_feeds)
        fileEdwQueryObj.file_edw_null_query()

    

    def executive(self):
        line.title_between_line("Recheck EDW DATA")

        # recheck feeds
        recheck_feeds_df = self.recheck_feeds_df()
        recheck_feeds_list = self.recheck_feeds_list(recheck_feeds_df)

        # DB
        edw_feeds_list_in_date_format_db_table = self.edw_feeds_list_in_date_format_db_table
        feeds_not_in_date_format_db_table = self.feeds_not_in_date_format_db_table(recheck_feeds_list, edw_feeds_list_in_date_format_db_table)
        self.complete_db_edw_date_format_hint(feeds_not_in_date_format_db_table)
        self.complete_db_edw_date_format(feeds_not_in_date_format_db_table)
        
        # Complete DB
        len_feeds_not_in_date_format_db_table = len(feeds_not_in_date_format_db_table)
        while  len_feeds_not_in_date_format_db_table > 0:
            line.create_simple_line()
            edw_feeds_list_in_date_format_db_table = self.edw_feeds_list_in_date_format_db_table
            feeds_not_in_date_format_db_table = self.feeds_not_in_date_format_db_table(recheck_feeds_list, edw_feeds_list_in_date_format_db_table)
            self.complete_db_edw_date_format_hint(feeds_not_in_date_format_db_table)
            self.complete_db_edw_date_format(feeds_not_in_date_format_db_table)

        
            # recheck len of table
            len_feeds_not_in_date_format_db_table = len(feeds_not_in_date_format_db_table)
            if len_feeds_not_in_date_format_db_table == 0:
                break
        
        tableObj = TABLES()
        edw_recheck_feeds = self.merge_EdwRecheckFeed_EdwDateFormat(recheck_feeds_df, tableObj.edw_date_format())
        
        self.edw_new_feed_query_information()

        self.edw_file_count_query(edw_recheck_feeds)

