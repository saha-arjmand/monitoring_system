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
from script_modules.subscript.query_creator.querymaker import EDL_QueryMaker



############################################################################

class RECHECK_EDL:

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
    def edl_feeds_list_in_date_format_db_table(self):

        tableObj = TABLES()
        edl_date_format_tbl = tableObj.edl_date_format()

        edl_date_format_np = edl_date_format_tbl.to_numpy()

        edl_date_format_list = []

        for anyItem in edl_date_format_np:
            edl_date_format_list.append(anyItem[1])

        return edl_date_format_list

    
    def feeds_not_in_date_format_db_table(self, recheck_feeds_list, edl_feeds_list_in_date_format_db_table):

        feeds_not_in_date_format_db_table_list = []

       
        for anyItem in recheck_feeds_list:
            if anyItem not in edl_feeds_list_in_date_format_db_table:
                feeds_not_in_date_format_db_table_list.append(anyItem)

        if len(feeds_not_in_date_format_db_table_list) > 0 :
            print("\n  These feeds do not exist in the EDL date format table and their status must be specified .\n")
            print("\n")
            x = 1
            for anyItem in feeds_not_in_date_format_db_table_list:
                print(f"  {x}. {anyItem}")
                x += 1
        else:
            print("\n  The database in EDL_DATE_FORMAT Table is complete.\n")

        return feeds_not_in_date_format_db_table_list


    def complete_db_edl_date_format_hint(self, feeds_not_in_date_format_db_table):
        if len(feeds_not_in_date_format_db_table) > 0 :
            print("\n")
            print("  Hint for inserting data: ")
            for anyItem in feeds_not_in_date_format_db_table:
                #INSERT INTO people (first_name, last_name) VALUES ("John", "Smith");
                print(f'  INSERT INTO EDL_DATE_FORMAT (edl_feed_name, cus_date_name, cus_date_format, type) VALUES ("{anyItem}", "cus_date", "YYYYMMDD", "file");')
            print(f"\n  If the table has a problem, query is like this : ")
            print(f'  INSERT INTO EDL_PROBLEM_FEEDS (edl_feed_name, first_date, last_date, details, status, last_follow_up, action_on_report) VALUES ("{anyItem}", "20220101", "{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}", "details", "inactive", "follow up from x", "0");')
            print(f'\n  Type "commit" when finished')
        else:
            pass


    def complete_db_edl_date_format(self, feeds_not_in_date_format_db_table):
        
        if len(feeds_not_in_date_format_db_table) > 0 :
            sqlModeObj = SQLMode()
            sqlModeObj.sqlMode()
        else:
            pass


    def merge_EdlRecheckFeed_EdlDateFormat(self, edl_recheck_df, edl_date_format):
        mergeObj = MergeDataFrame(edl_recheck_df, edl_date_format, self.dateObj)
        df = mergeObj.merge_unname_columns("EDL_FEED_NAME", "edl_feed_name")

        # remove additional column
        df = df.drop('id', axis=1)
        df = df.drop('edl_feed_name', axis=1)
        df = df.drop('type', axis=1)

        df.columns = ['EDL_FEED_NAME', 'cus_date_name', 'cus_date_format']

        print("  The merging of file sources and edl date format was done.\n")
        return df


    def edl_new_feed_query_information(self):
        line.title_between_line("Query Recheck (EDL)")
        print("  These feeds need to be rechecked on the edl side, get count for them through the following query : \n") 
        print(f"  Place (recheck_edl_file_feeds_{self.dateObj.year_file}{self.dateObj.month_file}{self.dateObj.day_file}.csv) file in the inpout folder\n") 


    def edl_file_count_query(self, edl_recheck_feeds):
        line.title_between_line("EDL RECHECK FILE COUNT QOUERY")
        
        fileEdlQueryObj = EDL_QueryMaker(self.dateObj, edl_recheck_feeds)
        fileEdlQueryObj.file_edl_null_query()

    

    def executive(self):
        line.title_between_line("Recheck EDL DATA")

        # recheck feeds
        recheck_feeds_df = self.recheck_feeds_df()
        recheck_feeds_list = self.recheck_feeds_list(recheck_feeds_df)

        # DB
        edl_feeds_list_in_date_format_db_table = self.edl_feeds_list_in_date_format_db_table
        feeds_not_in_date_format_db_table = self.feeds_not_in_date_format_db_table(recheck_feeds_list, edl_feeds_list_in_date_format_db_table)
        self.complete_db_edl_date_format_hint(feeds_not_in_date_format_db_table)
        self.complete_db_edl_date_format(feeds_not_in_date_format_db_table)
        
        # Complete DB
        len_feeds_not_in_date_format_db_table = len(feeds_not_in_date_format_db_table)
        while  len_feeds_not_in_date_format_db_table > 0:
            line.create_simple_line()
            edl_feeds_list_in_date_format_db_table = self.edl_feeds_list_in_date_format_db_table
            feeds_not_in_date_format_db_table = self.feeds_not_in_date_format_db_table(recheck_feeds_list, edl_feeds_list_in_date_format_db_table)
            self.complete_db_edl_date_format_hint(feeds_not_in_date_format_db_table)
            self.complete_db_edl_date_format(feeds_not_in_date_format_db_table)

            # recheck len of table
            len_feeds_not_in_date_format_db_table = len(feeds_not_in_date_format_db_table)
            if len_feeds_not_in_date_format_db_table == 0:
                break
        
        tableObj = TABLES()
        edl_recheck_feeds = self.merge_EdlRecheckFeed_EdlDateFormat(recheck_feeds_df, tableObj.edl_date_format())
        
        self.edl_new_feed_query_information()

        self.edl_file_count_query(edl_recheck_feeds)

