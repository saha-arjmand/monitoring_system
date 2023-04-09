# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import script_modules.subscript.line.CreateLine as line
from script_modules.subscript.merge_file.mergedf import MergeDataFrame
from script_modules.subscript.null_check.nullcheck import NullCheck
from script_modules.subscript.db.tables.tables import TABLES
from script_modules.subscript.null_check.null_status import NULL_STATUS
import script_modules.subscript.null_check.problem_list as problemlist
from script_modules.subscript.db.sql_mode.sql_mode import SQLMode
from script_modules.subscript.query_creator.querymaker import EDW_QueryMaker

############################################################################

class EDW_NULL:

    def __init__(self, df, dateObj) :
        self.df = df
        self.dateObj = dateObj

    def edw_null_feeds_df(self):
        line.title_between_line("Get null feeds from edw")
        edwNullObj = NullCheck(self.df, 'file')
        edwNullDf = edwNullObj.null_edw_check()
        return edwNullDf

    def edw_null_feeds_list(self, edw_null_feeds_df):
        edw_null_feeds_np = edw_null_feeds_df.to_numpy()
        

        edw_null_feeds_list = []

        for anyItem in edw_null_feeds_np :
            edw_null_feeds_list.append(anyItem[0])

        return edw_null_feeds_list

    @property
    def edw_feeds_list_in_date_format_db_table(self):

        tableObj = TABLES()

        edw_date_format_tbl = tableObj.edw_date_format()

        edw_date_format_np = edw_date_format_tbl.to_numpy()

        edw_date_format_list = []

        for anyItem in edw_date_format_np:
            edw_date_format_list.append(anyItem[1])

        return edw_date_format_list

    def feeds_not_in_date_format_db_table(self, edw_null_feeds_list, edw_feeds_list_in_date_format_db_table):

        feeds_not_in_date_format_db_table_list = []

       
        for anyItem in edw_null_feeds_list:
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
            for anyItem in feeds_not_in_date_format_db_table:
                #INSERT INTO people (first_name, last_name) VALUES ("John", "Smith");
                print(f'  INSERT INTO EDW_DATE_FORMAT (edl_feed_name, edw_table_name, partition, partition_format, type) VALUES ("{anyItem}", "STG_GEN.TABLENAME", "YES", "YYYYMMDD", "file");')
            print(f"\n  If the table has a problem, query is like this : ")
            print(f'  INSERT INTO EDW_PROBLEM_FEEDS (edl_feed_name, edw_table_name ,first_date, last_date, details, status, last_follow_up, action_on_report) VALUES ("{anyItem}" ,"STG_GEN.TABLENAME" ,"20220101", "{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}", "details", "inactive", "follow up from x", "0");')
            print(f'\n  Type "commit" when finished')
        else:
            pass
    
    def complete_db_edw_date_format(self, feeds_not_in_date_format_db_table):
        
        if len(feeds_not_in_date_format_db_table) > 0 :
            sqlModeObj = SQLMode()
            sqlModeObj.sqlMode()
        else:
            pass

    def status_null_feeds(self, null_feeds_list):
        nullStatusObj = NULL_STATUS(self.dateObj)
        nullStatusObj.null_status(null_feeds_list, "edw", 'file')

    def remove_problem_feeds_list(self, null_feeds_list):
        list = problemlist.feeds_list_minus_problem_feeds(null_feeds_list, "edw", "file")
        return list

    def merge_EdwNullFeed_EdwDateFormat(self, edw_null_feeds, edw_date_format):
        mergeObj = MergeDataFrame(edw_null_feeds, edw_date_format, self.dateObj)
        df = mergeObj.merge_unname_columns("EDL_FEED_NAME", "edl_feed_name")

        # remove additional column
        df = df.drop('id', axis=1)
        df = df.drop('edl_feed_name', axis=1)
        df = df.drop('type', axis=1)

        print("  The merging of file sources and edl date format was done.\n")

        return df

    def replacing_problematic_values(self):
        replace_df = problemlist.replacing_problematic_values(self.df, self.dateObj, "edw", "0", "file")
        return replace_df

    def edw_new_feed_query_information(self):
        line.title_between_line("Query null feeds (EDW)")
        print("  These feeds don't have count on EDW side, get count for them through the following query : \n") 
        print(f"  Place (new_edw_file_feeds_{self.dateObj.year_file}{self.dateObj.month_file}{self.dateObj.day_file}.csv) file in the inpout folder\n") 

    def edw_file_count_query(self, null_list):
        line.title_between_line("EDW FILE COUNT QOUERY")

        fileEdlQueryObj = EDW_QueryMaker(self.dateObj, null_list)
        fileEdlQueryObj.file_edw_null_query()


    def executive(self):
        edw_null_feeds_df = self.edw_null_feeds_df()
        edw_null_feeds_list = self.edw_null_feeds_list(edw_null_feeds_df)
        edw_feeds_list_in_date_format_db_table = self.edw_feeds_list_in_date_format_db_table
        feeds_not_in_date_format_db_table = self.feeds_not_in_date_format_db_table(edw_null_feeds_list, edw_feeds_list_in_date_format_db_table)
        
        self.complete_db_edw_date_format_hint(feeds_not_in_date_format_db_table)
        self.complete_db_edw_date_format(feeds_not_in_date_format_db_table)

        # Check Complete DB
        len_feeds_not_in_date_format_db_table = len(feeds_not_in_date_format_db_table)
        while  len_feeds_not_in_date_format_db_table > 0:
            line.create_simple_line()
            edw_feeds_list_in_date_format_db_table = self.edw_feeds_list_in_date_format_db_table
            feeds_not_in_date_format_db_table = self.feeds_not_in_date_format_db_table(edw_null_feeds_list, edw_feeds_list_in_date_format_db_table)
            self.complete_db_edw_date_format_hint(feeds_not_in_date_format_db_table)
            self.complete_db_edw_date_format(feeds_not_in_date_format_db_table)

            # recheck len of table
            len_feeds_not_in_date_format_db_table = len(feeds_not_in_date_format_db_table)
            if len_feeds_not_in_date_format_db_table == 0:
                break
        
        self.status_null_feeds(edw_null_feeds_list)

        # replace problem values
        file_source = self.replacing_problematic_values()

        # remove problem feeds from null list
        edw_null_feeds_list = self.remove_problem_feeds_list(edw_null_feeds_df)

        tableObj = TABLES()
        edw_null_feeds = self.merge_EdwNullFeed_EdwDateFormat(edw_null_feeds_list, tableObj.edw_date_format())

        self.edw_new_feed_query_information()

        self.edw_file_count_query(edw_null_feeds)

        return file_source