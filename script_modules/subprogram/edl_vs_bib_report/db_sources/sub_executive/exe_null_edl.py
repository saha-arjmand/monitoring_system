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
from script_modules.subscript.query_creator.querymaker import EDL_QueryMaker

############################################################################

class EDL_NULL:

    def __init__(self, df, dateObj) :
        self.df = df
        self.dateObj = dateObj

    def edl_null_feeds_df(self):
        line.title_between_line("Get null feeds from edl")
        edlNullObj = NullCheck(self.df, 'db')
        edlNullDf = edlNullObj.null_edl_check()
        return edlNullDf

    def edl_null_feeds_list(self, edl_null_feeds_df):
        edl_null_feeds_np = edl_null_feeds_df.to_numpy()
        

        edl_null_feeds_list = []

        for anyItem in edl_null_feeds_np :
            edl_null_feeds_list.append(anyItem[0])

        return edl_null_feeds_list

    @property
    def edl_feeds_list_in_date_format_db_table(self):
        tableObj = TABLES()
        edl_date_format_tbl = tableObj.edl_date_format()

        edl_date_format_np = edl_date_format_tbl.to_numpy()

        edl_date_format_list = []

        for anyItem in edl_date_format_np:
            edl_date_format_list.append(anyItem[1])

        return edl_date_format_list

    def feeds_not_in_date_format_db_table(self, edl_null_feeds_list, edl_feeds_list_in_date_format_db_table):

        feeds_not_in_date_format_db_table_list = []

       
        for anyItem in edl_null_feeds_list:
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
                print(f'  INSERT INTO EDL_DATE_FORMAT (edl_feed_name, cus_date_name, cus_date_format, type) VALUES ("{anyItem}", "cus_date", "YYYYMMDD", "db");')
            print(f"\n  If the table has a problem, query is like this : ")
            print(f'  INSERT INTO EDL_PROBLEM_FEEDS (edl_feed_name, first_date, last_date, details, status, last_follow_up, action_on_report, type) VALUES ("{anyItem}", "20220101", "{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}", "details", "inactive", "follow up from x", "0", "file");')
            print(f'\n  Type "commit" when finished')
        else:
            pass
    
    def complete_db_edl_date_format(self, feeds_not_in_date_format_db_table):
        
        if len(feeds_not_in_date_format_db_table) > 0 :
            sqlModeObj = SQLMode()
            sqlModeObj.sqlMode()
        else:
            pass

    def status_null_feeds(self, null_feeds_list):
        nullStatusObj = NULL_STATUS(self.dateObj)
        nullStatusObj.null_status(null_feeds_list, "edl", 'db')

    def remove_problem_feeds_list(self, null_feeds_list):
        list = problemlist.feeds_list_minus_problem_feeds(null_feeds_list, "edl", "db")

        # add additional column for merge with date format
        list["cus_date_name"] = ""
        list["cus_date_format"] = ""
        return list

    def merge_EdlNullFeed_EdlDateFormat(self, edl_null_feeds, edl_date_format):
        mergeObj = MergeDataFrame(edl_null_feeds, edl_date_format, self.dateObj)
        df = mergeObj.merge_unname_columns("EDL_FEED_NAME", "edl_feed_name")

        # remove additional column
        df = df.drop('id', axis=1)
        df = df.drop('edl_feed_name', axis=1)
        df = df.drop('type', axis=1)

        df = mergeObj.replace(df, "cus_date_name", "cus_date")
        df = mergeObj.replace(df, "cus_date_format_x", "cus_date_format_y")

        df.columns = ['EDL_FEED_NAME', 'cus_date_name', 'cus_date_format']

        print("  The merging of db sources and edl date format was done.\n")
        return df

    def replacing_problematic_values(self):
        replace_df = problemlist.replacing_problematic_values(self.df, self.dateObj, "edl", "0", "db")
        return replace_df

    def edl_new_feed_query_information(self):
        line.title_between_line("Query null feeds (EDL)")
        print("  These feeds don't have count on EDL side, get count for them through the following query : \n") 
        print(f"  Place (new_edl_db_feeds_{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}.csv) file in the inpout folder\n") 

    def edl_db_count_query(self, null_list):
        line.title_between_line("EDL DB COUNT QOUERY")
        
        dbEdlQueryObj = EDL_QueryMaker(self.dateObj, null_list)
        dbEdlQueryObj.db_edl_null_query()


    def executive(self):
        edl_null_feeds_df = self.edl_null_feeds_df()
        edl_null_feeds_list = self.edl_null_feeds_list(edl_null_feeds_df)
        edl_feeds_list_in_date_format_db_table = self.edl_feeds_list_in_date_format_db_table
        feeds_not_in_date_format_db_table = self.feeds_not_in_date_format_db_table(edl_null_feeds_list, edl_feeds_list_in_date_format_db_table)
        self.complete_db_edl_date_format_hint(feeds_not_in_date_format_db_table)
        self.complete_db_edl_date_format(feeds_not_in_date_format_db_table)

        # Check Complete DB
        len_feeds_not_in_date_format_db_table = len(feeds_not_in_date_format_db_table)
        while  len_feeds_not_in_date_format_db_table > 0:
            line.create_simple_line()
            edl_feeds_list_in_date_format_db_table = self.edl_feeds_list_in_date_format_db_table
            feeds_not_in_date_format_db_table = self.feeds_not_in_date_format_db_table(edl_null_feeds_list, edl_feeds_list_in_date_format_db_table)
            self.complete_db_edl_date_format_hint(feeds_not_in_date_format_db_table)
            self.complete_db_edl_date_format(feeds_not_in_date_format_db_table)

            # recheck len of table
            len_feeds_not_in_date_format_db_table = len(feeds_not_in_date_format_db_table)
            if len_feeds_not_in_date_format_db_table == 0:
                break
        
        self.status_null_feeds(edl_null_feeds_list)

        # replace problem values
        db_source = self.replacing_problematic_values()

        # remove problem feeds from null list
        edl_null_feeds_list = self.remove_problem_feeds_list(edl_null_feeds_df)

        tableObj = TABLES()
        edl_null_feeds = self.merge_EdlNullFeed_EdlDateFormat(edl_null_feeds_list, tableObj.edl_date_format())

        self.edl_new_feed_query_information()

        self.edl_db_count_query(edl_null_feeds)


        return db_source
