# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

from script_modules.subscript.db.tables.tables import TABLES
from script_modules.subscript.merge_file.mergedf import MergeDataFrame
import pandas as pd

############################################################################

class NULL_STATUS:

    def __init__(self, dateObj) :
        self.dateObj = dateObj

    def merge_NullFeed_DateFormat(self, edl_null_feeds, edl_date_format):
        mergeObj = MergeDataFrame(edl_null_feeds, edl_date_format, self.dateObj)
        df = mergeObj.merge_unname_columns("EDL_FEED_NAME", "edl_feed_name")

        # remove additional column
        df = df.drop('id', axis=1)
        df = df.drop('edl_feed_name', axis=1)
        df = df.drop('type', axis=1)

        return df


    def null_status(self, null_feeds_list, edl_or_edw, type):

        tableObj = TABLES()

        if edl_or_edw.lower() == "edl":
            print("\n")
            print("  The status of null feeds is as follows: ")
            print(f"  Total number of edl null feeds: {len(null_feeds_list)}")

            if type == 'file':
                if isinstance(tableObj.edl_problem_feeds_file, pd.DataFrame):
                    edl_problem_feeds_file_count = len(tableObj.edl_problem_feeds_file)
                    print(f"  Total number of EDL Problem file feeds: {edl_problem_feeds_file_count}")
                    print("\n")

                    showDetails = input("  Do you want to show details? (Y/N)")
                    if showDetails.lower() == "y":
                        print("\nTotal null feeds: ")
                        null_feeds_df = pd.DataFrame(null_feeds_list, columns=['EDL_FEED_NAME'])
                        null_feeds = self.merge_NullFeed_DateFormat(null_feeds_df, tableObj.edl_date_format())
                        print(null_feeds)
                        print("\n\n")

                        print("EDL problem db feeds: ")
                        print("\n")
                        print(tableObj.edl_problem_feeds_file)
                        print("\n\n")
                else:
                    print(tableObj.edl_problem_feeds_file)
            
            elif type == 'db':
                if isinstance(tableObj.edl_problem_feeds_db, pd.DataFrame):
                    edl_problem_feeds_file_count = len(tableObj.edl_problem_feeds_db)
                    print(f"  Total number of EDL Problem db feeds: {edl_problem_feeds_file_count}")
                    print("\n")

                    showDetails = input("  Do you want to show details? (Y/N)")
                    if showDetails.lower() == "y":
                        print("\nTotal null feeds: ")
                        null_feeds_df = pd.DataFrame(null_feeds_list, columns=['EDL_FEED_NAME'])
                        null_feeds = self.merge_NullFeed_DateFormat(null_feeds_df, tableObj.edl_date_format())
                        print(null_feeds)
                        print("\n\n")

                        print("EDL problem db feeds: ")
                        print("\n")
                        print(tableObj.edl_problem_feeds_db)
                        print("\n\n")
                else:
                    print(tableObj.edl_problem_feeds_db)
            

        elif edl_or_edw.lower() == "edw":
            print("\n")
            print("  The status of null feeds is as follows: ")
            print(f"  Total number of edw null feeds: {len(null_feeds_list)}")

            if type == 'file':
                if isinstance(tableObj.edw_problem_feeds_file, pd.DataFrame):
                    edw_problem_feeds_file_count = len(tableObj.edw_problem_feeds_file)
                    print(f"  Total number of EDW Problem file feeds: {edw_problem_feeds_file_count}")
                    print("\n")

                    showDetails = input("  Do you want to show details? (Y/N)")
                    if showDetails.lower() == "y":
                        print("\nTotal null feeds: ")
                        null_feeds_df = pd.DataFrame(null_feeds_list, columns=['EDL_FEED_NAME'])
                        null_feeds = self.merge_NullFeed_DateFormat(null_feeds_df, tableObj.edw_date_format())
                        print(null_feeds)
                        print("\n\n")

                        print("EDW problem db feeds: ")
                        print("\n")
                        print(tableObj.edw_problem_feeds_file)
                        print("\n\n")
                else:
                    print(tableObj.edw_problem_feeds_file)
            
            elif type == 'db':
                if isinstance(tableObj.edw_problem_feeds_db, pd.DataFrame):
                    edw_problem_feeds_file_count = len(tableObj.edw_problem_feeds_db)
                    print(f"  Total number of EDW Problem db feeds: {edw_problem_feeds_file_count}")
                    print("\n")

                    showDetails = input("  Do you want to show details? (Y/N)")
                    if showDetails.lower() == "y":
                        print("\nTotal null feeds: ")
                        null_feeds_df = pd.DataFrame(null_feeds_list, columns=['EDL_FEED_NAME'])
                        null_feeds = self.merge_NullFeed_DateFormat(null_feeds_df, tableObj.edw_date_format())
                        print(null_feeds)
                        print("\n\n")

                        print("EDW problem db feeds: ")
                        print("\n")
                        print(tableObj.edw_problem_feeds_db)
                        print("\n\n")
                else:
                    print(tableObj.edw_problem_feeds_db)

############################################################################

if __name__ == "__main__":

    nullStatusObj = NULL_STATUS()
