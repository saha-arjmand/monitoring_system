# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

from script_modules.subscript.db.tables.tables import TABLES
from script_modules.subscript.cleanup_file.cleanup import DataCleaner
import pandas as pd

############################################################################


class RECHECK_ISSUE:

    def __init__(self, df, dateObj, type) :
        self.type = type
        self.df = df
        self.dateObj = dateObj

    def apply_edl_issue_table(self):
  

        """ 
            list of issues
            edl :
                inactive   -> replace 0
        """

        tableObj = TABLES()
        edl_problem_feeds = tableObj.edl_problem_feeds()
        edl_problem_feeds_np = edl_problem_feeds.to_numpy()

        df_np = self.df.to_numpy()

        if self.type.lower() == 'file':
            # inactive
            for anyFeed in df_np :
                for anyFeedIssue in edl_problem_feeds_np:
                    if (anyFeedIssue[5]).lower() == 'inactive':
                        if (anyFeedIssue[1]).upper() == anyFeed[1]:
                            anyFeed[2] = 0

            df = pd.DataFrame(df_np)
            
            headerObj = DataCleaner(self.dateObj, df, 'file')
            file_source = headerObj.update_header()

            return file_source
        
        elif self.type.lower() == 'db':
            # inactive
            for anyFeed in df_np :
                for anyFeedIssue in edl_problem_feeds_np:
                    if (anyFeedIssue[5]).lower() == 'inactive':
                        if (anyFeedIssue[1]).upper() == anyFeed[1]:
                            anyFeed[3] = 0

            df = pd.DataFrame(df_np)
            
            headerObj = DataCleaner(self.dateObj, df, 'db')
            db_source = headerObj.update_header()

            return db_source
        


    def apply_edw_issue_table_list(self):

        """ 
            list of issues
            edw :
                NO (dont exist) -> replace 0
                header -> + (header count)
                bad row count -> + (row count)
        """
        
        tableObj = TABLES()
        edw_problem_feeds = tableObj.edw_problem_feeds()
        edw_problem_feeds_np = edw_problem_feeds.to_numpy()
        
        df_np = self.df.to_numpy()

        if self.type.lower() == 'file':
            # dont exist
            for anyFeed in df_np:
                for anyFeedIssue in edw_problem_feeds_np:
                    if (anyFeedIssue[6]).upper() == 'NO':
                        if (anyFeedIssue[1]).upper() == anyFeed[1]:
                            anyFeed[2] = 0

                    elif (anyFeedIssue[6]).lower() == 'header' or (anyFeedIssue[6]).lower() == 'badrow':
                            if (anyFeedIssue[1]).upper() == anyFeed[1]:
                                if anyFeed[3] != 0 :
                                    if anyFeed[2] != anyFeed[3]:
                                        anyFeed[3] = (int(anyFeed[3]) + int(anyFeedIssue[8]))

            

            df = pd.DataFrame(df_np)
            headerObj = DataCleaner(self.dateObj, df, 'file')
            file_source = headerObj.update_header()

            return file_source

        elif self.type.lower() == 'db':
            # dont exist
            for anyFeed in df_np:
                for anyFeedIssue in edw_problem_feeds_np:
                    if (anyFeedIssue[6]).upper() == 'NO':
                        if (anyFeedIssue[1]).upper() == anyFeed[1]:
                            anyFeed[3] = 0

                    elif (anyFeedIssue[6]).lower() == 'header' or (anyFeedIssue[6]).lower() == 'badrow':
                            if (anyFeedIssue[1]).upper() == anyFeed[1]:
                                if anyFeed[3] != 0 :
                                    if anyFeed[3] != anyFeed[4]:
                                        anyFeed[4] = (int(anyFeed[4]) + int(anyFeedIssue[8]))

            

            df = pd.DataFrame(df_np)
            headerObj = DataCleaner(self.dateObj, df, 'db')
            db_source = headerObj.update_header()

            return db_source




############################################################################

if __name__ == "__main__":
    pass