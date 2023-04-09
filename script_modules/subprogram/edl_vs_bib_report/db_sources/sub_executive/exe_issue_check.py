# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

from script_modules.subscript.recheck.recheck_issue import RECHECK_ISSUE
############################################################################

class ISSUECHECK:

    def __init__(self, df, dateObj) :
        self.df = df
        self.dateObj = dateObj


    def check_issues_feeds_and_replace_values_edl(self):
        recheck_issue_obj = RECHECK_ISSUE(self.df, self.dateObj ,"db")

        newdf = recheck_issue_obj.apply_edl_issue_table()
        return newdf

    def check_issues_feeds_and_replace_values_edw(self, df):
        recheck_issue_obj = RECHECK_ISSUE(df, self.dateObj ,"db")
        newdf = recheck_issue_obj.apply_edw_issue_table_list()
        return newdf


    def executive(self):
        # Recheck_Issue
        # edl
        apply_edl_issue_table_df = self.check_issues_feeds_and_replace_values_edl()
        # edw
        df = self.check_issues_feeds_and_replace_values_edw(apply_edl_issue_table_df)

        return df