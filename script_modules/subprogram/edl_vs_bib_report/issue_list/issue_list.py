# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

# show full pandas df
# pd.set_option('display.max_rows', None)

import script_modules.subscript.line.CreateLine as line
from script_modules.subprogram.edl_vs_bib_report.issue_list.sub_executive.file_issues import FILE_ISSUE

############################################################################

class ISSUE_LIST:

    def __init__(self, dateObj) :
        self.dateObj = dateObj

    def issue_list(self):
        
        line.title_between_line("Export list of issues")

        # get file issues
        fileIssueObj = FILE_ISSUE(self.dateObj)
        fileIssueObj.executive()
        



############################################################################

if __name__ == "__main__":
    issueListObj = ISSUE_LIST()


