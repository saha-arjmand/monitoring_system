# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

from script_modules.subscript.db.tables.tables import TABLES
import sqlite3
import pandas as pd
from script_modules.subscript.write.write import WriteData
############################################################################


class FILE_ISSUE:

    def __init__(self, dateObj) :
        self.dateObj = dateObj


    def get_file_issue_from_db_and_save_to_issue_tbl(self):

        tblObj = TABLES()

        edl_vs_bib_report_file_source_np = tblObj.EDL_VS_BIB_REPORT_FILE_SOURCE().to_numpy()

        date = int(self.dateObj.file_date)

        x = 1
        for anyItem in edl_vs_bib_report_file_source_np:
            if int(anyItem[1]) == date and float(anyItem[6].replace('%', '')) > 0.000000:
                conn = sqlite3.connect('Ingestion_monitoring.db')
                try:
                    conn.cursor().execute('''INSERT INTO EDL_VS_BIB_REPORT_FILE_ISSUES (id, date_key, edl_feed_name, old_edl_record_count, old_edw_record_count, old_diff, old_diff_percent ,new_edl_record_count, new_edw_record_count, new_diff, new_diff_percent, comment, Issue_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',( anyItem[0], anyItem[1], anyItem[2], anyItem[3], anyItem[4], anyItem[5], anyItem[6], None, None, None, None, anyItem[7], None))
                    conn.commit()
                except Exception as e:
                    print(f"  error{x} is : {e}\n")
                conn.close()
                x += 1

        print("  The problematic feeds were stored in the table .\n")


    def Issue_list(self):

        tblObj = TABLES()
        edl_vs_bib_report_file_issues_np = tblObj.EDL_VS_BIB_REPORT_FILE_ISSUES().to_numpy()

        issueList = []

        for anyItem in edl_vs_bib_report_file_issues_np:
            list_item = []
            list_item.extend([anyItem[1], anyItem[2], anyItem[3], anyItem[4], anyItem[5], anyItem[6], anyItem[7], anyItem[8], anyItem[9], anyItem[10], anyItem[11], anyItem[12] ])
            issueList.append(list_item)

        
        return issueList
    

    def convert_list_to_list_df(self, issue_list):

        columns = ['date_key', 'edl_feed_name', 'old_edl_record_count', 'old_edw_record_count', 'old_diff', 'old_diff_percent' ,'new_edl_record_count', 'new_edw_record_count', 'new_diff', 'new_diff_percent', 'comment', 'Issue_reason']

        df = pd.DataFrame(issue_list, columns=columns)

        ls = []
        ls.append(df)

        return ls


    def write_to_excel(self, list_dfs):

        path = f".\output\Issue_list_{self.dateObj.day}{self.dateObj.convert_month_number_to_word}.xlsx"
        title = "Write Issue list file to output"
        writeObj = WriteData(path, title)
        sheet_names = ['File Issues']
        writeObj.write_date_to_excel(list_dfs, sheet_names)


    def executive(self):
        
        self.get_file_issue_from_db_and_save_to_issue_tbl()

        issueList = self.Issue_list()
        list_of_dfs = self.convert_list_to_list_df(issueList)

        self.write_to_excel(list_of_dfs)
        