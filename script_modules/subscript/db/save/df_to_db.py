# solved python search path for import module in other sub directories
import sys
import os
from numpy import piecewise
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import sqlite3
import script_modules.subscript.line.CreateLine as line
import script_modules.subscript.db.sqlite_help.sqlite_help as sqliteHelp
from script_modules.subscript.db.tables.tables import TABLES

############################################################################


class SAVE_DF_TO_DB:

    def __init__(self, dateObj, file_source, sheet_name) :
        self.dateObj = dateObj
        self.file_source = file_source
        self.sheet_name = sheet_name

    
    def save_df(self):

        line.title_between_line("Save data in the database")

        df_np = self.file_source.to_numpy()

        if self.sheet_name == 'file_source':

            conn = sqlite3.connect('Ingestion_monitoring.db')
            x = 1
            for anyItem in df_np:
                try:
                    conn.cursor().execute('''INSERT INTO EDL_VS_BIB_REPORT_FILE_SOURCE (id, date_key, edl_feed_name, edl_record_count, edw_record_count, diff, diff_percent, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',( str(anyItem[0])+str(anyItem[1]), anyItem[0], anyItem[1], anyItem[2], anyItem[3], anyItem[4], "%.5f%%" % (100 * float(anyItem[5])), anyItem[6]))
                    conn.commit()
                except Exception as e:
                    print(f"  error{x} is : {e}\n")
                x += 1
            conn.close()
        
        elif self.sheet_name == 'db':
            conn = sqlite3.connect('Ingestion_monitoring.db')
            x = 1
            for anyItem in df_np:
                try:
                    conn.cursor().execute('''INSERT INTO EDL_VS_BIB_REPORT_DB_SOURCE ('id', 'date_key', 'edl_feed_name', 'bib_feed_name', 'edl_record_count', 'bib_record_count', 'diff', 'diff_percent') VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',( str(anyItem[0])+str(anyItem[1]), anyItem[0], anyItem[1], anyItem[2], anyItem[3], anyItem[4], anyItem[5] ,"%.5f%%" % (100 * float(anyItem[6])) ))
                    conn.commit()
                except Exception as e:
                    print(f"  error{x} is : {e}\n")
                x += 1
            conn.close()


        print("  Storage Operations Completed\n")