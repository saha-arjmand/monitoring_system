# solved python search path for import module in other sub directories
import sys
import os
from numpy import piecewise
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import sqlite3
import pandas as pd
import script_modules.subscript.line.CreateLine as line

# show full pandas df
pd.set_option('display.max_rows', None)

############################################################################

class TABLES:

    ############################### Real Tables ###############################

    def __init__(self) :
        pass

    # date
    def edl_date_format(self):
        # Create Table
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS EDL_DATE_FORMAT (
        #                 id integer primary key not null,
        #                 edl_feed_name text not null,
        #                 cus_date_name text not null,
        #                 cus_date_format text not null,
        #                 type text not null,
        #                 unique (edl_feed_name COLLATE NOCASE)
        #                 ) """)

        # convert table to df
        c.execute("SELECT * FROM edl_date_format")
        data = c.fetchall()
        edl_date_format = pd.DataFrame(data, columns=['id', 'edl_feed_name', 'cus_date', 'cus_date_format', 'type'])
        
        conn.commit()
        conn.close()

        return edl_date_format

    def edw_date_format(self):
        # Create Table
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS EDW_DATE_FORMAT (
        #                 id integer primary key not null,
        #                 edl_feed_name text not null,
        #                 edw_table_name text not null,
        #                 partition text not null,
        #                 partition_format text not null,
        #                 type text not null,
        #                 unique (edl_feed_name COLLATE NOCASE)
        #                 ) """)

        # convert table to df
        c.execute("SELECT * FROM edw_date_format")
        data = c.fetchall()
        edw_date_format = pd.DataFrame(data, columns=['id', 'edl_feed_name', 'edw_table_name', 'partition', 'partition_format', 'type'])

        conn.commit()
        conn.close()

        return edw_date_format

    # problem
    def edl_problem_feeds(self):
        # Create Table
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS EDL_PROBLEM_FEEDS (
        #                 id integer primary key not null,
        #                 edl_feed_name text not null,
        #                 first_date integer not null,
        #                 last_date integer not null,
        #                 details text not null,
        #                 status text not null,
        #                 last_follow_up text not null,
        #                 action_on_report text not null,
        #                 type text not null,
        #                 unique (edl_feed_name COLLATE NOCASE)
        #                 ) """)

        # convert table to df
        c.execute("SELECT * FROM edl_problem_feeds")
        data = c.fetchall()
        edl_problem_feeds = pd.DataFrame(data, columns=['id', 'edl_feed_name', 'first_date', 'last_date', 'details', 'status', 'last_follow_up', 'action_on_report', 'type'])

        conn.commit()
        conn.close()

        return edl_problem_feeds

    def edw_problem_feeds(self):

        # Create Table
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS EDW_PROBLEM_FEEDS (
        #                 id integer primary key not null,
        #                 edl_feed_name text not null,
        #                 edw_table_name text not null,
        #                 first_date integer not null,
        #                 last_date integer not null,
        #                 details text not null,
        #                 status text not null,
        #                 last_follow_up text not null,
        #                 action_on_report text not null,
        #                     type text not null,
        #                 unique (edl_feed_name COLLATE NOCASE)
        #                 ) """)

        # convert table to df
        c.execute("SELECT * FROM edw_problem_feeds")
        data = c.fetchall()
        edw_problem_feeds = pd.DataFrame(data, columns=['id','edl_feed_name' , 'edw_table_name', 'first_date', 'last_date', 'details', 'status', 'last_follow_up', 'action_on_report', 'type'])

        conn.commit()
        conn.close()

        return edw_problem_feeds

    # mapping
    def FILE_FEEDS_EDL_EDW_DRL_MAPPING(self):
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS FILE_FEEDS_EDL_EDW_DRL_MAPPING (
        #                 id integer primary key not null,
        #                 edl_feed_name text not null,
        #                 edw_feed_name not null,
        #                 dw_task_id not null,
        #                 table_name text not null,
        #                 unique (edl_feed_name COLLATE NOCASE)
        #                 ) """)

        #  convert table to df
        c.execute("SELECT * FROM FILE_FEEDS_EDL_EDW_DRL_MAPPING")
        data = c.fetchall()
        file_feeds_edl_edw_dtl_mapping = pd.DataFrame(data, columns=['id', 'edl_feed_name', 'edw_feed_name', 'dw_task_id', 'table_name'])

        conn.commit()
        conn.close()

        return file_feeds_edl_edw_dtl_mapping

    def FEEDS_HAVE_ONE_FEED_ON_EDW(self):
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS FEEDS_HAVE_ONE_FEED_ON_EDW (
        #                 id integer primary key not null,
        #                 edl_feed_name text not null,
        #                 edw_feed_name  text not null,
        #                 type text not null,
        #                 unique (edl_feed_name COLLATE NOCASE)
        #                 ) """)

        #  convert table to df
        c.execute("SELECT * FROM FEEDS_HAVE_ONE_FEED_ON_EDW")
        data = c.fetchall()
        feeds_have_one_feed_on_edw = pd.DataFrame(data, columns=['id', 'edl_feed_name', 'edw_feed_name', 'type'])

        conn.commit()
        conn.close()

        return feeds_have_one_feed_on_edw

    def EDL_TABLE_WITH_EDW_TASK(self):
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS EDL_TABLE_WITH_EDW_TASK (
        #                 id integer primary key not null,
        #                 edl_feed_name text not null,
        #                 dw_task_name  text not null,
        #                 unique (edl_feed_name COLLATE NOCASE)
        #                 ) """)

        #  convert table to df
        c.execute("SELECT * FROM EDL_TABLE_WITH_EDW_TASK")
        data = c.fetchall()
        feeds_have_one_feed_on_edw = pd.DataFrame(data, columns=['id', 'edl_feed_name', 'dw_task_name'])

        conn.commit()
        conn.close()

        return feeds_have_one_feed_on_edw

    # edl_vs_bib report tbls
    def EDL_VS_BIB_REPORT_FILE_SOURCE(self):
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS EDL_VS_BIB_REPORT_FILE_SOURCE (
        #                 id text primary key not null,
        #                 date_key integer not null,
        #                 edl_feed_name text not null,
        #                 edl_record_count integer not null,
        #                 edw_record_count integer not null,
        #                 diff integer not null,
        #                 diff_percent real not null,
        #                 comment text,
        #                 unique (id COLLATE NOCASE)
        #                 ) """)

        #  convert table to df
        c.execute("SELECT * FROM EDL_VS_BIB_REPORT_FILE_SOURCE")
        data = c.fetchall()
        edl_vs_bib_report_file_source = pd.DataFrame(data, columns=['id', 'date_key', 'edl_feed_name', 'edl_record_count', 'edw_record_count', 'diff', 'diff_percent', 'comment'])

        conn.commit()
        conn.close()

        return edl_vs_bib_report_file_source

    def EDL_VS_BIB_REPORT_DB_SOURCE(self):
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS EDL_VS_BIB_REPORT_DB_SOURCE (
        #                 id text primary key not null,
        #                 date_key integer not null,
        #                 edl_feed_name text not null,
        #                 bib_feed_name text not null,
        #                 edl_record_count integer not null,
        #                 bib_record_count integer not null,
        #                 diff integer not null,
        #                 diff_percent real not null,
        #                 unique (id COLLATE NOCASE)
        #                 ) """)

        #  convert table to df
        c.execute("SELECT * FROM EDL_VS_BIB_REPORT_DB_SOURCE")
        data = c.fetchall()
        edl_vs_bib_report_db_source = pd.DataFrame(data, columns=['id', 'date_key', 'edl_feed_name', 'bib_feed_name', 'edl_record_count', 'bib_record_count', 'diff', 'diff_percent'])

        conn.commit()
        conn.close()

        return edl_vs_bib_report_db_source

    
    # edl_vs_bib_report_issues
    def EDL_VS_BIB_REPORT_FILE_ISSUES(self):
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS EDL_VS_BIB_REPORT_FILE_ISSUES (
        #                 id text primary key not null,
        #                 date_key integer not null,
        #                 edl_feed_name text not null,
        #                 old_edl_record_count integer not null,
        #                 old_edw_record_count integer not null,
        #                 old_diff integer not null,
        #                 old_diff_percent real not null,       
        #                 new_edl_record_count integer ,
        #                 new_edw_record_count integer ,
        #                 new_diff integer ,
        #                 new_diff_percent real ,
        #                 comment text,
        #                 Issue_reason text,
        #                 unique (id COLLATE NOCASE)
        #                 ) """)

        #  convert table to df
        c.execute("SELECT * FROM EDL_VS_BIB_REPORT_FILE_ISSUES")
        data = c.fetchall()
        edl_vs_bib_report_file_issues = pd.DataFrame(data, columns=['id', 'date_key', 'edl_feed_name', 'old_edl_record_count', 'old_edw_record_count', 'old_diff', 'old_diff_percent' ,'new_edl_record_count', 'new_edw_record_count', 'new_diff', 'new_diff_percent', 'comment', 'Issue_reason'])

        conn.commit()
        conn.close()

        return edl_vs_bib_report_file_issues


    # edl_recon_regex
    def EDL_RECON_REGEX(self):
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS EDL_RECON_REGEX (
        #                 id integer primary key not null,
        #                 edl_feed_name text not null,
        #                 recon_table text not null,
        #                 recon_regex text not null,
        #                 unique (edl_feed_name COLLATE NOCASE)
        #                 ) """)

        #  convert table to df
        c.execute("SELECT * FROM EDL_RECON_REGEX")
        data = c.fetchall()
        edl_recon_regex = pd.DataFrame(data, columns=['id', 'edl_feed_name', 'recon_table', 'recon_regex'])

        conn.commit()
        conn.close()

        return edl_recon_regex

    
    def ALL_FILE_MAP(self):
        conn = sqlite3.connect('Ingestion_monitoring.db')
        c = conn.cursor()
        # c.execute(""" CREATE TABLE IF NOT EXISTS ALL_FILE_MAP (
        #                 id integer primary key not null,
        #                 task text not null,
        #                 column_order integer not null,
        #                 column_name text not null,
        #                 sqlloader_format text not null,
        #                 oracle_format text not null,
        #                 unique (id COLLATE NOCASE)
        #                 ) """)

        #  convert table to df
        c.execute("SELECT * FROM ALL_FILE_MAP")
        data = c.fetchall()
        all_file_map = pd.DataFrame(data, columns=['id', 'task', 'column_order', 'column_name', 'sqlloader_format', 'oracle_format'])

        conn.commit()
        conn.close()

        return all_file_map


    ############################### Virtual Table ###############################

    @property
    def edl_problem_feeds_file(self):
        tableObj = TABLES()
        edl_problem_feeds = tableObj.edl_problem_feeds()
        edl_problem_feeds_np = edl_problem_feeds.to_numpy()

        edl_problem_feeds_file_list = []
        
        for anyItem in edl_problem_feeds_np:
            if anyItem[8] == 'file':
                edl_problem_feeds_file_list.append(anyItem)

        columns = ['id', 'edl_feed_name', 'first_date', 'last_date', 'details', 'status', 'last_follow_up', 'action_on_report', 'type']
        edl_problem_feeds_file = pd.DataFrame(edl_problem_feeds_file_list, columns=columns)

        if edl_problem_feeds_file.empty:
            # msg = '  Edl_problem_feeds_file is empty!'
            # print(msg)
            return edl_problem_feeds_file
        else:
            return edl_problem_feeds_file


    @property
    def edl_problem_feeds_db(self):
        tableObj = TABLES()
        edl_problem_feeds = tableObj.edl_problem_feeds()
        edl_problem_feeds_np = edl_problem_feeds.to_numpy()

        edl_problem_feeds_db_list = []

        for anyItem in edl_problem_feeds_np:
            if anyItem[8] == 'db':
                edl_problem_feeds_db_list.append(anyItem)

        columns = ['id', 'edl_feed_name', 'first_date', 'last_date', 'details', 'status', 'last_follow_up', 'action_on_report', 'type']
        edl_problem_feeds_db = pd.DataFrame(edl_problem_feeds_db_list, columns=columns)

        if edl_problem_feeds_db.empty:
            # msg = '  Edl_problem_feeds_db is empty!'
            # print(msg)
            return edl_problem_feeds_db
        else:
            return edl_problem_feeds_db


    @property
    def edw_problem_feeds_file(self):
        tableObj = TABLES()
        edw_problem_feeds = tableObj.edw_problem_feeds()
        edw_problem_feeds_np = edw_problem_feeds.to_numpy()

        edw_problem_feeds_file_list = []
        
        for anyItem in edw_problem_feeds_np:
            if anyItem[9] == 'file':
                edw_problem_feeds_file_list.append(anyItem)

        columns = ['id','edl_feed_name' , 'edw_table_name', 'first_date', 'last_date', 'details', 'status', 'last_follow_up', 'action_on_report', 'type']
        edw_problem_feeds_file = pd.DataFrame(edw_problem_feeds_file_list, columns=columns)

        if edw_problem_feeds_file.empty:
            # msg = '  Edw_problem_feeds_file is empty!'
            # print(msg) 
            return edw_problem_feeds_file
        else:
            return edw_problem_feeds_file


    @property
    def edw_problem_feeds_db(self):
        tableObj = TABLES()
        edw_problem_feeds = tableObj.edw_problem_feeds()
        edw_problem_feeds_np = edw_problem_feeds.to_numpy()

        edw_problem_feeds_db_list = []
        
        for anyItem in edw_problem_feeds_np:
            if anyItem[9] == 'db':
                edw_problem_feeds_db_list.append(anyItem)

        columns = ['id','edl_feed_name' , 'edw_table_name', 'first_date', 'last_date', 'details', 'status', 'last_follow_up', 'action_on_report', 'type']
        edw_problem_feeds_db = pd.DataFrame(edw_problem_feeds_db_list, columns=columns)

        if edw_problem_feeds_db.empty:
            # msg = '  Edw_problem_feeds_file is empty!'
            # print(msg)
            return edw_problem_feeds_db
        else:
            return edw_problem_feeds_db



    def show_table(self, table_name):
        if table_name.lower() == 'edl_date_format':
            table = self.edl_date_format()
        elif table_name.lower() == 'edw_date_format':
            table = self.edw_date_format()
        elif table_name.lower() == 'edl_problem_feeds':
            table = self.edl_problem_feeds()
        elif table_name.lower() == 'edw_problem_feeds':
            table = self.edw_problem_feeds()
        elif table_name.upper() == 'FILE_FEEDS_EDL_EDW_DRL_MAPPING':
            table = self.FILE_FEEDS_EDL_EDW_DRL_MAPPING()
        elif table_name.upper() == 'FEEDS_HAVE_ONE_FEED_ON_EDW':
            table = self.FEEDS_HAVE_ONE_FEED_ON_EDW()
        elif table_name.upper() == 'EDL_VS_BIB_REPORT_FILE_SOURCE':
            table = self.EDL_VS_BIB_REPORT_FILE_SOURCE()
        elif table_name.upper() == 'EDL_TABLE_WITH_EDW_TASK':
            table = self.EDL_TABLE_WITH_EDW_TASK()
        elif table_name.upper() == 'EDL_VS_BIB_REPORT_DB_SOURCE':
            table = self.EDL_VS_BIB_REPORT_DB_SOURCE()
        elif table_name.upper() == 'EDL_VS_BIB_REPORT_FILE_ISSUES':
            table = self.EDL_VS_BIB_REPORT_FILE_ISSUES()
        elif table_name.upper() == 'EDL_RECON_REGEX':
            table = self.EDL_RECON_REGEX()
        elif table_name.upper() == 'ALL_FILE_MAP':
            table = self.ALL_FILE_MAP()
        
        
        return table

############################################################################


if __name__ == "__main__":

    tablesObj = TABLES()

    # date
    line.title_between_line("edl_date_format")
    print(f"{tablesObj.edl_date_format()}\n\n")

    line.title_between_line("edw_date_format")
    print(f"{tablesObj.edw_date_format()}\n\n")

    # problem
    line.title_between_line("edl_problem_feeds")
    print(f"{tablesObj.edl_problem_feeds()}\n\n")

    line.title_between_line("edw_problem_feeds")
    print(f"{tablesObj.edw_problem_feeds()}\n\n")

    line.title_between_line("FILE_FEEDS_EDL_EDW_DRL_MAPPING")
    print(f"{tablesObj.FILE_FEEDS_EDL_EDW_DRL_MAPPING()}\n\n")

    line.title_between_line("FEEDS_HAVE_ONE_FEED_ON_EDW")
    print(f"{tablesObj.FEEDS_HAVE_ONE_FEED_ON_EDW()}\n\n")

    line.title_between_line("EDL_VS_BIB_REPORT_FILE_SOURCE")
    print(f"{tablesObj.EDL_VS_BIB_REPORT_FILE_SOURCE()}\n\n")

    line.title_between_line("EDL_TABLE_WITH_EDW_TASK")
    print(f"{tablesObj.EDL_TABLE_WITH_EDW_TASK()}\n\n")

    line.title_between_line("EDL_VS_BIB_REPORT_DB_SOURCE")
    print(f"{tablesObj.EDL_VS_BIB_REPORT_DB_SOURCE()}\n\n")

    line.title_between_line("EDL_VS_BIB_REPORT_FILE_ISSUES")
    print(f"{tablesObj.EDL_VS_BIB_REPORT_FILE_ISSUES()}\n\n")

    line.title_between_line("EDL_RECON_REGEX")
    print(f"{tablesObj.EDL_RECON_REGEX()}\n\n")

    line.title_between_line("ALL_FILE_MAP")
    print(f"{tablesObj.ALL_FILE_MAP()}\n\n")
