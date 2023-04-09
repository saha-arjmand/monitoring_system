# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

# show full pandas df
# pd.set_option('display.max_rows', None)

import script_modules.subscript.line.CreateLine as line
import time

import script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_get_date as exe_get_date
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_template import Template
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_recon import Recon
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_null_edl import EDL_NULL
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_null_edw import EDW_NULL
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_merge_new_feeds import MERGE_NEW_FEEDS
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_merge_feeds_have_one_edw import MERGE_FEEDS_HAVE_ONE_EDW
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_calculation import CALCULATION
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_issue_check import ISSUECHECK
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_repair import REPAIR
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_recheck_edl import RECHECK_EDL
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_merge_recheck_feeds import *
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_recheck_edw import RECHECK_EDW
from script_modules.subscript.db.save.df_to_db import SAVE_DF_TO_DB
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_fillna import FILLNA

############################################################################

class DB_SOURCE:

    def __init__(self, dateObj):
        self.dateObj = dateObj

    def db_source(self):
        
        line.title_between_line("DB SOURCE start in the next 3 seconds")
        time.sleep(3)

        # 01
        dateObj = exe_get_date.get_date(self.dateObj)

        # 02
        templateObj = Template(dateObj)
        db_source = templateObj.executive()

        # 03
        reconObj = Recon(dateObj, db_source)
        db_source = reconObj.executive()

        # 04
        nullEdlObj = EDL_NULL(db_source, dateObj)
        db_source = nullEdlObj.executive()

        # 05
        nullEdwObj = EDW_NULL(db_source, dateObj)
        db_source = nullEdwObj.executive()

        # 06
        mergeNewFeedsObj = MERGE_NEW_FEEDS(dateObj, db_source)
        db_source = mergeNewFeedsObj.executive()

        # 07 merge feeds that have one feed in edw side
        merge_feeds_have_one_feed_on_edw_file = MERGE_FEEDS_HAVE_ONE_EDW(dateObj, db_source)
        db_source = merge_feeds_have_one_feed_on_edw_file.executive()

        # 08 If df have null in calc column then use fillna
        fill_naObj = FILLNA(db_source, dateObj, 'db')
        db_source = fill_naObj.executive()

        # 09
        calcObj = CALCULATION(db_source, dateObj)
        db_source = calcObj.executive()
        
        #############################
        # 10
        # We once fix issue before recheck because reduce recheck query
        issueCheckObj = ISSUECHECK(db_source, dateObj)
        db_source = issueCheckObj.executive()

        # 11
        calcObj = CALCULATION(db_source, dateObj)
        db_source = calcObj.executive()

        #############################
        # 12
        repairObj = REPAIR(db_source)
        repairQuery = repairObj.executive()

        # 13
        recheckEdlObj = RECHECK_EDL(db_source, dateObj)
        recheckEdl = recheckEdlObj.executive()

        # 14
        mergeRecheckEDL = MERGE_RECHECK_EDL_FEEDS(dateObj, db_source)
        db_source = mergeRecheckEDL.executive()

        # 15
        calcObj = CALCULATION(db_source, dateObj)
        db_source = calcObj.executive()

        #############################
        # 16
        recheckEdwObj = RECHECK_EDW(db_source, dateObj)
        recheckEdw = recheckEdwObj.executive()

        # 17
        mergeRecheckEDW = MERGE_RECHECK_EDW_FEEDS(dateObj, db_source)
        db_source = mergeRecheckEDW.executive()

        # 18 merge feeds that have one feed in edw side
        merge_feeds_have_one_feed_on_edw_file = MERGE_FEEDS_HAVE_ONE_EDW(dateObj, db_source)
        db_source = merge_feeds_have_one_feed_on_edw_file.executive()

        # 19
        calcObj = CALCULATION(db_source, dateObj)
        db_source = calcObj.executive()

        #############################

        # 20 Save df to db
        save_to_db_obj = SAVE_DF_TO_DB(dateObj, db_source, 'db')
        save_to_db_obj.save_df()

        return db_source
############################################################################

if __name__ == "__main__":
    pass
