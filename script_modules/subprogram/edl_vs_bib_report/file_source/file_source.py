# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)


# show full pandas df
# import pandas as pd
# pd.set_option('display.max_rows', None)

import script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_get_date as exe_get_date
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_template import Template
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_recon import Recon
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_null_edl import EDL_NULL
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_null_edw import EDW_NULL
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_merge_new_feeds import MERGE_NEW_FEEDS
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_calculation import CALCULATION
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_repair import REPAIR
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_recheck_edl import RECHECK_EDL
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_recheck_edw import RECHECK_EDW
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_issue_check import ISSUECHECK
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_merge_recheck_feeds import *
from script_modules.subprogram.edl_vs_bib_report.file_source.sub_executive.exe_merge_feeds_have_one_edw import MERGE_FEEDS_HAVE_ONE_EDW
from script_modules.subscript.db.save.df_to_db import SAVE_DF_TO_DB
from script_modules.subprogram.edl_vs_bib_report.db_sources.sub_executive.exe_fillna import FILLNA


############################################################################

class FILE_SOURCE:

    def __init__(self):
        pass

    def file_source(self):
        # 01
        dateObj = exe_get_date.get_date()


        # 02
        templateObj = Template(dateObj)
        file_source = templateObj.executive()


        # 03
        reconObj = Recon(dateObj, file_source)
        file_source = reconObj.executive()


        # 04
        nullEdlObj = EDL_NULL(file_source, dateObj)
        file_source = nullEdlObj.executive()


        # 05
        nullEdwObj = EDW_NULL(file_source, dateObj)
        file_source = nullEdwObj.executive()

        # 06
        mergeNewFeedsObj = MERGE_NEW_FEEDS(dateObj, file_source)
        file_source = mergeNewFeedsObj.executive()


        # 07 merge feeds that have one feed in edw side
        merge_feeds_have_one_feed_on_edw_file = MERGE_FEEDS_HAVE_ONE_EDW(dateObj, file_source)
        file_source = merge_feeds_have_one_feed_on_edw_file.executive()

        # 08 If df have null in calc column then use fillna
        fill_naObj = FILLNA(file_source, dateObj, 'file')
        file_source = fill_naObj.executive()

        # 09
        calcObj = CALCULATION(file_source, dateObj)
        file_source = calcObj.executive()
        
        #############################
        # 10
        # We once fix issue before recheck because reduce recheck query
        issueCheckObj = ISSUECHECK(file_source, dateObj)
        file_source = issueCheckObj.executive()

        # 11 Calculation again
        calcObj2 = CALCULATION(file_source, dateObj)
        file_source = calcObj2.executive()

        #############################
        # 12
        repairObj = REPAIR(file_source)
        repairQuery = repairObj.executive()

        # 13
        recheckEdlObj = RECHECK_EDL(file_source, dateObj)
        recheckEdl = recheckEdlObj.executive()

        # 14
        mergeRecheckEDL = MERGE_RECHECK_EDL_FEEDS(dateObj, file_source)
        file_source = mergeRecheckEDL.executive()

        # 15 new calc
        calcObj3 = CALCULATION(file_source, dateObj)
        file_source = calcObj3.executive()

        #############################

        # 16
        recheckEdwObj = RECHECK_EDW(file_source, dateObj)
        recheckEdw = recheckEdwObj.executive()

        # 17
        mergeRecheckEDW = MERGE_RECHECK_EDW_FEEDS(dateObj, file_source)
        file_source = mergeRecheckEDW.executive()

        # 18 new calc
        calcObj4 = CALCULATION(file_source, dateObj)
        file_source = calcObj4.executive()

        #############################

        # 19 merge feeds that have one feed in edw side
        merge_feeds_have_one_feed_on_edw_file = MERGE_FEEDS_HAVE_ONE_EDW(dateObj, file_source)
        file_source = merge_feeds_have_one_feed_on_edw_file.executive()

        # 20 new calc
        calcObj5 = CALCULATION(file_source, dateObj)
        file_source = calcObj5.executive()

        #############################

        # 21 Save df to db
        save_to_db_obj = SAVE_DF_TO_DB(dateObj, file_source, 'file_source')
        save_to_db_obj.save_df()

        return dateObj, file_source


############################################################################

if __name__ == "__main__":
    
    fileSourceObj = FILE_SOURCE()
    file_source = fileSourceObj.file_source()
    dateObj = file_source[0]
    file_source = file_source[1]

