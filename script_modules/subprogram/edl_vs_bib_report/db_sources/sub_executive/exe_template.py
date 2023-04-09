# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import script_modules.subscript.line.CreateLine as line
from script_modules.subscript.information.information import Print_Information
from script_modules.subscript.check_file_exist.CheckFile import CheckFile
from script_modules.subscript.read_file.read_file import ReadExcel
from script_modules.subscript.cleanup_file.cleanup import DataCleaner
############################################################################


class Template:

    def __init__(self, dateObj) :
        self.dateObj = dateObj

    def template_reading(self):
        path = ".\input\EDL_vs_BIB_Reconciliation.xlsx"
        readExcelObj = ReadExcel(path)
        db_sources = readExcelObj.readExcelSheet("DB Sources", None, [0,1])
        print("\n  DB Sources from template was read .")
        return db_sources

    def template_cleaning(self, df):
        dataCleanObj = DataCleaner(self.dateObj, df, 'db') 
        dbSourceClean = dataCleanObj.clean_exelsheet()
        print("  Old data was removed from the db source.\n")
        return dbSourceClean

    def template_header_update(self, df):
        updateObj = DataCleaner(self.dateObj, df, 'db') 
        updatedf = updateObj.update_header()
        print("  Update header.\n")
        return updatedf

    def executive(self):
        line.title_between_line("Read DB Template")
        db_source = self.template_reading()
        db_source = self.template_cleaning(db_source)
        db_source = self.template_header_update(db_source)
        return db_source