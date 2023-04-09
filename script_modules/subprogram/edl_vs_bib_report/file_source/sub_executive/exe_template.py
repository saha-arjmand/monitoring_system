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

    def template_print_information(self):
        line.title_between_line("Template")
        date = self.dateObj.date
        infoObj = Print_Information(date)
        infoObj.info_template()

    def template_isexist(self):
        fileExistObj = CheckFile(self.dateObj)
        path = ".\input\EDL_vs_BIB_Reconciliation.xlsx"
        fileExistObj.is_file_exist(path)
        return fileExistObj.path

    def template_reading(self, path):
        readExcelObj = ReadExcel(path)
        file_sources = readExcelObj.readExcelSheet("File Sources", None, [0,1])
        print("\n  file source from template was read .")
        return file_sources

    def template_cleaning(self, df):
        dataCleanObj = DataCleaner(self.dateObj, df, 'file') 
        fileSourceClean = dataCleanObj.clean_exelsheet()
        print("  Old data was removed from the file source.\n")
        return fileSourceClean

    def template_header_update(self, df):
        updateObj = DataCleaner(self.dateObj, df, 'file') 
        updatedf = updateObj.update_header()
        print("  Update header.\n")
        return updatedf

    def executive(self):
        self.template_print_information()
        path_template = self.template_isexist()
        file_source = self.template_reading(path_template)
        file_source = self.template_cleaning(file_source)
        file_source = self.template_header_update(file_source)
        return file_source