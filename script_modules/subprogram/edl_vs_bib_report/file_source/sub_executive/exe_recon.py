# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import script_modules.subscript.line.CreateLine as line
from script_modules.subscript.information.information import Print_Information
from script_modules.subscript.check_file_exist.CheckFile import CheckFile
from script_modules.subscript.read_file.read_file import ReadCSV
from script_modules.subscript.merge_file.mergedf import MergeDataFrame

############################################################################

class Recon:

    def __init__(self, dateObj, file_source) :
        self.dateObj = dateObj
        self.file_source = file_source

    def recon_print_information(self):
        line.title_between_line("Recon")
        infoObj = Print_Information(self.dateObj)
        infoObj.info_recon_file()

    def recon_isexist(self):
        reconExistObj = CheckFile(self.dateObj)
        path = f".\input\TZ_VW_EDL_EDW_FILE_RECON{self.dateObj.year_file}{self.dateObj.month_file}{self.dateObj.day_file}.csv"
        reconExistObj.is_file_exist(path)
        return reconExistObj.path

    def recon_reading(self, path):
        readReconObj = ReadCSV(path)
        readRecon = readReconObj.readCsvSheet(['EDL_FEED_NAME', 'EDL_COUNT', 'EDW_COUNT'])
        print("\n  recon file was read .")
        return readRecon

    def merge_FileSources_Filerecon(self, reconFile):
        mergeObj = MergeDataFrame(self.file_source, reconFile, self.dateObj)
        df = mergeObj.merge_multiIndex()
        df = mergeObj.replace(df, df.columns[2], df.columns[7])
        df = mergeObj.replace(df, df.columns[3], df.columns[7])
        print("  The merging of Reconciliation and ReconFile was done.\n")
        return df

    def executive(self):
        self.recon_print_information()
        path_recon = self.recon_isexist()
        recon_file = self.recon_reading(path_recon)
        file_source = self.merge_FileSources_Filerecon(recon_file)
        return file_source