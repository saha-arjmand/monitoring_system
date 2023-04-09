# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

from script_modules.subscript.date.date import Date
import script_modules.subscript.check_file_exist.CheckFile as checkfile

############################################################################

class Print_Information():

    def __init__(self, dateObj):
        self.dateObj = dateObj

    def info_template(self):
        print("  we need this files to start :\n")
        print("  #1")
        print("  The raw file of the previous day's report")
        print("  EDL_vs_BIB_Reconciliation.xlsx")
        print("\n")

    def info_recon_file(self):

        print("  #2")
        print(f"  TZ_VW_EDL_EDW_FILE_RECON{self.dateObj.file_date}.csv\n")
        print("  query is :\n")
        print("  %trino(trino)")
        print(f"  select UPPER(EDL_FEED_NAME) AS EDL_FEED_NAME, UPPER(EDW_FEED_NAME) AS EDW_FEED_NAME, UPPER(TABLE_NAME) AS TABLE_NAME, UPPER(FEED_NAME) AS FEED_NAME, EDL_DATE_KEY, EDW_DATE_KEY, COALESCE(EDL_COUNT, 0 ) EDL_COUNT, COALESCE(EDW_COUNT, 0 ) EDW_COUNT, COALESCE(DIFF, 0 ) DIFF  from EDL_TZ.TZ_VW_EDL_EDW_FILE_RECON where EDL_DATE_KEY = '{self.dateObj.year_file}-{self.dateObj.month_file}-{self.dateObj.day_file}'")
        print("\n")

    def info_recon_db(self):

        print("  #1")
        print(f"  TZ_VW_EDL_EDW_DB_RECON{self.dateObj.date}.csv")
        print("  query is :\n")
        print("  %trino(trino)")
        day = self.dateObj.day
        month = self.dateObj.month
        year = self.dateObj.year
        print(f"  select UPPER(EDL_FEED_NAME) AS EDL_FEED_NAME, UPPER(TABLE_NAME) AS TABLE_NAME, UPPER(EDW_FEED_NAME) AS EDW_FEED_NAME, EDL_DATE_KEY, EDW_DATE_KEY, COALESCE(EDL_COUNT, 0 ) EDL_COUNT, COALESCE(EDW_COUNT, 0 ) EDW_COUNT, COALESCE(DIFF, 0 ) DIFF from EDL_TZ.TZ_VW_EDL_EDW_DB_RECON where EDL_DATE_KEY = '{year}-{month}-{day}'")
        print("\n")


############################################################################

if __name__ == "__main__":

    # template
    # date = dateObj.date
    date = "20221010"
    infoObj = Print_Information(date)
    infoObj.info_title()
    infoObj.info_template()