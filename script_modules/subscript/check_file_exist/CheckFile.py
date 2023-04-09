# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

from os import path as pathlibrary
from script_modules.subscript.write.write import WriteData
import pandas as pd
############################################################################

class CheckFile():

    def __init__(self, dateObj):
        self.dateObj = dateObj
        self._path = None

    @property
    def path(self):
        return self._path
        
    def is_file_exist(self, path, columns = []):
        date = self.dateObj.date

        # remove file path for fileName
        fileName = path.replace(".\input\\", "")

        while True:
            print(f'  Is the file "{fileName}" in the input folder ? (Y/N)   ')
            print("  If answer of the query was empty, type this word: empty. ")
            is_exist = input(f'  Answer (Y/N/empty)  ').lower()

            if is_exist=='y' and pathlibrary.exists(path):

                print(f"  {fileName} is exist in .\input")
                self._path = path
                break

            elif is_exist=='empty':
                data = []
                df = pd.DataFrame(data, columns= columns)
                list_path = list(path.split("."))
                
                wrtieObj = WriteData(path)

                if list_path[-1] == 'csv':
                    wrtieObj.write_date_to_csv(df)
                    break
                elif list_path[-1] == 'xlsx':
                    list_df = []
                    sheet_name = ['Sheet1']
                    list_df.append(df)
                    wrtieObj.write_date_to_excel(list_df,sheet_name)
                    break

            else:
                print(f"  ERROR !  {fileName} didn't exist in .\input ")
                print("  Try again ...\n")


if __name__ == "__main__":

    # date = dateObj.date
    from script_modules.subscript.date.date import Date
    date = Date()
    fileExistObj = CheckFile(date)
    path = ".\input\EDL_vs_BIB_Reconciliation.xlsx"
    fileExistObj.is_file_exist(path)
    print(fileExistObj.path)