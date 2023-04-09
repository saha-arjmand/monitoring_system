# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

from script_modules.subscript.date.date import Date

############################################################################

# get date
def get_date(dateObj):
    print(f"\n  db date is   : {dateObj.date}   or   {dateObj.day} {dateObj.convert_month_number_to_word}")
    print("\n")
    return dateObj

############################################################################


