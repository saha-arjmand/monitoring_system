# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import script_modules.subscript.line.CreateLine as line
from script_modules.subscript.date.date import Date

############################################################################

# get date
def get_date():
    line.title_between_line("Get & Validate date")
    dateObj = Date()
    dateObj.execute('edl_vs_bib_report')
    return dateObj

############################################################################


