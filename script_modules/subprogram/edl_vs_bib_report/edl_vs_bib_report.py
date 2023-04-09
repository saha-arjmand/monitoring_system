import sys
# solved python search path for import module in other sub directories
original_path = "d:\Media\desktop\EDL_VS_BIB Automate"
sys.path.append(original_path)

import script_modules.subscript.line.CreateLine as line
from script_modules.subprogram.edl_vs_bib_report.file_source.file_source import FILE_SOURCE
from script_modules.subprogram.edl_vs_bib_report.db_sources.db_source import DB_SOURCE
from script_modules.subprogram.edl_vs_bib_report.issue_list.issue_list import ISSUE_LIST
from script_modules.subscript.write.write import WriteData
from pathlib import Path
import os
import pandas as pd
import openpyxl

############################################################################

# starter
line.title_between_line("EDL_VS_BIB Report maker v5.0")

list_of_dfs = []

# fileSource
file_sourceObj = FILE_SOURCE()
file_source = file_sourceObj.file_source()

dateObj = file_source[0]
list_of_dfs.append(file_source[1])

def save_file():
    line.create_simple_line()
    option = input(f"  Do you want file_source_{dateObj.day}{dateObj.convert_month_number_to_word} saved separately? (N/Y)  ")
    if option.lower() == 'y':
        path = f".\output\\EDL_vs_BIB_Reconciliation\\{dateObj.date}\\file_source_{dateObj.day}{dateObj.convert_month_number_to_word}.xlsx"

        # make folder on output if not exist create it
        Path(f".\output\\EDL_vs_BIB_Reconciliation\\{dateObj.date}").mkdir(parents=True, exist_ok=True)

        title = "Write file source to output"
        formula = [    
                    {
                    "DIFF" : '=D$-E$',
                    "Diff %" : '=ABS(IF(E$=0,0,F$/E$))'
                    }
                    ]

        formula_header = [
                            {
                                "Diff %" : '==ABS(AVERAGE(G3:G111)-100%)'
                            }
                            ]

        writeObj = WriteData(dateObj, path, title, formula, formula_header)
        sheet_names = ['File Sources']
        writeObj.write_date_to_excel([file_source[1]], sheet_names)
save_file()

# dbSource
db_sourceObj = DB_SOURCE(dateObj)
db_source = db_sourceObj.db_source()

list_of_dfs.append(db_source)

def save_db():
    line.create_simple_line()
    option = input(f"  Do you want db_source_{dateObj.day}{dateObj.convert_month_number_to_word} saved separately? (N/Y)  ")
    if option.lower() == 'y':
        path = f".\output\\EDL_vs_BIB_Reconciliation\\{dateObj.date}\\db_source_{dateObj.day}{dateObj.convert_month_number_to_word}.xlsx"

        # make folder on output if not exist create it
        Path(f".\output\\EDL_vs_BIB_Reconciliation\\{dateObj.date}").mkdir(parents=True, exist_ok=True)

        title = "Write db source to output"
        formula = [    
                    {
                    "DIFF" : '=E$-F$',
                    "Diff %" : '=ABS(IF(F$=0,0,G$/F$))'
                    }
                    ]

        formula_header = [
                            {
                                "Diff %" : '==ABS(AVERAGE(H3:H111)-100%)'
                            }
                            ]

        writeObj = WriteData(dateObj, path, title, formula, formula_header)
        sheet_names = ['DB Sources']
        writeObj.write_date_to_excel([db_source], sheet_names)
save_db()


# issue list
# issue_listObj = ISSUE_LIST(dateObj)
# issue_listObj.issue_list()





############################################################################

# write
def save_all_to_excell():
    path = f".\output\\EDL_vs_BIB_Reconciliation\\{dateObj.date}\\EDL_vs_BIB_Reconciliation_{dateObj.day}{dateObj.convert_month_number_to_word}.xlsx"

    # make folder on output if not exist create it
    Path(f".\output\\EDL_vs_BIB_Reconciliation\\{dateObj.date}").mkdir(parents=True, exist_ok=True)

    title = f"Write EDL_vs_BIB_Reconciliation_{dateObj.day}{dateObj.convert_month_number_to_word} to output"

    formula = [    
            {
            "DIFF" : '=D$-E$',
            "Diff %" : '=ABS(IF(E$=0,0,F$/E$))'
            },
            {
            "DIFF" : '=E$-F$',
            "Diff %" : '=ABS(IF(F$=0,0,G$/F$))'
            }
            ]

    formula_header = [
                    {
                        "Diff %" : '==ABS(AVERAGE(G3:G111)-100%)'
                    },
                    {
                        "Diff %" : '==ABS(AVERAGE(H3:H111)-100%)'
                    }

                    ]

    writeObj = WriteData(dateObj, path, title, formula, formula_header)
    sheet_names = ['File Sources' ,'DB Sources']
    writeObj.write_date_to_excel(list_of_dfs, sheet_names)
save_all_to_excell()