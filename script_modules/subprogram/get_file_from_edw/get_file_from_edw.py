# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import script_modules.subscript.line.CreateLine as line
from script_modules.subscript.db.tables.tables import TABLES
from script_modules.subscript.check_file_exist.CheckFile import CheckFile
from script_modules.subscript.date.date import Date
from script_modules.subscript.read_file.read_file import ReadCSV
from script_modules.subscript.read_file.read_file import ReadExcel
import pandas as pd
from datetime import datetime

tblObj = TABLES()
tbl_mapping = tblObj.FILE_FEEDS_EDL_EDW_DRL_MAPPING().to_numpy()

list_of_edl_feeds = []
for anyFeeds in tbl_mapping:
    list_of_edl_feeds.append(anyFeeds[1])

#############################################
line.title_between_line("Get Files From EDW V1.0")

#############################################

feed_name = str(input("  Enter the feed without dataingest schema : ")).upper()

while feed_name not in list_of_edl_feeds:
    feed_name = str(input("  Enter the feed without dataingest schema : ")).upper()
    feed_name = feed_name.strip()


#############################################
dateObj = Date()
dateObj.execute('get_diff_from_edw')
date = dateObj.date
dateInt = int(date)

#############################################

line.create_simple_line()
print("  To start we need to run the following query in EDL side : \n")
print("  %trino(trino)")
print(f"  Select count(1) from dataingest.{feed_name} where cus_date = '{dateInt}'\n")

edl_count_confirm = 'N'
while edl_count_confirm == 'N':
    edl_count = int(input("  Enter the Count of the edl side: "))
    print("\n")
    edl_count_confirm = input(f"  Do you confirm the value of Count ({edl_count})  : (y/n)  ").upper()
    print("\n")

#############################################

line.create_simple_line()
print("  we need to run the following query in EDW side : \n")

for any_edl_feed_name in tbl_mapping:
    if any_edl_feed_name[1] == feed_name:
        edw_table_name = any_edl_feed_name[4]

print(f"  Select /*+parallel(5)*/ count(1) from {edw_table_name} partition for ({dateInt})\n")

edw_count_confirm = 'N'
while edw_count_confirm == 'N':
    edw_count = int(input("  Enter the Count of the edw side: "))
    print("\n")
    edw_count_confirm = input(f"  Do you confirm the value of Count ({edw_count})  : (y/n)  ").upper()
    print("\n")

#############################################

line.create_simple_line()

diff = edl_count - edw_count
print(f"  Diff = edl_count - edw_count = {edl_count} - {edw_count} = {diff}")

if diff < 0 :
    pass
else:
    print("  We have no data issues for this day.")


#############################################

line.title_between_line("Get files not in recon")
print("  we need to run the following query in EDL side : \n")

tbl_regex = tblObj.EDL_RECON_REGEX().to_numpy()

for anyFeed in tbl_regex:
    if anyFeed[1] == feed_name:

            recon_table = anyFeed[2]
            regexlist = list(anyFeed[3].split(" "))

            regex = ''
            if len(regexlist) > 1:
                for anyRegex in regexlist:
                    regex += f"file_name like '{anyRegex}' OR "
                regex = regex[:-3]
                regex = f'({regex})'
            else:
                regex += f"file_name like '{regexlist[0]}'"

            query_na = ''
            query_na += "%trino(trino)\n"
            query_na += "select file_name, record_count diff, 'NA' file_type from(\n"
            query_na += f"select file_name,record_count from dataingest.{recon_table} where cus_file_date ='{dateInt}' and {regex} and  file_name\n"
            query_na += "        NOT in\n"
            query_na += f"    (select cus_file_name from dataingest.{feed_name} where cus_date in ('{dateInt-1}','{dateInt}','{dateInt+1}','{dateInt+2}','{dateInt+3}') and cus_file_date = '{dateInt}' group by cus_file_name)) \n"
            print(query_na)
#############################################

line.create_simple_line()

# check
path_na = f".\\input\\NAFILES{dateInt}.csv"
columns = ['file_name', 'diff', 'file_type']
checkObj = CheckFile(dateObj)
checkObj.is_file_exist(path_na, columns)

# read
readNAObj = ReadCSV(path_na)
readNA= readNAObj.readCsvSheet(['file_name', 'diff', 'file_type'])
print("\n  NA file was read .")

#############################################

line.title_between_line("Get files with have diff")
print("  we need to run the following query in EDL side : \n")

query_diff = ''
query_diff += "%trino(trino)\n"
query_diff += "select file_name, diff, 'DIFF' file_type from (\n"
query_diff += "SELECT \n"
query_diff += "    A.file_name, \n"
query_diff += "    case \n"
query_diff += "        when CAST(A.record_count AS INT) - CAST(B.rocrd_count_table AS INT) = CAST(B.rocrd_count_table AS INT) then -1\n"
query_diff += "         \n"
query_diff += "        ELSE CAST(A.record_count AS INT) - CAST(B.rocrd_count_table AS INT)  \n"
query_diff += "    end as diff\n"
query_diff += "from \n"
query_diff += f"    (SELECT file_name, record_count from dataingest.{recon_table} where cus_date = '{dateInt}' and cus_file_date ='{dateInt}' and {regex}) A \n"
query_diff += "    LEFT JOIN\n"
query_diff += f"    (SELECT cus_file_name, count(1) rocrd_count_table from dataingest.{feed_name} where cus_date in ('{dateInt-1}','{dateInt}','{dateInt+1}', '{dateInt+2}', '{dateInt+3}') and  cus_file_date = '{dateInt}' group by cus_file_name) B\n"
query_diff += "    ON A.file_name = B.cus_file_name \n"
query_diff += "    where CAST(A.record_count AS INT) - CAST(B.rocrd_count_table AS INT) <>0\n"
query_diff += ")"
print(query_diff)

#############################################

line.create_simple_line()

# check
path_diff = f".\\input\\DIFFFILES{dateInt}.csv"
columns = ['file_name', 'diff', 'file_type']
checkObj = CheckFile(dateObj)
checkObj.is_file_exist(path_diff, columns)

# read
readDiffObj = ReadCSV(path_diff)
readDiff= readDiffObj.readCsvSheet(['file_name', 'diff', 'file_type'])
print("\n  Diff file was read .")

#############################################

# merge two files to one df
line.create_simple_line()
frames = [readNA, readDiff]
final_df =  pd.concat(frames)

print("  This information is about the final files: ")
print(f"  Count files : {len(final_df)}")
print(f"  Sum Files : {final_df['diff'].sum()}")
print("\n")
commit = input(f"  The initial_Diff ({abs(diff)}) and the files_Diff ({final_df['diff'].sum()}) are like this. Do you want to continue? (Y/N)  ").lower()

if commit == 'y':
    pass

#############################################

line.title_between_line("Get file DW_FILE_KEY from EDW")
print("  we need to run the following query in EDW side : \n")

# dw_task_id
for anyItem in tbl_mapping:
    if anyItem[1] == feed_name.upper():
        dw_task_id = anyItem[3]

dw_task_id = int(dw_task_id.replace(',',''))

# calc date
today = datetime.today()
targetdate = datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8]))
diff_date = today - targetdate
diff_date_day = diff_date.days

period1 = int(diff_date_day) + 1
period2 = period1 - 15

# files
files_str = ''
for anyFile in final_df.to_numpy():
    str_temp = anyFile[0]
    files_str += f"'{str_temp}', "

files_str = files_str[:-2]

dw_file_key_query = ''
dw_file_key_query += "SELECT /*+parallel(5)*/ DW_FILE_KEY,  TRUNC (REGISTER_DATETIME) AS REGISTER_DATETIME, SUBSTR(SHORT_FILENAME,5,8) AS FILE_DATE, A.SHORT_FILENAME , PROCESSING_STATUS , ROW_COUNT FROM BIB_META.DW_RT_FILES A\n"
dw_file_key_query += f"WHERE DW_TASK_ID = {dw_task_id}\n"
dw_file_key_query += f"AND REGISTER_DATETIME >= TRUNC(SYSDATE-{period1})\n"
dw_file_key_query += f"AND REGISTER_DATETIME <= TRUNC(SYSDATE-{period2})\n"
dw_file_key_query += f"AND SHORT_FILENAME IN ({files_str});"
print(dw_file_key_query)

#############################################
line.create_simple_line()

# check
path_edwfileid= f".\\input\\EdwFileId{dateInt}.xlsx"
columns = ['DW_FILE_KEY', 'REGISTER_DATETIME', 'FILE_DATE', 'SHORT_FILENAME', 'PROCESSING_STATUS', 'ROW_COUNT']
checkObj = CheckFile(dateObj)
checkObj.is_file_exist(path_edwfileid, columns)

# read
# newEdw = newEdwObj.readExcelSheet()
readedwfileidObj = ReadExcel(path_edwfileid)
readedwfileid= readedwfileidObj.readExcelSheet()
print("\n  EdwFileId file was read .")


#############################################

line.title_between_line("Get files from EDW table with DW_FILE_KEY")
print("  we need to run the following query in EDW side : \n")

# get task_name
for anyItem in tbl_mapping:
    if anyItem[1] == feed_name.upper():
        task_name = anyItem[2]

# get column sort
all_file_map = tblObj.ALL_FILE_MAP().to_numpy()



all_column_list = []
for anyfile_map in all_file_map:
    if anyfile_map[1] == task_name :
        all_column_list.append(anyfile_map)
        # all_column += f" {anyfile_map[3]},"

all_column = 'dw_file_id,'

for anyFeed in all_column_list:
    x = 1
    while x <= len(all_column_list) + 1:
    # for anyFeed2 in all_column_list:
        if x == int(anyFeed[2]):
            all_column += f" {anyFeed[3]},"
        x += 1

all_column = all_column[:-1]

# get edw table
for anyItem in tbl_mapping:
    if anyItem[1] == feed_name.upper():
        edw_table = anyItem[4]


# get dw_file_id
dw_file_ids = ''
for anything in readedwfileid.to_numpy():
    dw_file_ids += f"{anything[0]}, "

dw_file_ids = dw_file_ids[:-2]

edw_query = ''
edw_query += "select /*+parallel(5)*/ \n"
edw_query += f"{all_column}\n"
edw_query += f"from {edw_table} partition for ({date}) where DW_FILE_ID IN ({dw_file_ids} ) "
print(edw_query)

#############################################
line.create_simple_line()

# check
path_edwfiles= f".\\input\\EdwFiles{dateInt}.dat"
checkObj = CheckFile(dateObj)
checkObj.is_file_exist(path_edwfiles, columns)

# header
df_header = pd.read_csv(path_edwfiles, header=None, sep='\s\s+', engine='python')
df_header_np = df_header.to_numpy()

z = 0
while z < 1:
    header = df_header_np[z][0]
    z += 1

# read
df = pd.read_csv(path_edwfiles, header=0, sep='\s\s+', engine='python')
df_np = df.to_numpy()

x = 0
set_files = set()
for anyItem in df_np:
    li_files = df_np[x][0].split("|")
    set_files.add(li_files[0])
    x+= 1



for anyItem in set_files:
    file_str = ''

    for anyItem2 in readedwfileid.to_numpy():
        if int(anyItem) == int(anyItem2[0]):
            file_name = anyItem2[3]

    path_final = f".\output\\{file_name}"

    with open(path_final , 'a') as file:  
        file.write(f"{header}\n")

    for anyItem3 in df_np:
        li_files = anyItem3[0].split("|")
        if li_files[0] == anyItem:
            with open(path_final , 'a') as file:  
                file.write(f"{anyItem3[0]}\n")

    
    print("  The process is completed.")