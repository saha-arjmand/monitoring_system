# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import pandas as pd
from datetime import datetime

############################################################################

class EDL_QueryMaker():

    def __init__(self, dateObj, null_list_with_date) :
        self.dateObj = dateObj
        self.null_list_with_date = null_list_with_date
        

    def file_edl_null_query(self):

        print("%trino(trino)\nSELECT")
        x = 0
        count = 0
        quary = ''
        ls = []

        # convert null df list to null numpy list
        nulldf = pd.DataFrame(self.null_list_with_date)
        nullfeeds_np = nulldf.to_numpy()

        while x < len(nullfeeds_np):

            # if date format (cus_date,time_key) not nan
            if str(nullfeeds_np[x][1]) != 'nan':
                # nullfeed_np[x][0] = feed_name

                if nullfeeds_np[x][0] == 'TZ_FILE_SRC_BigData_Site_Hourly' or nullfeeds_np[x][0] == 'TZ_FILE_SRC_BigData_Site_Daily':
                    ls.append(nullfeeds_np[x][0])

                    # print feed name comment
                    comment = f"--{count+1}\n"
                    quary += comment

                    query_tmp = f"( SELECT COUNT(1) FROM dataingest.{nullfeeds_np[x][0]} where {nullfeeds_np[x][1]} ='{self.dateObj.year_file}-{self.dateObj.month_file}-{self.dateObj.day_file}') AS {nullfeeds_np[x][0]},\n"
                    quary += query_tmp
                    count += 1

                elif nullfeeds_np[x][1] == 'cus_date':
                    ls.append(nullfeeds_np[x][0])

                    # print feed name comment
                    comment = f"--{count+1}\n"
                    quary += comment

                    query_tmp = f"( SELECT COUNT(1) FROM dataingest.{nullfeeds_np[x][0]} where {nullfeeds_np[x][1]} = '{self.dateObj.file_date}') AS {nullfeeds_np[x][0]},\n"
                    quary += query_tmp
                    count += 1

                elif nullfeeds_np[x][1] == 'time_key':
                    ls.append(nullfeeds_np[x][0])

                    # print feed name comment
                    comment = f"--{count+1}\n"
                    quary += comment

                    query_tmp = f"( SELECT COUNT(1) FROM dataingest.{nullfeeds_np[x][0]} where {nullfeeds_np[x][1]} = '{self.dateObj.year_file}-{self.dateObj.month_file}-{self.dateObj.day_file}') AS {nullfeeds_np[x][0]},\n"
                    quary += query_tmp
                    count += 1

            x += 1

        quary = quary[:-2]
        print(quary)
        print("\n")


    def db_edl_null_query(self):

        print("%trino(trino)\nSELECT")
        x = 0
        count = 0
        quary = ''
        ls = []

        # convert null df list to null numpy list
        nulldf = pd.DataFrame(self.null_list_with_date)
        nullfeeds_np = nulldf.to_numpy()

        while x < len(nullfeeds_np):

            # if date format (cus_date,time_key) not nan
            if str(nullfeeds_np[x][1]) != 'no':
                # nullfeed_np[x][0] = feed_name

                if nullfeeds_np[x][1] == 'cus_date':
                    if nullfeeds_np[x][2] == 'YYYY-MM-DD':
                        ls.append(nullfeeds_np[x][0])

                        # print feed name comment
                        comment = f"--{count+1}\n"
                        quary += comment

                        query_tmp = f"( SELECT COUNT(1) FROM dataingest.{nullfeeds_np[x][0]} where {nullfeeds_np[x][1]} = '{self.dateObj.year}-{self.dateObj.month}-{self.dateObj.day}') AS {nullfeeds_np[x][0]},\n"
                        quary += query_tmp
                        count += 1
            
            elif str(nullfeeds_np[x][1]) == 'no':

                ls.append(nullfeeds_np[x][0])

                # print feed name comment
                comment = f"--{count+1}\n"
                quary += comment

                query_tmp = f"( SELECT COUNT(1) FROM dataingest.{nullfeeds_np[x][0]}) AS {nullfeeds_np[x][0]},\n"
                quary += query_tmp
                count += 1

            x += 1

        quary = quary[:-2]
        print(quary)
        print("\n")


class EDW_QueryMaker():

    def __init__(self, dateObj, null_list_with_date) :
        self.dateObj = dateObj
        self.null_list_with_date = null_list_with_date
        

    def file_edw_null_query(self):

        x = 0
        quary = ''
        count = 0

        # edw_nullfeeds_np[x][0] : EDL_FEED_NAME
        # edw_nullfeeds_np[x][1] : edw_table_name
        # edw_nullfeeds_np[x][2] : partition (YES/NO)
        # edw_nullfeeds_np[x][3] : partition_format (YYYYMMDD)


        # convert null df list to null numpy list
        nullfeeds_np = self.null_list_with_date.to_numpy()

        print("SELECT  /*+parallel(5)*/")
        while x < len(nullfeeds_np):
            if str(nullfeeds_np[x][2]) == 'YES':
                comment = f"--{count+1} {nullfeeds_np[x][0]}\n"
                # print("not nan", " - ", "YES PARTITION", " - ", comment)
                quary += comment
                count += 1

                # Oracle does not accept titles longer than 30 characters
                title = nullfeeds_np[x][0]
                if len(title) > 30:
                    std_len = int(len(title) - 30)
                    title = title[std_len:]
                    if title[0] == '_':
                        std_len += 1
                        title = title[std_len:]
                query_tmp = f"(SELECT COUNT(1) FROM {nullfeeds_np[x][1]} PARTITION for ({self.dateObj.year_file}{self.dateObj.month_file}{self.dateObj.day_file})) AS {title},\n"
                quary += query_tmp

            elif str(nullfeeds_np[x][2]) == 'NO':
                comment = f"--{count+1} {nullfeeds_np[x][0]}\n"
            #     # print("not nan", " - ", "NO PARTITION", " - ", comment)
                quary += comment
                count += 1

                # Oracle does not accept titles longer than 30 characters
                title = nullfeeds_np[x][0]
                if len(title) > 30:
                    std_len = int(len(title) - 30)
                    title = title[std_len:]
                    if title[0] == '_':
                        std_len += 1
                        title = title[std_len:]
                query_tmp = f"(SELECT COUNT(1) FROM {nullfeeds_np[x][1]}) AS {title},\n"
                quary += query_tmp

            x += 1

        quary = quary[:-2]
        print(quary)
        print("FROM dual")
        print("\n")

    
    def db_edw_null_query(self):

        x = 0
        quary = ''
        count = 0

        # edw_nullfeeds_np[x][0] : EDL_FEED_NAME
        # edw_nullfeeds_np[x][1] : edw_table_name
        # edw_nullfeeds_np[x][2] : partition (YES/NO)
        # edw_nullfeeds_np[x][3] : partition_format (YYYYMMDD)


        # convert null df list to null numpy list
        nullfeeds_np = self.null_list_with_date.to_numpy()

        print("SELECT  /*+parallel(5)*/")
        while x < len(nullfeeds_np):
            if str(nullfeeds_np[x][2]) == 'YES':
                comment = f"--{count+1} {nullfeeds_np[x][0]}\n"
                # print("not nan", " - ", "YES PARTITION", " - ", comment)
                quary += comment
                count += 1

                # Oracle does not accept titles longer than 30 characters
                title = nullfeeds_np[x][0]
                if len(title) > 30:
                    std_len = int(len(title) - 30)
                    title = title[std_len:]
                    if title[0] == '_':
                        std_len += 1
                        title = title[std_len:]
                query_tmp = f"(SELECT COUNT(1) FROM {nullfeeds_np[x][1]} PARTITION for ({self.dateObj.year}{self.dateObj.month}{self.dateObj.day})) AS {title},\n"
                quary += query_tmp

            elif str(nullfeeds_np[x][2]) == 'NO':
                comment = f"--{count+1} {nullfeeds_np[x][0]}\n"
            #     # print("not nan", " - ", "NO PARTITION", " - ", comment)
                quary += comment
                count += 1

                # Oracle does not accept titles longer than 30 characters
                title = nullfeeds_np[x][0]
                if len(title) > 30:
                    std_len = int(len(title) - 30)
                    title = title[std_len:]
                    if title[0] == '_':
                        std_len += 1
                        title = title[std_len:]
                query_tmp = f"(SELECT COUNT(1) FROM {nullfeeds_np[x][1]}) AS {title},\n"
                quary += query_tmp

            x += 1

        quary = quary[:-2]
        print(quary)
        print("FROM dual")
        print("\n")


    def db_edw_recheck_query(self):
        x = 0
        quary = ''
        count = 0

        nowTime = datetime.today()
        reconDate_tmp = f"{self.dateObj.year}{self.dateObj.month}{self.dateObj.day}"
        reconDate = datetime(int(reconDate_tmp[0:4]), int(reconDate_tmp[4:6]), int(reconDate_tmp[6:8]))
        minus_days = int(abs(nowTime - reconDate).days)

        # convert null df list to null numpy list
        recheckfeeds_np = self.null_list_with_date.to_numpy()

        print("SELECT  /*+parallel(10)*/")
        while x < len(recheckfeeds_np):
            comment = f"--{count+1} {recheckfeeds_np[x][0]}\n"
            quary += comment
            count += 1

            # Oracle does not accept titles longer than 30 characters
            title = recheckfeeds_np[x][0]
            if len(title) > 30:
                std_len = int(len(title) - 30)
                title = title[std_len:]
                if title[0] == '_':
                    std_len += 1
                    title = title[std_len:]

            query_tmp = f"(SELECT MAX(A.ROWS_LOADED) FROM BIB_META.DW_RT_RUNS A WHERE START_DATE>TRUNC(SYSDATE-{minus_days}) AND START_DATE<TRUNC (SYSDATE -{minus_days-1})"\
                            f"AND DW_TASK_ID IN ( SELECT DW_TASK_ID FROM BIB_META.DW_TASKS WHERE TASK LIKE upper('%{recheckfeeds_np[x][1]}%'))) AS {title},\n\n"
            quary += query_tmp

            x += 1

        quary = quary[:-3]
        print(quary)
        print("FROM dual")
        print("\n")
