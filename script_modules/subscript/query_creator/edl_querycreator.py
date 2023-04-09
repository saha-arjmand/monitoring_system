# solved python search path for import module in other sub directories
import sys
original_path = "d:\Media\desktop\EDL_VS_BIB Automate"
sys.path.append(original_path)


class CreateQuery():

    def edlQuery(self, date, feedlist):

        print("%trino(trino)")
        print("select")
        query = ""
        for anyItem in feedlist:
            query_tmp = f"( SELECT COUNT(1) FROM   dataingest.{anyItem} where cus_date = '{date}') AS {anyItem},\n"
            query += query_tmp

        print(query[:-2])

