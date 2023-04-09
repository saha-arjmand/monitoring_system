# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import script_modules.subscript.line.CreateLine as line
############################################################################

class REPAIR:

    def __init__(self, db_source) :
        self.db_source = db_source


    def need_repair_query(self):
        
        df_np = self.db_source.to_numpy()

        query = "%jdbc(hive)\n"
        x = 1
        for anyItem in df_np :
         if float(anyItem[6]) == 1.00:
            query_tmp = f"MSCK REPAIR TABLE dataingest.{anyItem[1]} SYNC PARTITIONS;\n\n"
            query += query_tmp
            x += 1

        print(query[:-2])


    def executive(self):

        line.title_between_line("Repair tables")
        
        while True:
            print("\n  These feeds need repair .\n")
            need_repair = self.need_repair_query()
            print("\n")
            
            isRepaired = input("  Are the above tables repaired ?  (Y/N) ")
            if isRepaired.lower() == 'y':
                break

