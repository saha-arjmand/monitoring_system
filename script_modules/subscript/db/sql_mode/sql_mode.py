# solved python search path for import module in other sub directories
import sys
import os
from numpy import piecewise
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import sqlite3
import script_modules.subscript.line.CreateLine as line
import script_modules.subscript.db.sqlite_help.sqlite_help as sqliteHelp
from script_modules.subscript.db.tables.tables import TABLES

############################################################################


class SQLMode:

    def sqlMode(self):
        line.create_title_line("SQL Mode")


        while True:
            conn = sqlite3.connect('Ingestion_monitoring.db')
            c = conn.cursor()
            query = input("  SQL>> ")

            if query.lower() == "exit":
                import main
                break

            if query.lower() == "commit":
                break
            
            elif query.lower() == "help":
                sqliteHelp.sqlite_help()

            elif query.lower() == "tables":
                c.execute("select * from sqlite_master where type='table';")
                tables = c.fetchall()
                for table in tables:
                    print(f"\ntable name : {table[1]}")
                    print("Structure: ")
                    print(table[4])
                    print("\n")

            # show special tables
            elif "show" in query.lower() :
                table_name = query.split(" ")[1]

                # If you want to remove leading and ending spaces, use str.strip()
                table_name = table_name.strip()

                try:
                    tablesObj = TABLES()
                    print(tablesObj.show_table(table_name))
                except:
                    print("  There is no table with this name !")

            else:
                try:
                    c.execute(query)
                    show = c.fetchall()
                    print("\n")
                    print(show)
                    print("\n")
                    conn.commit()
                    conn.close()
                except:
                    print("\n")
                    print("  The desired query has encountered an error !\n")