# solved python search path for import module in other sub directories
import sys
import os
from numpy import insert, piecewise
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

############################################################################


def sqlite_help():
    help = ""

    # header
    header = "\n"
    header += "  SQLite Help :"
    header += "\n"
    header += "\n"

    # data type
    data_type = "\n"
    data_type += "  data_type"
    data_type += "\n"
    data_type += "  sqllite have 5 dataType : NULL, INTEGER, REAL, TEXT, BLOB"
    data_type += "\n"
    data_type += "  REAL : The values is a floating point value, stored as an 8-byte IEEE floating point number."
    data_type += "\n"
    data_type += "  BLOB : The value is a blob of data, stored exactly as it was input."
    data_type += "\n"
    data_type += "\n"

    # Show tables
    show_tables = "\n"
    show_tables += "  show all tables with structures"
    show_tables += "\n"
    show_tables += "  tables"
    show_tables += "\n"
    show_tables += "\n"



    # Select
    select = "\n"
    select += "  Select"
    select += "\n"
    select += "  SELECT * FROM TableName"
    select += "\n"
    select += '  SELECT * FROM employees WHERE last=?", ("Schafer",)'
    select += "\n"
    select += '  SELECT * FROM employees WHERE last=:last", {"last": "Doe"}'
    select += "\n"
    select += "\n"

    # show all tables
    all_tables = "\n"
    all_tables += "  show all tables"
    all_tables += "\n"
    all_tables += "  SELECT * FROM sqlite_master where type='table';"
    all_tables += "\n"
    all_tables += "\n"

    # show special table
    special_table = "\n"
    special_table += "  show a special table"
    special_table += "\n"
    special_table += "  show tableName"
    special_table += "\n"
    special_table += "  exp : show EDL_DATE_FORMAT"
    special_table += "\n"
    special_table += "\n"


    # Insert
    insert_db = "\n"
    insert_db += "  Insert"
    insert_db += "\n"
    insert_db += "  Suggested format :"
    insert_db += "\n"
    insert_db += '  INSERT INTO people (first_name, last_name) VALUES ("John", "Smith");'
    insert_db += "\n"
    insert_db += "  Other format :"
    insert_db += "\n"
    insert_db += "  INSERT INTO EDL_DATE_FORMAT VALUES (1, 'feedName', 'cus_date', 'YYYYMMDD', 'file')"
    insert_db += "\n"
    insert_db += "  insert data with python class :"
    insert_db += "\n"
    insert_db += "  emp_1 = Employee('John', 'Doe', 80000)"
    insert_db += "\n"
    insert_db += '  "INSERT INTO employees VALUES (?, ?, ?)", (emp_1.first, emp_1.last, emp_1.pay)'
    insert_db += "\n"
    insert_db += '  "INSERT INTO employees VALUES (:first, :last, :pay)", "{"first": emp_1.first, "last": emp_1.last, "pay": emp_1.pay}'
    insert_db += "\n"
    insert_db += "\n"

    # Update
    update_db = "\n"
    update_db += "  Update"
    update_db += "\n"
    update_db += "  UPDATE table_name SET column_name = 'something' WHERE condition;"
    update_db += "\n"
    update_db += "  UPDATE employeesSET lastname = 'Smith' WHERE employeeid = 3;"
    update_db += "\n"
    update_db += "\n"

    # delete
    delete_db = "\n"
    delete_db += "  delete"
    delete_db += "\n"
    delete_db += "  drop table main.tableName"
    delete_db += "\n"
    delete_db += "\n"



    help += header
    help += data_type
    help += show_tables
    help += special_table
    help += select
    help += all_tables
    help += insert_db
    help += update_db
    help += delete_db

    print(help)


############################################################################

if __name__ == "__main__":
    sqlite_help()