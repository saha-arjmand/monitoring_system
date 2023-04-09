
import script_modules.subscript.line.CreateLine as line



while True:
    line.title_between_line("data ingestion monitoring")

    print("  1. EDL_vs_BIB_Reconciliation (manual)\n")
    print("  2. EDL_vs_BIB_Reconciliation (Automatic)\n")
    print("  3. Getting files from the EDW side\n")
    print("  4. SQL Mode\n")
    print("  0. Setting\n")

    print("\n")
    option = input("  Please select one of the options above : ")
    print("\n")

    if option == "1":
        
        import script_modules.subprogram.edl_vs_bib_report.edl_vs_bib_report

    elif option == "2":
        print("  Error!\n  This section is currently unavailable !\n")

    elif option == "3":
        import script_modules.subprogram.get_file_from_edw.get_file_from_edw

    elif option == "4":
        from script_modules.subscript.db.sql_mode.sql_mode import SQLMode
        sqlMode = SQLMode()
        sqlMode.sqlMode()
        break

    elif option == "0":
        print(" 1. Clear caches")
        option = input("  Please select one of the options setting : ")

        if option == "1":
            import setting.clear_cache


    else:
        print("please select a valid option")



