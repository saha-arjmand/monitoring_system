# solved python search path for import module in other sub directories
import sys
original_path = "d:\Media\desktop\EDL_VS_BIB Automate"
sys.path.append(original_path)


class EdwQueryCreator():

    def edwquery(self,date, edwlist):

        split = int(input("enter split: "))
        chunks = [edwlist[x:x+split] for x in range(0, len(edwlist), split)]

        x = 1
        for anyItem in chunks:
            print(x)
            print("\n")
            print("SELECT  /*+parallel(5)*/")
            query = ""

            for anything in anyItem:

                
                # remove STG_CDR From label
                label = str(anything).replace("STG_CDR.","")
                if len(anything) < 30:
                    query_tmp = f"( SELECT COUNT(1) FROM {anything} PARTITION for ({date})) AS {label},\n"
                    query += query_tmp
                else:
                    removeCharactor = len(anything) - 30 
                    query_tmp = f"( SELECT COUNT(1) FROM {anything} PARTITION for ({date})) AS {label[:-removeCharactor]},\n"
                    query += query_tmp
            x += 1

        #     query.replace("")
            print(query[:-2])
            print("FROM dual")
            print("\n\n\n")




