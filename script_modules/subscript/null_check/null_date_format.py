# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

import pandas as pd
############################################################################

# class EDL_QueryMaker():

#     def __init__(self, date, null_list_with_date, original_null_list) :
#         self.date = date
#         self.null_list_with_date = null_list_with_date
#         self.original_null_list = original_null_list
        

#     @property
#     def edl_feedlist_not_exist_in_query(self):
            
#         ls = []

#         notinlistdf = pd.DataFrame(self.original_null_list)
#         notinlistdf_np = notinlistdf.to_numpy()
        
#         i = 0
#         while i < len(notinlistdf_np):
#             if notinlistdf_np[i][0] in self.edl_feedlist_exist_in_query:
#                 pass
#             else:
#                 ls.append(notinlistdf_np[i][0]) 
#             i += 1
        
        # return ls