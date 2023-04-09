# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

from script_modules.subscript.math_calc.mathcalc import MathCalc


############################################################################

class CALCULATION:

    def __init__(self, db_source, dateObj) :
        self.db_source = db_source
        self.dateObj = dateObj

    def subtraction(self):

        subObj = MathCalc(self.db_source, self.dateObj, "db")
        diff = subObj.subtraction("EDL_RECORD_COUNT", "BIB_RECORD_COUNT")
        return diff


    def percentage(self, df):

        percent = MathCalc(df, self.dateObj, "db")
        percent = percent.percentage()
        return percent

    
    def total_percentage(self, df):
        
        totalPercentObj = MathCalc(df, self.dateObj, "db")
        totalPercent = totalPercentObj.total_percentage()
        return totalPercent

    def executive(self):

        calc_diff = self.subtraction()

        calc_percent = self.percentage(calc_diff)

        total_percent = self.total_percentage(calc_percent)

        return total_percent




