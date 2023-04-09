# solved python search path for import module in other sub directories
import sys
import os
current_working_directory = os.getcwd()
sys.path.append(current_working_directory)

from datetime import datetime, timedelta

############################################################################

class Date(object):
    
    # initialize ###########################################
    def __init__(self):
         self._input_date = None

    @property
    def date(self):
        # getter
        return self._input_date

    def isdate(self):
        # Validate input date
        if len(self._input_date) == 8 :
            return True
        else:
            return False

    # current date ###########################################
    @property
    def day(self):
        day = self._input_date[6:8]
        return day

    @property
    def month(self):
        month = self._input_date[4:6]
        return month

    @property
    def year(self):
        month = self._input_date[0:4]
        return month
    
    # word date ###########################################
    @property
    def convert_month_number_to_word(self):
        if self.month == '01':
            date_month_word = 'Jan'
            return date_month_word
        elif self.month == '02':
            date_month_word = 'Feb'
            return date_month_word
        elif self.month == '03':
            date_month_word = 'Mar'
            return date_month_word
        elif self.month == '04':
            date_month_word = 'Apr'
            return date_month_word
        elif self.month == '05':
            date_month_word = 'May'
            return date_month_word
        elif self.month == '06':
            date_month_word = 'Jun'
            return date_month_word
        elif self.month == '07':
            date_month_word = 'Jul'
            return date_month_word
        elif self.month == '08':
            date_month_word = 'Agu'
            return date_month_word
        elif self.month == '09':
            date_month_word = 'Sep'
            return date_month_word
        elif self.month == '10':
            date_month_word = 'Oct'
            return date_month_word
        elif self.month == '11':
            date_month_word = 'Nov'
            return date_month_word
        elif self.month == '12':
            date_month_word = 'Dec'
            return date_month_word
        else:
            error = "  month is wrong !!\n"
            return error

    # file date ###########################################
    @property
    def file_date(self):
        std_date = datetime.strptime(f"{self.year}-{self.month}-{self.day}", "%Y-%m-%d")
        file_date = (std_date - timedelta(2)).strftime('%Y%m%d')
        return (file_date)

    @property
    def day_file(self):
        day = self.file_date[6:8]
        return day
    
    @property
    def month_file(self):
        month = self.file_date[4:6]
        return month

    @property
    def year_file(self):
        year = self.file_date[0:4]
        return year

    def execute(self, type):

        # this code get date like setter and execute code
        while True:
        # Get Date
            self._input_date = input("  Enter date (YYYYMMDD): ")

            if self.isdate() == True:

                if type == 'edl_vs_bib_report':
                    print(f"\n  db date is   : {self.date}   or   {self.day} {self.convert_month_number_to_word}")
                    print(f"  file date is : {self.file_date}\n")
                else:
                    print(f"\n  date is   : {self.date}")
                iscorrect = input("  Is the date correct ? (Y/N)  ").lower()

                if iscorrect == 'y':
                    break
                else:
                    print("  the (db & file) date is wrong! please start againg.")
                    print("  Try again.\n")
            else:
                print("  The date format is wrong! \n")


############################################################################

if __name__ == "__main__":

    dateObj = Date()
    dateObj.execute('edl_vs_bib_report')
    print(dateObj.date)
    print(dateObj.month)
    print(dateObj.year)
    print(dateObj.convert_month_number_to_word)
    print(f"file date is : {dateObj.file_date}")
    print(dateObj.convert_month_number_to_word)
        

