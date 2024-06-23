# import required module
import os
import sys
import re

# help function
def help():
    sa = """ Usage:- 
    $ ./loan_app validate_zipcode   # Validate entered zipcode is in valid format
    $ ./loan_app validate_monthyear # Validate entered Month & year are in valid format
    $ ./loan_app ls_trans           # List all the transactions made by customer in specific zipcode for the given month and year
    $ ./loan_app sort               # Sort the list of transaction in descending order """
    sys.stdout.buffer.write(sa.encode('utf8'))

# Validate entered zipcode is in valid format
def isvalid_zipcode(z):
    pattern = r"^\d{5}$"
    match = re.match(pattern,z)
    if match: 
        print("Valid Zipcode")
        return True
    else: 
        print("Please enter the valid 5-digit zip code format")  
        return False 
    
# Validate entered month is in valid format(1-12)
def isvalid_month(month):
    if month >=1 and month <= 12:
        print("valid Month")
        return True
    else:
        print("Invalid Month")
        return False
    
# Validate entered year is in valid YYYY format
def isvalid_year(year):
    if len(year)==4 and year.isdigit()==True:
        print("valid year")
        return True
    else:
        print("Invalid year")
        return False

