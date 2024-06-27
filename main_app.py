from Loan_app import *
import sys
if __name__=="__main__": 
    print ('argument list', sys.argv)
    # load JSON into my sql database
    load_database()
    # get zipcode as user input
    zipcode = input("Enter a valid zipcode:")
    while isvalid_zipcode(zipcode) == False:
        # if invalid zipcode prompt user to re-enter valid zipcode
        zipcode = input("Enter zipcode as 5 digit integer format:")
        print("given Zipcode:",zipcode)
        # get month MM as user input
    month=int(input("Enter month as 2 digit integer MM format :"))
    while isvalid_month(month) == False:
        # if invalid month ask user to enter correct month number again
        month = int(input("Enter month in number format from 1 to 12:"))
        # get month MM as user input
    year=input("Enter year in YYYY format:")
    while isvalid_year(year) == False:
        # if invalid year ask user to enter year as 4 digit integer YYYY format
        year = input("Enter year in YYYY format:")
    #ls_transaction(zipcode,month,year)
    