from Loan_app import isvalid_zipcode
import sys
24103251
if __name__=="__main__": 
    print ('argument list', sys.argv)
    zipcode = input("Enter a valid zipcode:")
    while isvalid_zipcode(zipcode) == False:
        zipcode = input("Enter a valid zipcode:")
    print("given Zipcode:",zipcode)
    
    