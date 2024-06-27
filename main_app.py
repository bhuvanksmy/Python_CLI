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

    ls_transaction(zipcode,month,year)
    # to get existing account details of a customer
    # prompt user to input Credit card #, first name & last 4 digit of SSN
    customer_credit_card_no,last_four_digit_SSN,first_name = input("Enter Customer Credit Card NO & last_four_digit_SSN & first_name:").split()

    get_existing_acc_details(customer_credit_card_no,last_four_digit_SSN)
    updated_first_name = None
    updated_last_name = None
    updated_email = None
    updated_phone = None
    
    is_update_required = input("do you want to modify the existing account details of a customer(YES/NO):")
    if is_update_required == 'YES':
        update_column = int(input("What customer detail you want to update:(1.FIRST_NAME 2.LAST_NAME 3.CUST_EMAIL 4.CUST_PHONE 5.Done)?"))
        while update_column != 5:
            if update_column == 1:
                updated_first_name = input("Enter updated value for the First Name:")
                print(updated_first_name)
            elif update_column == 2:
                updated_last_name = input("Enter updated value for the Last Name:")
                print(updated_last_name)
            elif update_column == 3:
                updated_email = input("Enter updated value for the email:")
                print(updated_email) 
            elif update_column == 4:
                updated_phone = input("Enter updated value for the phone number:")
                print(updated_phone) 
            elif update_column == 5:
                print("Done. Got all the new values to update for the existing customer")
            else:
                print("Invalid Input")
            update_column = int(input("What customer detail you want to update:(1.FIRST_NAME 2.LAST_NAME 3.CUST_PHONE 4.CUST_EMAIL 5.Done)?"))
        # Calling the update_into_table function to update the new values for the existing customer
        update_into_table(customer_credit_card_no,first_name, last_four_digit_SSN, updated_first_name,updated_last_name,updated_email,updated_phone)

        # generate a monthly bill for a credit card number for a given month and year
        credit_card_no = input("Enter 16-digit credit card no:")
        month=int(input("Enter month as 2 digit integer MM format :"))
        while isvalid_month(month) == False:
        # if invalid month ask user to enter correct month number again
            month = int(input("Enter month in number format from 1 to 12:"))
        # get month MM as user input

        year=input("Enter year in YYYY format:")
        while isvalid_year(year) == False:
        # if invalid year ask user to enter year as 4 digit integer YYYY format
            year = input("Enter year as 2018 in YYYY format:")
    get_monthly_bill(credit_card_no,month,year)
    



    


    