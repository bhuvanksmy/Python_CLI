# import required module
import re
import sys

import mysql.connector as dbconnect
import pandas as pd
import requests
from mysql.connector import Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import make_date, expr, date_format
from tabulate import tabulate
import calendar

# pd.set_option('display.max_columns', None)
# pd.set_option('max_colwidth', None)

# Define MySQL connection properties

mysql_props = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# JDBC URL for MySQL to create new table cdw_sapp_customer and loading data into the table
mysql_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
conn = dbconnect.connect(host='localhost', database='creditcard_capstone', user='root', password='password',
                         port='3306')

# getting zipcode from the user
def get_valid_zipcode():
    zipcode = input("Enter a valid zipcode:")
    while not isvalid_zipcode(zipcode):# calling the isvalid_zipcode() to validate zipcode 
        # if invalid zipcode prompt user to re-enter valid zipcode
        zipcode = input("Enter zipcode as 5 digit integer format:")
        print("given Zipcode:", zipcode)
    return zipcode

# getting month from the user
def get_valid_month():
    month = int(input("Enter month as 2 digit integer MM format :"))
    while not isvalid_month(month):# calling the isvalid_month() to validate month
        # if invalid month ask user to enter correct month number again
        month = int(input("Enter month in number format from 1 to 12:"))
    return month

# # getting year from the user
def get_valid_year():
    year = input("Enter year in YYYY format:")
    while not isvalid_year(year): # calling the isvalid_year() to validate year
        # if invalid year ask user to enter year as 4 digit integer YYYY format
        year = input("Enter year in YYYY format:")
    return year


# Validate entered zipcode is in correct format
def isvalid_zipcode(z):
    pattern = r"^\d{5}$"
    match = re.match(pattern, z)
    if match:
        print("Valid Zipcode")
        return True
    else:
        print("Please enter the valid 5-digit zip code format")
        return False
    

# Validate entered month is in valid format(1-12)
def isvalid_month(month):
    if month >= 1 and month <= 12:
        print("valid Month")
        return True
    else:
        print("Invalid Month")
        return False


# Validate entered year is in valid YYYY format
def isvalid_year(year):
    if len(year) == 4 and year.isdigit() == True and year ==  '2018':
        print("valid year")
        return True
    else:
        print("Invalid year")
        return False


def load_json_data_to_database():
    print("Loading the Credit Card Data & Loan Application Data into MySQL Database")
    # create the SparkSession
    spark = SparkSession.builder.appName('Capstone_Project').getOrCreate()
    # Reading JSON Customer data 
    df_customer = spark.read.option("multiline", "true").json("resources/cdw_sapp_customer.json")
    # Manipulating customer data according to the specifications in mapping document.
    df_customer = df_customer.select("SSN", expr("initcap(FIRST_NAME)").alias("FIRST_NAME"),
                                     expr("lower(MIDDLE_NAME)").alias("MIDDLE_NAME"),
                                     expr("initcap(LAST_NAME)").alias("LAST_NAME"), "CREDIT_CARD_NO"
                                     , expr("APT_NO || ',' || STREET_NAME").alias("FULL_STREET_ADDRESS"), "CUST_CITY",
                                     "CUST_STATE", "CUST_COUNTRY", "CUST_ZIP", expr(
            "'(' || substr(CUST_PHONE,0,3) || ')' || substr(CUST_PHONE,4,3) || '-' || substr(CUST_PHONE,7,1) || substr(CUST_PHONE,2,3)").alias(
            "CUST_PHONE"), "CUST_EMAIL", "LAST_UPDATED")
    # Reading JSON Branch Data
    df_branch = spark.read.option("multiline", "true").json("resources/cdw_sapp_branch.json")
    # Manipulating branch data according to the specifications in mapping document.
    df_branch = df_branch.select("BRANCH_CODE", "BRANCH_NAME", "BRANCH_STREET", "BRANCH_CITY", "BRANCH_STATE",
                                 expr("CASE WHEN BRANCH_ZIP IS NULL  THEN '99999' ELSE BRANCH_ZIP END").alias(
                                     "BRANCH_ZIP"), expr(
            "'(' || substr(BRANCH_PHONE,0,3) || ')' || substr(BRANCH_PHONE,4,3) || '-' || substr(BRANCH_PHONE,7,3)").alias(
            "BRANCH_PHONE"), "LAST_UPDATED")
    # Reading JSON Credit_card data 
    df_creditcard = spark.read.option("multiline", "true").json("resources/cdw_sapp_credit.json")
    # Manipulating Credit card data according to the specifications in mapping document.
    df_creditcard = df_creditcard.select("CREDIT_CARD_NO", date_format(
        make_date(df_creditcard.YEAR, df_creditcard.MONTH, df_creditcard.DAY), "yyyyMMdd").alias("TIMEID"), "CUST_SSN",
                                         "BRANCH_CODE", "TRANSACTION_TYPE", "TRANSACTION_VALUE", "TRANSACTION_ID")

    # API Endpoint URL
    api_url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

    # req 4.1 Fetch data from the above API endpoint for the loan application dataset
    response = requests.get(api_url)

    # req 4.2 status code of the above API endpoint.
    print("API Status Code : " + str(response.status_code))

    if response.status_code == 200:
        # Convert JSON response to DataFrame
        json_df = spark.read.json(spark.sparkContext.parallelize([response.json()]))

    df_customer.write \
        .jdbc(url=mysql_url, table="cdw_sapp_customer", mode="overwrite", properties=mysql_props)
    df_branch.write \
        .jdbc(url=mysql_url, table="cdw_sapp_branch", mode="overwrite", properties=mysql_props)
    df_creditcard.write \
        .jdbc(url=mysql_url, table="cdw_sapp_credit_card", mode="overwrite", properties=mysql_props)
    json_df.write \
        .jdbc(url=mysql_url, table="CDW_SAPP_loan_application", mode="overwrite", properties=mysql_props)
    # Stop SparkSession
    spark.stop()
    print("Successfully loaded the data into Database")
    # get list of transactions made by the customers for the specified zip code,month and year.

# method to get the list of transactions made by the customers for the specific zipcode for given month and year.
def get_transaction():
    # get zipcode as user input
    zipcode = get_valid_zipcode()

    # get month MM as user input
    month = get_valid_month()

    year = get_valid_year()
    # checking the connection established successfully
    if conn.is_connected():
        print(' ')
    else:
        conn.connect()
    mycursor = conn.cursor()
    #Query to get the list of transactions from Mysql Database.
    query = ("SELECT cr.TRANSACTION_ID,cr.CREDIT_CARD_NO, cr.TIMEID as TRANSACTION_DATE,"
             "cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE FROM cdw_sapp_credit_card AS cr INNER JOIN cdw_sapp_customer "
             "cu ON cr.CREDIT_CARD_NO = cu.CREDIT_CARD_NO AND cr.CUST_SSN = cu.SSN WHERE SUBSTRING(TIMEID, 1, "
             "4) = ") + str(
        year) + " AND SUBSTRING(TIMEID, 5, 2) = " + str(month) + " AND cu.cust_zip =" + str(
        zipcode) + " order by TIMEID desc"
   
    mycursor.execute(query)
    result = mycursor.fetchall();  # fetch all the values from the mysql database
    # Convert to Pandas Dataframe
    df = pd.DataFrame(result)
    print("--------------------------------------------------------------------------------------------------------")
    print("List of Transactions made in the zipcode  "+ str(zipcode) +" for "+calendar.month_name[month]+" "+str(year))
   
    df.columns = ['TRANSACTION_ID','CREDIT_CARD_NO', 'TRANSACTION_DATE', 
                  'TRANSACTION_TYPE', 'TRANSACTION_VALUE']
    # Display the Pandas Dataframe
    
    # Display the pandas dataframe in Table format
    print(tabulate(df, headers = 'keys', tablefmt = 'psql'))
    
    mycursor.close()  # closing the cursor object connection
    conn.close()
    print("Successfully closed the connection")


def get_existing_acc_details():
    customer_credit_card_no, last_four_digit_SSN = input(
        "Enter Customer Credit Card NO & last_four_digit_SSN :").split()
    # checking the connection established successfully
    if conn.is_connected():
        print(' ')
    else:
        conn.connect()
    mycursor = conn.cursor()
    query = "select FIRST_NAME, MIDDLE_NAME, LAST_NAME, CONCAT(FULL_STREET_ADDRESS,', ',CUST_CITY,', ',CUST_STATE),CUST_PHONE, CUST_EMAIL from creditcard_capstone.cdw_sapp_customer where CREDIT_CARD_NO = " + customer_credit_card_no + " AND substring(SSN,6,4) = " + last_four_digit_SSN + ""
    #print(query)
    mycursor.execute(query)
    result = mycursor.fetchall();  # fetch all the values from the mysql database
    # Convert to Pandas Dataframe
    df = pd.DataFrame(result)
    df.columns = ['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'ADDRESS','CUST_PHONE', 'CUST_EMAIL']
    # Display the Pandas Dataframe
    print(tabulate(df, headers = 'keys', tablefmt = 'psql',showindex=False))
    # print(result)
    mycursor.close()  # closing the cursor object connection
    conn.close()
    #print("Successfully closed the connection")


def modify_existing_acc_details():
    updated_first_name = None
    updated_middle_name = None
    updated_last_name = None
    updated_email = None
    updated_phone = None
    is_update_required = False
    customer_credit_card_no, last_four_digit_SSN = input(
        "Enter Customer Credit Card NO & last_four_digit_SSN :").split()
    try:
        update_column = int(input(
            "What customer detail you want to update:(1.FIRST_NAME 2.MIDDLE_NAME,3.LAST_NAME 4.CUST_EMAIL 5.CUST_PHONE 6.Done)?"))
        while update_column != 6:
            if update_column == 1:
                is_update_required = True
                updated_first_name = input("Enter updated value for the First Name:")
                print(updated_first_name)
            elif update_column == 2:
                is_update_required = True
                updated_middle_name = input("Enter updated value for the Middle Name:")
                print(updated_middle_name)
            elif update_column == 3:
                is_update_required = True
                updated_last_name = input("Enter updated value for the Last Name:")
                print(updated_last_name)
            elif update_column == 4:
                is_update_required = True
                updated_email = input("Enter updated value for the email:")
                print(updated_email)
            elif update_column == 5:
                is_update_required = True
                updated_phone = input("Enter updated value for the phone number:")
                print(updated_phone)
            else:
                print("Invalid Input")
            update_column = int(input(
                "What customer detail you want to update:(1.FIRST_NAME 2.MIDDLE_NAME,3.LAST_NAME 4.CUST_PHONE 5.CUST_EMAIL 6.Done)?"))
        if update_column == 6 and is_update_required:
            print("Got all the new values to update for the existing customer")
        # checking the connection established successfully
            if not conn.is_connected():
                conn.connect()
            mycursor = conn.cursor()
            mySql_update_query = """UPDATE creditcard_capstone.cdw_sapp_customer SET """
            count = 0
            parameters = ()  # parameter tuple initialized
            if updated_first_name:
                mySql_update_query = mySql_update_query + """ FIRST_NAME = %s """
                count = count + 1
                parameters = parameters + (updated_first_name,)
            if updated_middle_name:
                mySql_update_query = mySql_update_query + """ MIDDLE_NAME = %s """
                count = count + 1
                parameters = parameters + (updated_middle_name,)
            if updated_last_name:
                if count > 0: mySql_update_query = mySql_update_query + ""","""
                mySql_update_query = mySql_update_query + """ LAST_NAME = %s """
                count = count + 1
                parameters = parameters + (updated_last_name,)
            if updated_email:
                if count > 0: mySql_update_query = mySql_update_query + ""","""
                mySql_update_query = mySql_update_query + """ CUST_EMAIL = %s """
                count = count + 1
                parameters = parameters + (updated_email,)
            if updated_phone:
                if count > 0: mySql_update_query = mySql_update_query + ""","""
                mySql_update_query = mySql_update_query + """ CUST_PHONE = %s """
                count = count + 1
                parameters = parameters + (updated_phone,)
            mySql_update_query = mySql_update_query + """, LAST_UPDATED=concat(date_format(now(),'%Y-%m-%dT%T.000'),time_format(timediff(now(),utc_timestamp),'%H:%i')) WHERE CREDIT_CARD_NO = %s and substring(SSN,6,4)=%s """
            parameters = parameters + (customer_credit_card_no, last_four_digit_SSN,)
            #print(mySql_update_query)
            #print(parameters)
            mycursor.execute(mySql_update_query, parameters)
            conn.commit()
            mycursor.close()
            conn.close()
            print("Record updated successfully into cdw_sapp_customer table")
        elif update_column == 6 and is_update_required == False:
            print("Currently Not updating the existing customer account details")
    except Error as error:
        print("Failed to insert into MySQL table {}".format(error))
    


# method to generate a monthly bill for a credit card number for a given month and year
def get_monthly_bill():
    # generate a monthly bill for a credit card number for a given month and year
    credit_card_no = input("Enter 16-digit credit card no:")
    month = int(input("Enter month as 2 digit integer MM format :"))
    while not isvalid_month(month):
        # if invalid month ask user to enter correct month number again
        month = int(input("Enter month in number format from 1 to 12:"))
    # get month MM as user input

    year = input("Enter year in YYYY format:")
    while not isvalid_year(year):
        # if invalid year ask user to enter year as 4 digit integer YYYY format
        year = input("Enter year as 2018 in YYYY format:")
    # checking the connection established successfully
    if conn.is_connected():
        print('Successfully Connected to MySQL database')
    else:
        conn.connect()
    mycursor = conn.cursor()
    query = """select TRANSACTION_ID,CREDIT_CARD_NO, TIMEID as TRANSACTION_DATE, TRANSACTION_TYPE, TRANSACTION_VALUE from cdw_sapp_credit_card where CREDIT_CARD_NO = %s AND month(TIMEID) = %s AND year(TIMEID) = %s """
    # print(query)
    values = (credit_card_no, month, year)
    mycursor.execute(query, values)
    result = mycursor.fetchall();  # fetch all the values from the mysql database
    # Convert to Pandas Dataframe
    df = pd.DataFrame(result)
    df.columns = ['TRANSACTION_ID','CREDIT_CARD_NO', "TRANSACTION_DATE", 'TRANSACTION_TYPE', 'TRANSACTION_VALUE']
    # Display the Pandas Dataframe
    #print(df)
    print("----------------------------------------------------------------------------------------------------------")

    print("Credit Card Statement  for " + calendar.month_name[month]+ " "+str(year))

    print(tabulate(df, headers = 'keys', tablefmt = 'psql',showindex=False))
    total_amount_query = """select SUM(TRANSACTION_VALUE) from cdw_sapp_credit_card where CREDIT_CARD_NO=%s AND month(TIMEID)=%s AND year(TIMEID)=%s """
    #print(total_amount_query)
    mycursor.execute(total_amount_query,values)
    total_amount_result = mycursor.fetchall();  # fetch all the values from the mysql database
    # Convert to Pandas Dataframe
    df1 = pd.DataFrame(total_amount_result)
    df1.columns = ['Total_Amount']
    # Display the Pandas Dataframe
    #print(df1.to_string(index=False))
    #print("Total_Amount : ",tabulate(df1.iat[0,0], tablefmt = 'psql',showindex=False))
    print("                                                                  Total_Amount   :              ",df1.iat[0,0])
    print("------------------------------------------------------------------------------------------------------------")
    mycursor.close()  # closing the cursor object connection
    conn.close()
    print("Successfully closed the connection")


# method to display the transactions made by a customer between two dates.
def get_transactions_within_range():
    # calling the method to display the transactions made by a customer between two dates.
    print("Enter the start & End date to pull the transaction made by the customer for specific date range:")
    start_date = int(input("Enter Start Date(YYYYMMDD):"))
    end_date = int(input("Enter End Date(YYYYMMDD):"))
    print("Transaction Details for the time period {} to {}".format(start_date, end_date))

    # checking the connection established successfully
    if conn.is_connected():
        print('Successfully Connected to MySQL database')
    else:
        conn.connect()
    mycursor = conn.cursor()
    query = """select distinct TRANSACTION_ID,TRANSACTION_TYPE,TRANSACTION_VALUE, CREDIT_CARD_NO,TIMEID from cdw_sapp_credit_card where TIMEID BETWEEN '%s' AND '%s' ORDER BY TIMEID desc """
    # print(query)
    values = (start_date, end_date)
    mycursor.execute(query, values)
    result = mycursor.fetchall();  # fetch all the values from the mysql database
    # Convert to Pandas Dataframe
    df = pd.DataFrame(result)
    df.columns = ['TRANSACTION_ID', 'TRANSACTION_TYPE', 'TRANSACTION_VALUE', 'CREDIT_CARD_NO', "TIMEID"]
    # Display the Pandas Dataframe
    print(tabulate(df,showindex=False,tablefmt='psql'))
    # print(result)
    mycursor.close()  # closing the cursor object connection
    conn.close()
    print("Successfully closed the connection")
