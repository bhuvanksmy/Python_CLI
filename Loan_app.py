# import required module
import os
import sys
import re
import pandas as pd
import numpy as np
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,make_date,expr,date_format
import requests
import mysql.connector as dbconnect
from mysql.connector import Error

# Define MySQL connection properties
mysql_props = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# JDBC URL for MySQL to create new table cdw_sapp_customer and loading data into the table
mysql_url = "jdbc:mysql://localhost:3306/creditcard_capstone"

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
    
def load_database():
    # create the SparkSession
    spark = SparkSession.builder.appName('Capstone_Project').getOrCreate()
    # Reading JSON Customer data 
    df_customer = spark.read.option("multiline","true").json("cdw_sapp_customer.json")
    # Manipulating customer data according to the specifications in mapping document.
    df_customer = df_customer.select("SSN",expr("initcap(FIRST_NAME)").alias("FIRST_NAME"),expr("lower(MIDDLE_NAME)").alias("MIDDLE_NAME"),expr("initcap(LAST_NAME)").alias("LAST_NAME"),"CREDIT_CARD_NO"
                    ,expr("APT_NO || ',' || STREET_NAME").alias("FULL_STREET_ADDRESS"),"CUST_CITY","CUST_STATE","CUST_COUNTRY","CUST_ZIP",expr("'(' || substr(CUST_PHONE,0,3) || ')' || substr(CUST_PHONE,4,3) || '-' || substr(CUST_PHONE,7,1) || substr(CUST_PHONE,2,3)").alias("CUST_PHONE"),"CUST_EMAIL","LAST_UPDATED")
    #Reading JSON Branch Data
    df_branch = spark.read.option("multiline","true").json("cdw_sapp_branch.json")
    # Manipulating branch data according to the specifications in mapping document.
    df_branch = df_branch.select("BRANCH_CODE","BRANCH_NAME","BRANCH_STREET","BRANCH_CITY","BRANCH_STATE",expr("CASE WHEN BRANCH_ZIP IS NULL  THEN '99999' ELSE BRANCH_ZIP END").alias("BRANCH_ZIP"),expr("'(' || substr(BRANCH_PHONE,0,3) || ')' || substr(BRANCH_PHONE,4,3) || '-' || substr(BRANCH_PHONE,7,3)").alias("BRANCH_PHONE"),"LAST_UPDATED")
    # Reading JSON Credit_card data 
    df_creditcard = spark.read.option("multiline","true").json("cdw_sapp_credit.json")
    # Manipulating Credit card data according to the specifications in mapping document.
    df_creditcard=df_creditcard.select("CREDIT_CARD_NO",date_format(make_date(df_creditcard.YEAR,df_creditcard.MONTH,df_creditcard.DAY),"yyyyMMdd").alias("TIMEID"),"CUST_SSN","BRANCH_CODE","TRANSACTION_TYPE","TRANSACTION_VALUE","TRANSACTION_ID")


    df_customer.write \
        .jdbc(url=mysql_url,table="cdw_sapp_customer",mode="overwrite",properties=mysql_props)
    df_branch.write \
        .jdbc(url=mysql_url,table="cdw_sapp_branch",mode="overwrite",properties=mysql_props)
    df_creditcard.write \
        .jdbc(url=mysql_url,table="cdw_sapp_credit_card",mode="overwrite",properties=mysql_props)

    # get list of transactions made by the customers for the specified zip code,month and year.
def ls_transaction(zipcode,month,year):
    conn =dbconnect.connect(host='localhost',database='creditcard_capstone',user='root',password='password',port='3306')
    # checking the connection established successfully
    if conn.is_connected():
        print('Successfully Connected to MySQL database')
    mycursor=conn.cursor()
    query = "SELECT cr.CREDIT_CARD_NO, cr.TIMEID, cr.CUST_SSN, cr.BRANCH_CODE,cu.CUST_ZIP,cr.TRANSACTION_ID, cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE FROM cdw_sapp_credit_card AS cr INNER JOIN cdw_sapp_customer cu ON cr.CREDIT_CARD_NO = cu.CREDIT_CARD_NO AND cr.CUST_SSN = cu.SSN WHERE SUBSTRING(TIMEID, 1, 4) = " + str(year) + " AND SUBSTRING(TIMEID, 5, 2) = " + str(month) + " AND cu.cust_zip =" + str(zipcode)+" order by CAST(SUBSTRING(TIMEID, 5, 2) as UNSIGNED),CAST(SUBSTRING(TIMEID, 7, 2) as UNSIGNED),CAST(SUBSTRING(TIMEID, 1, 4) as UNSIGNED) desc"
    #print(query)
    mycursor.execute(query)
    result = mycursor.fetchall(); # fetch all the values from the mysql database
    # Convert to Pandas Dataframe
    df = pd.DataFrame(result)

    # Display the Pandas Dataframe
    print(df)
    #print(result)
    mycursor.close() # closing the cursor object connection
    conn.close()
    print("Successfully closed the connection")

def get_existing_acc_details(customer_credit_card_no,last_four_digit_SSN):
    # spark_acc = SparkSession.builder.appName('get_existing_acc_details').getOrCreate()
    # _select_sql = "(select * from creditcard_capstone.cdw_sapp_customer)"
    # df_select = spark_acc.read.jdbc(url=mysql_url,table=_select_sql,properties=mysql_props)
    # df_select.show()

    conn =dbconnect.connect(host='localhost',database='creditcard_capstone',user='root',password='password',port='3306')
    # checking the connection established successfully
    if conn.is_connected():
        print('Successfully Connected to MySQL database')
    mycursor=conn.cursor()
    query = "select SSN, FIRST_NAME, MIDDLE_NAME, LAST_NAME, CREDIT_CARD_NO, CUST_PHONE, CUST_EMAIL from creditcard_capstone.cdw_sapp_customer where CREDIT_CARD_NO = " + customer_credit_card_no + " AND substring(SSN,6,4) = " +last_four_digit_SSN +""
    # print(query)
    mycursor.execute(query)
    result = mycursor.fetchall(); # fetch all the values from the mysql database
    # Convert to Pandas Dataframe
    df = pd.DataFrame(result)
    df.columns = ['SSN', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CREDIT_CARD_NO', 'CUST_PHONE', 'CUST_EMAIL']
    # Display the Pandas Dataframe
    print(df)
    #print(result)
    mycursor.close() # closing the cursor object connection
    conn.close()
    print("Successfully closed the connection")

# def get_monthly_bill():
#     conn =dbconnect.connect(host='localhost',database='creditcard_capstone',user='root',password='password',port='3306')
#     # checking the connection established successfully
#     if conn.is_connected():
#         print('Successfully Connected to MySQL database')
#     mycursor=conn.cursor()
#     query = ""
#     # print(query)
#     mycursor.execute(query)
#     result = mycursor.fetchall(); # fetch all the values from the mysql database
#     # Convert to Pandas Dataframe
#     df = pd.DataFrame(result)
#     df.columns = ['SSN', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CREDIT_CARD_NO', 'CUST_PHONE', 'CUST_EMAIL']
#     # Display the Pandas Dataframe
#     print(df)
#     #print(result)
#     mycursor.close() # closing the cursor object connection
#     conn.close()
#     print("Successfully closed the connection")

def update_into_table(customer_credit_card_no,first_name, ssn, updated_first_name,updated_last_name,updated_email,updated_phone):
    try:
        conn = dbconnect.connect(database='creditcard_capstone', user='root',password='password', port ='3306')
        mycursor = conn.cursor()
        mySql_update_query = """UPDATE creditcard_capstone.cdw_sapp_customer SET """
        count=0
        parameters=()# parameter tuple initialized
        if updated_first_name:
            mySql_update_query=mySql_update_query+""" FIRST_NAME = %s """
            count=count+1
            parameters = parameters+(updated_first_name,)
        if updated_last_name:
            if count>0: mySql_update_query=mySql_update_query+""","""
            mySql_update_query=mySql_update_query+""" LAST_NAME = %s """
            count=count+1
            parameters = parameters+(updated_last_name,)
        if updated_email:
            if count>0: mySql_update_query=mySql_update_query+""","""
            mySql_update_query=mySql_update_query+""" CUST_EMAIL = %s """
            count=count+1
            parameters = parameters+(updated_email,)
        if updated_phone:
            if count>0: mySql_update_query=mySql_update_query+""","""
            mySql_update_query=mySql_update_query+""" CUST_PHONE = %s """
            count=count+1
            parameters = parameters+(updated_phone,)
        mySql_update_query=mySql_update_query+""" WHERE CREDIT_CARD_NO = %s and FIRST_NAME=%s and substring(SSN,6,4)=%s """                  
        parameters = parameters+(customer_credit_card_no,first_name,ssn,)
        print(mySql_update_query)
        print(parameters)
        mycursor.execute(mySql_update_query, parameters)
        conn.commit()
        print("Record updated successfully into cdw_sapp_customer table")
    except Error as error:
        print("Failed to insert into MySQL table {}".format(error))
    finally:
        if conn.is_connected():
            mycursor.close()
            conn.close()
            print("MySQL connection is closed")