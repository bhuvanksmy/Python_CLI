import pandas as pd
import numpy as np
import findspark
# Initializes the spark session
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,expr
import requests


# create the SparkSession
spark = SparkSession.builder.appName('CapStone_Project').getOrCreate()
# Reading JSON Customer data 
df_customer = spark.read.option("multiline","true").json("C:/Users/ASB/Desktop/Bhuvana/Perscholas/Capstone_Project/cdw_sapp_customer.json")
# Manipulating customer data according to the specifications in mapping document.
df_customer = df_customer.select("SSN",expr("initcap(FIRST_NAME)").alias("FIRST_NAME"),expr("lower(MIDDLE_NAME)").alias("MIDDLE_NAME"),expr("initcap(LAST_NAME)").alias("LAST_NAME"),"CREDIT_CARD_NO"
                   ,expr("APT_NO || ',' || STREET_NAME").alias("FULL_STREET_ADDRESS"),"CUST_CITY","CUST_STATE","CUST_COUNTRY","CUST_ZIP",expr("'(' || substr(CUST_PHONE,0,3) || ')' || substr(CUST_PHONE,4,3) || '-' || substr(CUST_PHONE,7,1) || substr(CUST_PHONE,2,3)").alias("CUST_PHONE"),"CUST_EMAIL","LAST_UPDATED")
#Reading JSON Branch Data
df_branch = spark.read.option("multiline","true").json("C:/Users/ASB/Desktop/Bhuvana/Perscholas/Capstone_Project/cdw_sapp_branch.json")
# Manipulating branch data according to the specifications in mapping document.
df_branch = df_branch.select("BRANCH_CODE","BRANCH_NAME","BRANCH_STREET","BRANCH_CITY","BRANCH_STATE",expr("CASE WHEN BRANCH_ZIP IS NULL  THEN '99999' ELSE BRANCH_ZIP END").alias("BRANCH_ZIP"),expr("'(' || substr(BRANCH_PHONE,0,3) || ')' || substr(BRANCH_PHONE,4,3) || '-' || substr(BRANCH_PHONE,7,3)").alias("BRANCH_PHONE"),"LAST_UPDATED")
# Reading JSON Credit_card data 
df_creditcard = spark.read.option("multiline","true").json("C:/Users/ASB/Desktop/Bhuvana/Perscholas/Capstone_Project/cdw_sapp_credit.json")
# Manipulating Credit card data according to the specifications in mapping document.
df_creditcard=df_creditcard.select("CREDIT_CARD_NO",expr("YEAR || DAY || MONTH").alias("TIMEID"),"CUST_SSN","BRANCH_CODE","TRANSACTION_TYPE","TRANSACTION_VALUE","TRANSACTION_ID")


# Define MySQL connection properties
mysql_props = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# JDBC URL for MySQL to create new table cdw_sapp_customer and loading data into the table
mysql_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
df_customer.write \
    .jdbc(url=mysql_url,table="cdw_sapp_customer",mode="overwrite",properties=mysql_props)
df_branch.write \
    .jdbc(url=mysql_url,table="cdw_sapp_branch",mode="overwrite",properties=mysql_props)
df_creditcard.write \
    .jdbc(url=mysql_url,table="cdw_sapp_credit",mode="overwrite",properties=mysql_props)