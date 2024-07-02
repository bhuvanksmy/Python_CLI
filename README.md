# Python_CLI
https://docs.google.com/document/d/1QxkEehdLovK2f8PfqzJtfRn83PwHyqAs0r-1rPwaXV8/edit

Data Engineering - Python CLI -Capstone Project 
This capstone project demonstrates my knowledge and abilities in Python (Pandas, advanced modules), SQL, Apache Spark (Spark Core, Spark SQL), CLI and created ETL process to extract data from Loan Application dataset which is in JSON format.

Workflow Diagram of the Requirements.
The workflow diagram below will help you visualize the flow and scope of this capstone project at a high level.

<div align = "center">
<img src = "data/Dataflow.JPG"/>
</div>

### Credit Card Dataset Overview.
The Credit Card System database is an independent system developed for managing activities such as registering new customers and approving or canceling requests, etc., using the architecture.
A credit card is issued to users to enact the payment system. It allows the cardholder to access financial services in exchange for the holder's promise to pay for them later. Below are three files that contain the customer’s transaction information and inventories in the credit card information.
CDW_SAPP_CUSTOMER.JSON: This file has the existing customer details.
CDW_SAPP_CREDITCARD.JSON: This file contains all credit card transaction information.
CDW_SAPP_BRANCH.JSON: Each branch’s information and details are recorded in this file. 

Business Requirements - ETL
1. Functional Requirements - Load Credit Card Database (SQL)

### 1. Functional Requirements - Load Credit Card Database (SQL)

### Functional Requirement 1.1
    For “Credit Card System,” created a Python and PySpark SQL program to read/extract the following JSON files according to the specifications found in the mapping document.

### Function Requirement 1.2

    Once PySpark reads data from JSON files, and then utilizes Python, PySpark, and Python modules to load data into RDBMS(SQL).
    
    Created a creditcard_capstoned database in SQL(MySQL), and created a Python and Pyspark Program to write the “Credit Card System Data” into following tables in RDBMS:
        1)CDW_SAPP_BRANCH
        2)CDW_SAPP_CREDIT_CARD
        3)CDW_SAPP_CUSTOMER 

### 2. Functional Requirements - Application Front-End
###  2.1 Transaction Details Module

Created a user defined function that accomplishes the following tasks:

    2.1.1 - Prompt the user for a zip code, provide contextual cues for valid input, and verify it is in the correct format.
    2.1.2 - Ask for a month and year,  and provide contextual cues for valid input and verify it is in the correct format.
    2.1.3 - Use the provided inputs to query the database and retrieve a list of transactions made by customers in the specified zip code for the given month and year.
    2.1.4 - Sort the transactions by day in descending order.





