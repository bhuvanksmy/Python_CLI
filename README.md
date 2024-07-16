# Python_CLI
https://docs.google.com/document/d/1QxkEehdLovK2f8PfqzJtfRn83PwHyqAs0r-1rPwaXV8/edit

Data Engineering - Python CLI -Capstone Project 
This capstone project demonstrates my knowledge and abilities in Python, Python advanced modules(Pandas, Numpy, Matplotlib,Seaborn), MySQL, Apache Spark (Spark Core, Spark SQL), CLI and created ETL process to extract data from Credit card Application dataset(dataset in JSON format) & Loan Application API.

# Software needed to run this application
Software Requirements:
- ![#c5f015](https://via.placeholder.com/15/c5f015/c5f015.png) java 22.0.1 2024-04-16
- ![#f03c15](https://via.placeholder.com/15/f03c15/f03c15.png) MySQL 8.0
- ![#1589F0](https://via.placeholder.com/15/1589F0/1589F0.png) Python 3.11.9
- ![#58508d](https://via.placeholder.com/15/58508d/58508d.png) Spark version 3.5.1
- ![#8fce00](https://via.placeholder.com/15/8fce00/8fce00.png) Python libraries are listed in requirement.txt created using the python command <b>Pip freeze > requirement.txt</b>.
Run requirement.txt document in terminal after installing Python 3.11.9 to install all the required python libraries.

<b>Workflow Diagram of the Requirements.</b>
The workflow diagram below will help you visualize the flow and scope of this capstone project at a high level.

<div align = "center">
<img src = "images/Dataflow.JPG"/>
</div>

### Credit Card Dataset Overview.
The Credit Card System database is an independent system developed for creating new user, viewing and managing existing customers, handling transactions made by the customers and approving or canceling requests, etc., using the architecture.
A credit card is issued to users to enact the payment system. It allows the cardholder to access financial services in exchange for the holder's promise to pay for them later. Below are three files that contain the customer’s transaction information and inventories in the credit card information.

<b>CDW_SAPP_CUSTOMER.JSON:</b> This file has the existing customer details includes(SSN, FIRST_NAME, MIDDLE_NAME, LAST_NAME, CREDIT_CARD_NO, FULL_STREET_ADDRESS, CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, CUST_PHONE, CUST_EMAIL, LAST_UPDATED).The combination of (CREDIT_CARD_NO + SSN) composite key is the Unique key for customer table. 

<b>CDW_SAPP_CREDITCARD.JSON:</b> This file contains all credit card transaction information includes CREDIT_CARD_NO, TIMEID, CUST_SSN, BRANCH_CODE, TRANSACTION_TYPE, TRANSACTION_VALUE, TRANSACTION_ID. TRANSACTION_ID is the primary key and combination of (CREDIT_CARD_NO + SSN) composite key is the Unique key with respect to customer table.

<b>CDW_SAPP_BRANCH.JSON:</b> Each branch’s information includes BRANCH_CODE, BRANCH_NAME, BRANCH_STREET, BRANCH_CITY, BRANCH_STATE, BRANCH_ZIP, BRANCH_PHONE, LAST_UPDATED and details are recorded in this file. BRANCH_CODE  

### Business Requirements - ETL
<div align = "center">
<img src = "images/ApplicationMenu.jpeg"/>
</div>
### 1. Functional Requirements - Load Credit Card Database (SQL)

### 1.1 Functional Requirement
For “Credit Card System,” created a Python and PySpark SQL function to read/extract the following JSON files according to the specifications found in the mapping document. https://docs.google.com/spreadsheets/d/1t8UxBrUV6dxx0pM1VIIGZpSf4IKbzjdJ/edit?gid=672931242#gid=672931242

### 1.2 Functional Requirement

Once PySpark reads data from JSON files, and then utilizes Python, PySpark, and Python modules to load data into RDBMS(SQL).
    
Created a creditcard_capstoned database in SQL(MySQL), and created a Python and Pyspark Program to write the “Credit Card System Data” into following tables in RDBMS:
- ![#f03c15](https://www.iconsdb.com/icons/download/color/f03c15/circle-16.png) CDW_SAPP_BRANCH
- ![#c5f015](https://www.iconsdb.com/icons/download/color/c5f015/circle-16.png) CDW_SAPP_CREDIT_CARD
- ![#1589F0](https://www.iconsdb.com/icons/download/color/1589F0/circle-16.png) CDW_SAPP_CUSTOMER       

### 2. Functional Requirements - Application Front-End
### 2.1 Transaction Details Module

Created a user defined function that accomplishes the following tasks:

    2.1.1 - Prompt the user for a zip code, provide contextual cues for valid input, and verify it is in the correct format.
    2.1.2 - Ask for a month and year,  and provide contextual cues for valid input and verify it is in the correct format.
    2.1.3 - Use the provided inputs to query the database and retrieve a list of transactions made by customers in the specified zip code for the given month and year.
    2.1.4 - Sort the transactions by day in descending order.

### 2.2 Customer Details Module

Created user defined function that accomplishes the following tasks:

    2.2.1 - to check the existing account details of a customer.
    2.2.2 - Used to modify the existing account details of a customer.
    2.2.3 - Used to generate a monthly bill for a credit card number for a given month and year.
    2.2.4 - Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.

### 3. Functional Requirements - Data Analysis and Visualization
    Data Analysis and visualization are done using the Matlpoltlib & Seaborn python libraries. 

### 3.1 Calculate and plot which transaction type has the highest transaction count.
<div align = "center">
<img src = "images/plot_highest_transaction_count.jpeg"/>
</div>

### 3.2 Calculate and plot top 10 states with the highest number of customers.
<div align = "center">
<img src = "images/top10_States_with_high_customers.jpeg"/>
</div>

### 3.3 Calculate the total transaction sum for each customer based on their individual transactions. Identify the top 10 customers with the highest transaction amounts (in dollar value). Create a plot to showcase these top customers and their transaction sums.
<div align = "center">
<img src = "images/top10_customers_with_high_transaction_amount.jpeg"/>
</div>

### 4. Functional Requirements - LOAN Application Dataset

### Overview of LOAN Application Data API

Banks offer home loans across urban, semi-urban, and rural areas. Customers apply for a home loan, and the bank validates their eligibility. To automate this process in real time banks uses customer details such as Application_ID, Gender, Married, Dependents, Education, Self_Employed, Credit_History, Property_Area, Income, Application_Status from the online application form. They aim to identify eligible customer segments for targeted marketing, using a provided partial dataset. The dataset used for the LOAN Application is in the below API Endpoint

<b>API Endpoint:</b> https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json

### 4.1 Created a Python program to GET (consume) data from the above API endpoint for the loan application dataset.
Used API request.get() method to get response from API endpoint URL.

### 4.2 Calculate the status code of the above API endpoint. Hint: status code could be 200, 400, 404, 401.
Response is successful with the status code 200 and verified using response.statuscode.

### 4.3 Once Python reads data from the API, utilize PySpark to load data into RDBMS (SQL). The table name should be CDW-SAPP_loan_application in the database. Note: Use the “creditcard_capstone” database.
Used spark.read.JSON() to read the API data from endpoit URL , ulilized PySpark write method to load the API data into creditcard_capstone Database.

### 5. Functional Requirements - Data Analysis and Visualization for LOAN Application

### Create an appropriate visualization to perform the following task 

### 5.1 - Calculate and plot the percentage of applications approved for self-employed applicants. 
<div align = "center">
<img src = "images/plot_percentage_for_applications_approved_for_selfemployed.jpeg"/>
</div>

### 5.2 - Calculate the percentage of rejection for married male applicants. Use the ideal chart or graph to represent this data.
<div align = "center">
<img src = "images/plot_percentage_of_rejections_for_married_male_applicants.jpeg"/>
</div>

### 5.3 - Calculate and plot the top three months with the largest volume of transaction data. (hint: use `CDW_SAPP_CREDIT_CARD` table)
<div align = "center">
<img src = "images/top3_months_with_high_transactional_data.jpeg"/>
</div>

### 5.4 - Calculate and plot which branch processed the highest total dollar value of healthcare transactions. Use the ideal chart or graph to represent this data. (hint: use `CDW_SAPP_CREDIT_CARD` table)
<div align = "center">
<img src = "images/get_branch_with_highest_dollarvalue_healthcare_transactions.jpeg"/>
</div>














