import matplotlib.pyplot as plt
import mysql.connector as dbconnect
import pandas as pd
import seaborn as sns


# req 3.1 - Calculate and plot which transaction type has the highest transaction count.
def plot_highest_transaction_count():
    try:
        conn = dbconnect.connect(host='localhost', database='creditcard_capstone', user='root', password='password',
                                 port='3306')
        # checking the connection established successfully
        if conn.is_connected():
            print('Successfully Connected to MySQL database')
        mycursor = conn.cursor()
        query = """ select transaction_type, count(transaction_type) as transaction_count from cdw_sapp_credit_card group by transaction_type ORDER BY transaction_count desc"""
        mycursor.execute(query)
        result = mycursor.fetchall();  # fetch all the values from the mysql database
        # Convert to Pandas Dataframe
        df = pd.DataFrame(result, columns=['TRANSACTION_TYPE', 'TRANSACTION_COUNT'])
        print(df)
        # Defining the plotsize
        plt.figure(figsize=(8, 6))

        # Defining the x-axis, the y-axis and the data
        # from where the values are to be taken
        palette = ["#000000", "#171819", "#2f3132", "#464a4b", "#5e6264", "#767b7d", "#8d9496"]
        plots = sns.barplot(x=df['TRANSACTION_TYPE'], y=df['TRANSACTION_COUNT'], palette=palette,
                            hue=df['TRANSACTION_TYPE'], data=df)
        for bar in plots.patches:
            plots.annotate(format(bar.get_height(), '.0f'),
                           (bar.get_x() + bar.get_width() / 2,
                            bar.get_height()), ha='center', va='center',
                           size=15, xytext=(0, 8),
                           textcoords='offset points')
        # Title
        plt.title(" Transaction type with highest transaction count")
        # Setting the x-axis label and its size
        plt.xlabel("TRANSACTION_TYPE", size=13)

        # Setting the y-axis label and its size
        plt.ylabel("TRANSACTION_TYPE", size=13)

        # Finally plotting the graph
        plt.show()
        mycursor.close()  # closing the cursor object connection
        conn.close()
        print("Successfully closed the connection")
    except Exception as e:
        conn.close()
        print(str(e))


# req 3.2 - Calculate and plot top 10 states with the highest number of customers.
def top10_States_with_high_customers():
    try:
        conn = dbconnect.connect(host='localhost', database='creditcard_capstone', user='root', password='password',
                                 port='3306')
        # checking the connection established successfully
        if conn.is_connected():
            print('Successfully Connected to MySQL database')
        mycursor = conn.cursor()
        query = """select distinct CUST_STATE as STATE,count(CUST_STATE) as CUSTOMER_COUNT from cdw_sapp_customer group by CUST_STATE order by count(CUST_STATE) desc limit 10"""
        mycursor.execute(query)
        result = mycursor.fetchall();  # fetch all the values from the mysql database
        # Convert to Pandas Dataframe
        df = pd.DataFrame(result, columns=['STATE', 'CUSTOMER_COUNT'])
        print(df)
        # Defining the plotsize
        plt.figure(figsize=(8, 6))

        # Defining the x-axis, the y-axis and the data
        # from where the values are to be taken
        plots = sns.barplot(x=df['STATE'], y=df['CUSTOMER_COUNT'], data=df)
        for bar in plots.patches:
            plots.annotate(format(bar.get_height(), '.0f'),
                           (bar.get_x() + bar.get_width() / 2,
                            bar.get_height()), ha='center', va='center',
                           size=15, xytext=(0, 8),
                           textcoords='offset points')
        # Title
        plt.title(" Top 10 states with highest number of customers")
        # Setting the x-axis label and its size
        plt.xlabel("TRANSACTION_TYPE", size=13)

        # Setting the y-axis label and its size
        plt.ylabel("TRANSACTION_TYPE", size=13)

        # Finally plotting the graph
        plt.show()
        mycursor.close()  # closing the cursor object connection
        conn.close()
        print("Successfully closed the connection")
    except Exception as e:
        conn.close()
        print(str(e))


# req 3.3 - top 10 customers with the highest transaction amounts (in dollar

def top10_customers_with_high_transaction_amount():
    try:
        conn = dbconnect.connect(host='localhost', database='creditcard_capstone', user='root', password='password',
                                 port='3306')
        # checking the connection established successfully
        if conn.is_connected():
            print('Successfully Connected to MySQL database')
        mycursor = conn.cursor()
        query = """select distinct cus.FIRST_NAME as CUSTOMER_NAME,cc.CREDIT_CARD_NO,round(sum(TRANSACTION_VALUE),2) as TRANSACTION_SUM from cdw_sapp_credit_card as cc INNER JOIN cdw_sapp_customer as cus on cc.CREDIT_CARD_NO = cus.CREDIT_CARD_NO AND cc.CUST_SSN = cus.SSN group by cc.CREDIT_CARD_NO,cus.FIRST_NAME order by sum(TRANSACTION_VALUE) desc LIMIT 10"""
        mycursor.execute(query)
        result = mycursor.fetchall();  # fetch all the values from the mysql database
        # Convert to Pandas Dataframe
        df = pd.DataFrame(result, columns=['CUSTOMER_NAME', 'CREDIT_CARD_NO', 'TRANSACTION_SUM'])
        print(df)
        # Defining the plotsize
        plt.figure(figsize=(8, 6))
        palette = ["#fee090", "#fdae61", "#4575b4", "#313695", "#4682B4", "#abd9e9", "#d73027", "#a50026", "#4575b4",
                   "#abd9e9"]
        # Defining the x-axis, the y-axis and the data
        # from where the values are to be taken
        plots = sns.barplot(x=df['CUSTOMER_NAME'], y=df['TRANSACTION_SUM'], palette=palette, hue=df['CUSTOMER_NAME'],
                            data=df)
        for bar in plots.patches:
            plots.annotate(format(bar.get_height(), '.0f'),
                           (bar.get_x() + bar.get_width() / 2,
                            bar.get_height()), ha='center', va='center',
                           size=15, xytext=(0, 8),
                           textcoords='offset points')
        # Title
        plt.title(" Top 10 customers with highest transaction amount(in dollars)")
        # Setting the x-axis label and its size
        plt.xlabel("CUSTOMER_NAME", size=13)

        # Setting the y-axis label and its size
        plt.ylabel("TRANSACTION_SUM", size=13)

        # Finally plotting the graph
        plt.show()
        mycursor.close()  # closing the cursor object connection
        conn.close()
        print("Successfully closed the connection")
    except Exception as e:
        conn.close()
        print(str(e))


# 5. Functional Requirements - Data Analysis and Visualization for LOAN Application
# req - 5.1 Calculate and plot the percentage of applications approved for self-employed applicants
def plot_percentage_for_applications_approved_for_selfemployed():
    try:
        conn = dbconnect.connect(host='localhost', database='creditcard_capstone', user='root', password='password',
                                 port='3306')
        # checking the connection established successfully
        if conn.is_connected():
            print('Successfully Connected to MySQL database')
        mycursor = conn.cursor()
        query = """select count(Application_Status) as count from cdw_sapp_loan_application where Self_Employed = 'Yes' group by Application_Status"""
        mycursor.execute(query)
        result = mycursor.fetchall()
        # print(result)
        num_rows = int(mycursor.rowcount)
        print(num_rows)
        x = map(list, list(result))  # change the type
        x = sum(x, [])
        print(x)
        # Defining the plotsize
        plt.figure(figsize=(8, 6))

        # labels for data, replace with your own
        keys = ['YES', 'NO']

        # define Seaborn color palette to use 
        palette_color = sns.color_palette('bright')

        # plotting data on chart 
        plt.pie(x, labels=keys, colors=palette_color, autopct='%.0f%%', startangle=90)

        plt.legend(loc='upper right')

        # Add title to the chart
        plt.title('Percentage of applications approved for self-employed applicants')

        # displaying chart 
        plt.show()

    except Exception as e:
        conn.close()
        print(str(e))


# req - 5.2 Calculate the percentage of rejection for married male applicants.
def plot_percentage_of_rejections_for_married_male_applicants():
    try:
        conn = dbconnect.connect(host='localhost', database='creditcard_capstone', user='root', password='password',
                                 port='3306')
        # checking the connection established successfully
        if conn.is_connected():
            print('Successfully Connected to MySQL database')
        mycursor = conn.cursor()
        query = """select count(*) from cdw_sapp_loan_application where Gender = 'Male' AND Married = 'Yes' group by Application_Status"""
        mycursor.execute(query)
        result = mycursor.fetchall()
        # print(result)
        num_rows = int(mycursor.rowcount)
        print(num_rows)
        x = map(list, list(result))
        x = sum(x, [])
        print(x)
        # Defining the plotsize
        plt.figure(figsize=(8, 6))

        # labels for data
        keys = ['Rejected', 'Approved']

        # define Seaborn color palette to use 
        palette_color = sns.color_palette('bright')

        # plotting data on chart 
        plt.pie(x, labels=keys, colors=palette_color, autopct='%.0f%%', startangle=90)

        plt.legend(loc='upper right')

        # Add title to the chart
        plt.title('Percentage of applications approved for self-employed applicants')

        # displaying chart 
        plt.show()

    except Exception as e:
        conn.close()
        print(str(e))


# req 5.3 - Calculate and plot the top three months with the largest volume of transaction data

def top3_months_with_high_transactional_data():
    try:
        conn = dbconnect.connect(host='localhost', database='creditcard_capstone', user='root', password='password',
                                 port='3306')
        # checking the connection established successfully
        if conn.is_connected():
            print('Successfully Connected to MySQL database')
        mycursor = conn.cursor()
        query = """select month(TIMEID),round(sum(TRANSACTION_VALUE)) from cdw_sapp_credit_card group by month(TIMEID) order by round(sum(TRANSACTION_VALUE)) desc limit 3"""
        mycursor.execute(query)
        result = mycursor.fetchall();  # fetch all the values from the mysql database
        # Convert to Pandas Dataframe
        df = pd.DataFrame(result, columns=['MONTH', 'TRANSACTION_VALUE'])
        # print(df)
        # Defining the plotsize
        plt.figure(figsize=(8, 6))

        # Defining the x-axis, the y-axis and the data
        # from where the values are to be taken
        palette = ["red", "blue", "green"]
        plots = sns.barplot(x=df['MONTH'], y=df['TRANSACTION_VALUE'], data=df, palette=palette, width=0.5)

        for bar in plots.patches:
            plots.annotate(format(bar.get_height(), '.0f'),
                           (bar.get_x() + bar.get_width() / 2,
                            bar.get_height()), ha='center', va='center',
                           size=15, xytext=(0, 8),
                           textcoords='offset points')
        # Title
        plt.title(" Top 3 months with highest volume of transactional data")
        # Setting the x-axis label and its size
        plt.xlabel("MONTH", size=13)

        # Setting the y-axis label and its size
        plt.ylabel("TRANSACTION_VALUE", size=13)

        # Finally plotting the graph
        plt.show()
        mycursor.close()  # closing the cursor object connection
        conn.close()
        print("Successfully closed the connection")
    except Exception as e:
        conn.close()
        print(str(e))


# req 5.4 - Calculate and plot which branch processed the highest total dollar value of healthcare transactions.

def get_branch_with_highest_dollarvalue_healthcare_transactions():
    try:
        conn = dbconnect.connect(host='localhost', database='creditcard_capstone', user='root', password='password',
                                 port='3306')
        # checking the connection established successfully
        if conn.is_connected():
            print('Successfully Connected to MySQL database')
        mycursor = conn.cursor()
        query = """ select distinct br.BRANCH_CITY,cr.TRANSACTIONAL_VALUE from cdw_sapp_branch br inner join (select BRANCH_CODE,round(sum(TRANSACTION_VALUE)) as TRANSACTIONAL_VALUE from cdw_sapp_credit_card where TRANSACTION_TYPE = 'Healthcare' group by BRANCH_CODE order by round(sum(TRANSACTION_VALUE)) desc ) cr ON  br.BRANCH_CODE = cr.BRANCH_CODE order by cr.TRANSACTIONAL_VALUE desc limit 10"""
        mycursor.execute(query)
        result = mycursor.fetchall();  # fetch all the values from the mysql database
        # Convert to Pandas Dataframe
        df = pd.DataFrame(result, columns=['BRANCH_CITY', 'TRANSACTION_VALUE'])
        # print(df)
        # Defining the plotsize
        plt.figure(figsize=(8, 6))

        # Defining the x-axis, the y-axis and the data
        # from where the values are to be taken
        # palette = ["red","blue","green"]
        plots = sns.barplot(x=df['BRANCH_CITY'], y=df['TRANSACTION_VALUE'], data=df, width=0.5)

        for bar in plots.patches:
            plots.annotate(format(bar.get_height(), '.0f'),
                           (bar.get_x() + bar.get_width() / 2,
                            bar.get_height()), ha='center', va='center',
                           size=15, xytext=(0, 8),
                           textcoords='offset points')
        # Title
        plt.title("Branch with highest total dollar value of healthcare transactions")
        # Setting the x-axis label and its size
        plt.xlabel("BRANCH", size=13)

        # Setting the y-axis label and its size
        plt.ylabel("TRANSACTION_VALUE", size=13)

        # Finally plotting the graph
        plt.show()
        mycursor.close()  # closing the cursor object connection
        conn.close()
        print("Successfully closed the connection")
    except Exception as e:
        conn.close()
        print(str(e))
