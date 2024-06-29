import mysql.connector as conn
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector as dbconnect
from mysql.connector import Error

# req 3.1 - Calculate and plot which transaction type has the highest transaction count. 
def plot_highest_transaction_count():
    try:
        conn =dbconnect.connect(host='localhost',database='creditcard_capstone',user='root',password='password',port='3306')
        # checking the connection established successfully
        if conn.is_connected():
            print('Successfully Connected to MySQL database')
        mycursor=conn.cursor()
        query = """ select transaction_type, count(transaction_type) as transaction_count from cdw_sapp_credit_card group by transaction_type ORDER BY transaction_count desc"""
        mycursor.execute(query)
        result = mycursor.fetchall(); # fetch all the values from the mysql database
        #Convert to Pandas Dataframe
        df = pd.DataFrame(result,columns = ['TRANSACTION_TYPE','TRANSACTION_COUNT'])
        print(df)
        # Defining the plotsize
        plt.figure(figsize=(8, 6))

        # Defining the x-axis, the y-axis and the data
        # from where the values are to be taken
        palette = ["#000000","#171819","#2f3132","#464a4b","#5e6264","#767b7d","#8d9496"]
        plots = sns.barplot(x=df['TRANSACTION_TYPE'], y=df['TRANSACTION_COUNT'], palette= palette,hue = df['TRANSACTION_TYPE'], data=df)
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
        mycursor.close() # closing the cursor object connection
        conn.close()
        print("Successfully closed the connection")
    except Exception as e:
        conn.close()
        print(str(e))

# req 3.2 - Calculate and plot top 10 states with the highest number of customers.
def top10_States_with_high_customers():
    try:
        conn =dbconnect.connect(host='localhost',database='creditcard_capstone',user='root',password='password',port='3306')
        # checking the connection established successfully
        if conn.is_connected():
            print('Successfully Connected to MySQL database')
        mycursor=conn.cursor()
        query = """select distinct CUST_STATE as STATE,count(CUST_STATE) as CUSTOMER_COUNT from cdw_sapp_customer group by CUST_STATE order by count(CUST_STATE) desc limit 10"""
        mycursor.execute(query)
        result = mycursor.fetchall(); # fetch all the values from the mysql database
        #Convert to Pandas Dataframe
        df = pd.DataFrame(result,columns = ['STATE','CUSTOMER_COUNT'])
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
        mycursor.close() # closing the cursor object connection
        conn.close()
        print("Successfully closed the connection")
    except Exception as e:
        conn.close()
        print(str(e))

# req 3.3 - top 10 customers with the highest transaction amounts (in dollar

def top10_customers_with_high_transaction_amount():
    try:
        conn =dbconnect.connect(host='localhost',database='creditcard_capstone',user='root',password='password',port='3306')
        # checking the connection established successfully
        if conn.is_connected():
            print('Successfully Connected to MySQL database')
        mycursor=conn.cursor()
        query = """select distinct cus.FIRST_NAME as CUSTOMER_NAME,cc.CREDIT_CARD_NO,round(sum(TRANSACTION_VALUE),2) as TRANSACTION_SUM from cdw_sapp_credit_card as cc INNER JOIN cdw_sapp_customer as cus on cc.CREDIT_CARD_NO = cus.CREDIT_CARD_NO AND cc.CUST_SSN = cus.SSN group by cc.CREDIT_CARD_NO,cus.FIRST_NAME order by sum(TRANSACTION_VALUE) desc LIMIT 10"""
        mycursor.execute(query)
        result = mycursor.fetchall(); # fetch all the values from the mysql database
        #Convert to Pandas Dataframe
        df = pd.DataFrame(result,columns = ['CUSTOMER_NAME','CREDIT_CARD_NO','TRANSACTION_SUM'])
        print(df)
        # Defining the plotsize
        plt.figure(figsize=(8, 6))
        palette = ["#fee090", "#fdae61", "#4575b4", "#313695", "#4682B4", "#abd9e9", "#d73027", "#a50026","#4575b4","#abd9e9"]
        # Defining the x-axis, the y-axis and the data
        # from where the values are to be taken
        plots = sns.barplot(x=df['CUSTOMER_NAME'], y=df['TRANSACTION_SUM'], palette= palette,hue= df['CUSTOMER_NAME'],data=df)
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
        mycursor.close() # closing the cursor object connection
        conn.close()
        print("Successfully closed the connection")
    except Exception as e:
        conn.close()
        print(str(e))


# if __name__=="__main__": 
#     print("plotting")
#     plot_highest_transaction_count()
#     top10_States_with_high_customers()
#     top10_customers_with_high_transaction_amount()