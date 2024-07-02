import findspark

findspark.init()
from pyspark.sql import SparkSession
import requests

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("loan application dataset") \
    .getOrCreate()

# API Endpoint URL
api_url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

# req 4.1 Fetch data from the above API endpoint for the loan application dataset
response = requests.get(api_url)

# req 4.2 status code of the above API endpoint.
print(response.status_code)

if response.status_code == 200:
    # Convert JSON response to DataFrame
    json_df = spark.read.json(spark.sparkContext.parallelize([response.json()]))

    # Show DataFrame schema and content
    json_df.printSchema()
    json_df.show()

    # Define MySQL connection properties
    mysql_props = {
        "user": "root",
        "password": "password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    # JDBC URL for MySQL
    mysql_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
    # req 4.3 load data into MySQL table CDW-SAPP_loan_application in the database.
    json_df.write \
        .jdbc(url=mysql_url, table="CDW_SAPP_loan_application", mode="overwrite", properties=mysql_props)
else:
    print(f"Failed to fetch data from API. Status code: {response.status_code}")
# Stop SparkSession
spark.stop()
