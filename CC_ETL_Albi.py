
# Data Extraction and Transformation with Python and PySpark Rq 1.1


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CC_MySQL_JSON_ETL").getOrCreate()


cdw_branch_df = spark.read.json("cdw_sapp_branch.json")
cdw_credit_df = spark.read.json("cdw_sapp_credit.json")
cdw_customer_df = spark.read.json("cdw_sapp_custmer.json")

cdw_branch_df.show()
cdw_credit_df.show()
cdw_customer_df.show()


# Connecting to our "creditcard_capstone" database in MySQL Workbench.

mySql_db_properties = {
    "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"}

# Writing in our JSON Dataframe into a table in our database
cdw_customer_df.write.jdbc(
    url=mySql_db_properties["url"],
    table="CDW_SAPP_CUSTOMER",
    mode="overwrite",
    properties=mySql_db_properties
)

# Credit file into table

cdw_credit_df.write.jdbc(
    url=mySql_db_properties["url"],
    table="CDW_SAPP_CREDIT",
    mode="overwrite",
    properties=mySql_db_properties
)


# Branch file into table

cdw_branch_df.write.jdbc(
    url=mySql_db_properties["url"],
    table="CDW_SAPP_BRANCH",
    mode="overwrite",
    properties=mySql_db_properties
)

cdw_credit_df = spark.read.jdbc(url=mySql_db_properties["url"], table="CDW_SAPP_CREDIT", properties=mySql_db_properties)
cdw_customer_df = spark.read.jdbc(url=mySql_db_properties["url"], table="CDW_SAPP_CUSTOMER", properties=mySql_db_properties)



def given_zip_code():
    zip_code = input("Enter the zip code: ")
    year = input("Enter the year (YYYY): ")
    month = input("Enter the month (MM): ")

    result = cdw_credit_df.filter((cdw_credit_df.CUST_ZIP == zip_code) & 
                                   (cdw_credit_df.TIMEID.startswith(year + month)))
    result.show()



def given_type():
    transaction_type = input("Enter the transaction type: ")
    result = cdw_credit_df.filter(cdw_credit_df.TRANSACTION_TYPE == transaction_type)
    print(f"Total Transactions: {result.count()}")
    print(f"Total Value: {result.groupBy().sum('TRANSACTION_VALUE').collect()[0][0]}")

def given_state():
    state = input("Enter the branch state: ")
    result = cdw_credit_df.filter(cdw_credit_df.BRANCH_STATE == state)
    print(f"Total Transactions: {result.count()}")
    print(f"Total Value: {result.groupBy().sum('TRANSACTION_VALUE').collect()[0][0]}")

def account_details():
    ssn = input("Enter the SSN of the customer: ")
    result = cdw_customer_df.filter(cdw_customer_df.SSN == ssn)
    result.show()


def monthVSyear_bill():
    credit_card_no = input("Enter the credit card number: ")
    month = input("Enter the month (MM): ")
    year = input("Enter the year (YYYY): ")

    transactions = cdw_credit_df.filter(
        (cdw_credit_df.CUST_CC_NO == credit_card_no) & 
        (cdw_credit_df.TIMEID.startswith(year + month))
    )
    
    total_amount = transactions.groupBy().sum('TRANSACTION_VALUE').collect()[0][0]

    print(f"Monthly bill for {credit_card_no} in {month}/{year} is: ${total_amount}")

spark.stop()