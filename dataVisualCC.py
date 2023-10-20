from pyspark.sql import SparkSession
import matplotlib.pyplot as plt



# Create a Spark session
spark = SparkSession.builder.appName("DataVisualization").getOrCreate()


db_properties = {
    "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}


transactions_df = spark.read.jdbc(db_properties["url"], "CDW_SAPP_CREDIT_CARD", properties=db_properties)
customers_df = spark.read.jdbc(db_properties["url"], "CDW_SAPP_CUSTOMER", properties=db_properties)


transaction_count = transactions_df.groupBy("TRANSACTION_TYPE").count().orderBy("count", ascending=False).toPandas()
xCC = transaction_count.plot(kind='bar', x='TRANSACTION_TYPE', y='count', title="Transaction Counts by Type", legend=False)
xCC.set_ylabel('Count')
xCC.set_xlabel('Transaction Type')
xCC.set_xticklabels(xCC.get_xticklabels(), rotation=35)
plt.show()


state_count = customers_df.groupBy("CUST_STATE").count().orderBy("count", ascending=False).toPandas()
zCC = state_count.plot(kind='bar', x='CUST_STATE', y='count', title="Number of Customers by State", legend=False)
zCC.set_ylabel("Count")
zCC.set_xlabel("State")
plt.show()

# Req-3.3: Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
customer_count = transactions_df.groupBy("CUST_SSN").agg({'TRANSACTION_VALUE': 'sum'}).orderBy("sum(TRANSACTION_VALUE)", ascending=False).limit(10).toPandas()
CC_count = customer_count.plot(kind='bar', x='CUST_SSN', y='sum(TRANSACTION_VALUE)', title="Top 10 Customers by Transaction Amount", legend=False)
CC_count.set_ylabel("# of Transactions")
CC_count.set_xlabel("Customer #")
CC_count.set_xticklabels(CC_count.get_xticklabels(), rotation=35)
plt.show()

# Stop the Spark session
spark.stop()
