
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, substring , when , month, year,cast
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("Loan_API_Visual").getOrCreate()


mySQL_db_properties = {
    "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}


cdw_sapp_credit = spark.read.jdbc(mySQL_db_properties["url"], "CDW_SAPP_CREDIT", properties=mySQL_db_properties)
api_loan_df = spark.read.jdbc(mySQL_db_properties["url"], "CDW_SAPP_loan_API", properties=mySQL_db_properties)

cdw_sapp_branch_df = spark.read.jdbc(mySQL_db_properties["url"], "CDW_SAPP_BRANCH", properties=mySQL_db_properties)

cdw_sapp_branch_df = spark.read.jdbc(mySQL_db_properties["url"], table="CDW_SAPP_CREDIT_CARD", properties=mySQL_db_properties)

self_employed_approved_count = api_loan_df.filter((col("Self_Employed") == "Yes") & (col("Application_Status") == "Y")).count()
self_employed_total_count = api_loan_df.filter(col("Self_Employed") == "Yes").count()
self_employed_approved_percentage = (self_employed_approved_count / self_employed_total_count) * 100

plt.bar(['Self_Employed_Approval'], [self_employed_approved_percentage])
plt.ylim(0,100)
plt.ylabel('Percentage (%)')
plt.title('Loan Approval by Employment Type')
plt.show()


married_male_rejected_count = api_loan_df.filter((col("Gender") == "Male") & (col("Married") == "Yes") & (col("Application_Status") == "N")).count()
married_male_total_count = api_loan_df.filter((col("Gender") == "Male") & (col("Married") == "Yes")).count()
married_male_rejected_percentage = (married_male_rejected_count / married_male_total_count) * 100
print(f"Percentage Rejection for Married Males: {married_male_rejected_percentage:.2f}%")


transaction_counts = (cdw_sapp_credit
    .withColumn("Month", substring(cdw_sapp_credit["TIMEID"], 5, 2).cast("int"))
    .groupBy("Month")
    .count()
    .orderBy(col("count").desc())
    .limit(3))
transaction_counts.show()
ax1 = transaction_counts.toPandas().plot(x='Month', y='count', title='Top 3 Months of Transactions', legend=False)
plt.ylabel('Transaction Count')
ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45)
plt.show()


healthcare_transactions = cdw_sapp_credit.filter(col("TRANSACTION_TYPE") == "Healthcare").groupBy("BRANCH_CODE").agg(sum("TRANSACTION_VALUE").alias("Total Amount")).orderBy(col("Total Amount").desc()).limit(1)
healthcare_transactions.show()
ax2 = healthcare_transactions.toPandas().plot(x='BRANCH_CODE', y='Total $',title='Branch with thr Highest Value of Healthcare Transactions', legend=False)
plt.ylabel('Total Dollar Amount')
ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45)
plt.show()

# Stop spark sesssion
spark.stop()