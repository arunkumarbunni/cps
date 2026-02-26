import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, count
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

customer_df = spark.read.csv("s3://financial-data-bronze/customers/customers.csv", header=True, inferSchema=True)
trnx_df = spark.read.parquet("s3://financial-data-silver/transactions/")

#########################################################
#     Conting transactions done by the customer         #
#########################################################

trnx_df_count = trnx_df.groupBy("card_id").agg(count("transaction_id").alias("total_transactions"))

#########################################################
#               deduplication                           #
#########################################################

customer_distinct = customer_df.dropDuplicates()
#########################################################
#               Customer Demographics                   #
#########################################################
customer_modified = customer_distinct.withColumn("income_group", when(col("income") <= 50000, "lower income group").when((col("income") > 50000) & (col("income") <= 100000), "middle income group").otherwise("higher income group"))

customer_modified = customer_modified.withColumn("age_group", when(col("age") <= 25, "young age").when((col("age") > 25) & (col("age") <= 50), "middle age").otherwise("old age"))

########################################################
#               Historical Expenditure Flag            #
########################################################
customer_trnx_union = customer_modified.join(trnx_df_count, customer_modified["card_id"] == trnx_df_count["card_id"], 'left').drop(trnx_df_count["card_id"])

customer_silver = customer_trnx_union.withColumn("historical_spend_flag", when(col("total_transactions") > 10, "high").when((col("total_transactions") <= 10) & (col("total_transactions") > 5), "medium").otherwise("low")).drop(col("total_transactions"))


customer_silver.write.mode("overwrite").parquet("s3://financial-data-silver/customers/")

job.commit()