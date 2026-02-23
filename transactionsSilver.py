import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, hour, dayofmonth, floor, when


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#read data
transaction_df = spark.read.csv("s3://financial-data-bronze/transactions/transactions.csv", header=True, inferSchema=True)
#dropping duplicates
transaction_distinct = transaction_df.dropDuplicates()
#standardizing timestamp format and adding new time realted columns
transaction_distinct = transaction_distinct.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")).withColumn("hour_of_day", hour("timestamp")).withColumn("week_of_month", floor((dayofmonth("timestamp") - 1) / 7) + 1)
#adding expenditure column
transaction_silver = transaction_distinct.withColumn("expenditure", when(col("amount") <= 1000, "low").when((col("amount") > 1000) & (col("amount") <= 4000), "medium").otherwise("high"))
#writing as parquet file in destination S3 bucket
transaction_silver.write.mode("overwrite").parquet("s3://financial-data-silver/transactions/")

job.commit()