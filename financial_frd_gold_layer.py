import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, avg, concat_ws, substring, date_format

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read silver-layer data
trnx_df = spark.read.parquet("s3://financial-data-silver/transactions/")
cust_df = spark.read.parquet("s3://financial-data-silver/customers/")

# Join customers and transactions
ctunion = cust_df.join(trnx_df, cust_df["card_id"] == trnx_df["card_id"], 'left') \
                 .drop(trnx_df["card_id"])

# Create customer_id and rename transaction location
ctunion = ctunion.withColumn(
    "customer_id",
    concat_ws("-", substring(col("home_location"), 1, 2), col("card_id"))
).withColumnRenamed("location", "transaction_location")

# Flag mismatches
ctunion_trnxrisk = ctunion.withColumn(
    "mismatch_flag",
    when(col("home_location") == col("transaction_location"), "false").otherwise("true")
)

# Risk levels
ctunion_trnxrisk = ctunion_trnxrisk.withColumn(
    "risk_level",
    when(col("mismatch_flag") == "false", "low")
    .when((col("mismatch_flag") == "true") & (col("merchant_category") == "Travel"), "medium")
    .when((col("mismatch_flag") == "true") & ((col("merchant_category") == "Home Improvement") | (col("merchant_category") == "Groceries")), "critical")
    .otherwise("high")
)

# Location mismatch anomalies
location_mismatch_anomalies = ctunion_trnxrisk.select(
    col("customer_id"),
    col("home_location"),
    col("transaction_location"),
    col("mismatch_flag"),
    col("risk_level")
)

# Monthly spend calculation
df_monthly = ctunion_trnxrisk.withColumn("year_month", date_format(col("timestamp"), "yyyy-MM"))

ctunion_b_a_group = df_monthly.groupBy("customer_id", "year_month") \
    .agg(avg(col("amount")).alias("avg_monthly_spend"))

# Join back on both customer_id and year_month to avoid duplication
ctunion_b_a_group = ctunion_b_a_group.join(
    df_monthly,
    (ctunion_b_a_group["customer_id"] == df_monthly["customer_id"]) &
    (ctunion_b_a_group["year_month"] == df_monthly["year_month"]),
    "left"
).drop(df_monthly["customer_id"]).drop(df_monthly["year_month"])

# Deviation from normal
ctunion_b_a_group = ctunion_b_a_group.withColumn(
    "deviation_from_normal",
    col("amount") - col("avg_monthly_spend")
)

# Anomaly flag
ctunion_b_a_group = ctunion_b_a_group.withColumn(
    "anomaly_flag",
    when(
        ((col("deviation_from_normal") / col("avg_monthly_spend")) <= -0.75) |
        ((col("deviation_from_normal") / col("avg_monthly_spend")) >= 0.5),
        "anomalous"
    ).otherwise("normal")
)

# Behavioural anomalies
behavioural_anomalies = ctunion_b_a_group.select(
    col("customer_id"),
    col("transaction_id").alias("txn_id"),
    col("amount"),
    col("avg_monthly_spend"),
    col("deviation_from_normal"),
    col("anomaly_flag")
)

# Write gold-layer outputs
location_mismatch_anomalies.write.mode("overwrite").parquet("s3://financial-data-gold/location_mismatch_anomalies/")
behavioural_anomalies.write.mode("overwrite").parquet("s3://financial-data-gold/behavioural_anomalies/")

job.commit()