from pyspark.sql.functions import col, to_timestamp, hour, dayofmonth, floor, when


def process_transactions(df):

    # dropping duplicates
    df = df.dropDuplicates()

    # timestamp formatting and new columns
    df = df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "hour_of_day", hour("timestamp")
    ).withColumn(
        "week_of_month",
        floor((dayofmonth("timestamp") - 1) / 7) + 1
    )

    # expenditure column
    df = df.withColumn(
        "expenditure",
        when(col("amount") <= 1000, "low")
        .when((col("amount") > 1000) & (col("amount") <= 4000), "medium")
        .otherwise("high")
    )

    return df