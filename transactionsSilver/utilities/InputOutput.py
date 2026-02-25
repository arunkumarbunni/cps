def read_csv_from_s3(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True )


def write_parquet_to_s3(df, path):
    df.write.mode("overwrite").parquet(path)