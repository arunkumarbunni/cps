from utilities.sparkinit import init_glue
from utilities.InputOutput import read_csv_from_s3, write_parquet_to_s3
from modules.transformation import process_transactions


def main():

    spark, glue_context, job = init_glue()

    # S3 paths
    source_path = "s3://financial-data-bronze/transactions/transactions.csv"
    target_path = "s3://financial-data-silver/transactions/"

    # Read
    transaction_df = read_csv_from_s3(spark, source_path)

    # Transform
    transaction_silver = process_transactions(transaction_df)

    # Write
    write_parquet_to_s3(transaction_silver, target_path)

    job.commit()


if __name__ == "__main__":
    main()