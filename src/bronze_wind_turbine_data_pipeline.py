from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import current_timestamp

INPUT_DATA_LOCATION = "input_data/*.csv"
TARGET_TABLE_NAME = "bronze_wind_turbine_measurements"


def read_csv_with_header_and_infered_schema(spark, path: str) -> DataFrame:
    return spark.read.load(path=path, inferSchema=True, header=True, format="csv")


def append_dataset_to_delta_table(
    df: DataFrame, target_table_name: str, external_location: str = ""
) -> None:
    options = {}
    if external_location:
        options["path"] = external_location
    df.write.saveAsTable(
        name=target_table_name, format="delta", mode="append", **options
    )


def main():
    spark = SparkSession.getActiveSession()
    raw_data_df = read_csv_with_header_and_infered_schema(
        spark, path=INPUT_DATA_LOCATION
    )
    raw_data_with_metadata_df = raw_data_df.withColumn(
        "date_created", current_timestamp()
    )
    append_dataset_to_delta_table(raw_data_with_metadata_df, TARGET_TABLE_NAME)


if __name__ == "__main__":

    main()
