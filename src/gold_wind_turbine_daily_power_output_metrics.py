from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, max, mean, min, round

from utils import merge_all_dataset_into_table, read_table

INPUT_TABLE_NAME = "silver_wind_turbine_measurements"
TARGET_TABLE_NAME = "gold_wind_turbine_daily_power_output_metrics"


def add_date_column(df: DataFrame) -> DataFrame:
    return df.withColumn("date", col("timestamp").cast("date"))


def add_daily_power_output_metrics(df: DataFrame) -> DataFrame:
    return df.groupBy("turbine_id", "date").agg(
        min("power_output").alias("power_output_daily_min"),
        max("power_output").alias("power_output_daily_max"),
        round(mean("power_output"), 2).alias("power_output_daily_mean"),
    )


def main():
    spark = SparkSession.getActiveSession()
    # EXTRACT
    silver_data_df = read_table(spark, INPUT_TABLE_NAME)
    # TRANSFORM
    with_date_column_df = add_date_column(silver_data_df)
    daily_power_output_metrics_df = add_daily_power_output_metrics(
        df=with_date_column_df
    )
    # LOAD
    merge_all_dataset_into_table(
        spark=spark,
        df=daily_power_output_metrics_df,
        target_table_name=TARGET_TABLE_NAME,
        merge_cols=["turbine_id", "date"],
    )


if __name__ == "__main__":
    main()
