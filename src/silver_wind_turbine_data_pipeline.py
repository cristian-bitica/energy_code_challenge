from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import coalesce, col, max, mean, rank, stddev, when
from pyspark.sql.window import Window

from src.utils import (
    create_delta_table_if_not_exists,
    merge_all_dataset_into_table,
    read_table,
)

INPUT_TABLE_NAME = "bronze_wind_turbine_measurements"
TARGET_TABLE_NAME = "silver_wind_turbine_measurements"
TARGET_TABLE_SCHEMA = """
    `timestamp` timestamp,
    turbine_id integer,
    wind_speed double,
    wind_direction integer,
    power_output double,
    date_created timestamp
    """


def deduplicate_dataset(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("turbine_id", "timestamp").orderBy(
        col("date_created").desc()
    )
    return df.withColumn("_rank", rank().over(window)).filter("_rank = 1").drop("_rank")


def filter_for_newly_arrived_data(spark: SparkSession, df: DataFrame) -> DataFrame:
    max_date_in_target_table = spark.sql(
        f"SELECT COALESCE(MAX(date_created), TO_TIMESTAMP(0)) FROM {TARGET_TABLE_NAME}"
    ).collect()[0][0]
    return df.filter(col("date_created") > max_date_in_target_table)


def fill_nulls_with_mean_value(df: DataFrame, colum_name: str) -> DataFrame:
    return df.withColumn(
        colum_name, coalesce(col(colum_name), col(f"{colum_name}_mean"))
    )


def add_mean_and_std_dev_to_df(
    df: DataFrame, column_name: str, partition_by: list
) -> DataFrame:
    window = Window.partitionBy(partition_by)
    mean_and_std_dev_df = df.withColumn(
        f"{column_name}_std_dev", stddev(column_name).over(window)
    ).withColumn(f"{column_name}_mean", mean(column_name).over(window))
    return mean_and_std_dev_df


def add_upper_and_lower_boundaries_of_2_std_dev_to_df(
    df: DataFrame, column_name: str
) -> DataFrame:

    boundary_2_stddev_df = df.withColumn(
        f"{column_name}_2_stddev_upper",
        col(f"{column_name}_mean") + (col(f"{column_name}_std_dev") * 2),
    ).withColumn(
        f"{column_name}_2_stddev_lower",
        col(f"{column_name}_mean") - (col(f"{column_name}_std_dev") * 2),
    )
    return boundary_2_stddev_df


def replace_outliers_with_mean_value(df: DataFrame, column_name: str) -> DataFrame:

    return df.withColumn(
        column_name,
        when(
            (col(column_name) > col(f"{column_name}_2_stddev_upper"))
            | (col(column_name) < col(f"{column_name}_2_stddev_lower")),
            col(f"{column_name}_mean"),
        ).otherwise(col(column_name)),
    )


def drop_helper_columns(df: DataFrame) -> DataFrame:
    columns_to_drop = []
    for column_name in df.columns:
        if (
            column_name.endswith("_mean")
            or column_name.endswith("_std_dev")
            or column_name.endswith("_2_stddev_upper")
            or column_name.endswith("_2_stddev_lower")
        ):
            columns_to_drop.append(column_name)
    return df.drop(*columns_to_drop)


def main():
    spark = SparkSession.getActiveSession()
    create_delta_table_if_not_exists(
        spark=spark, table_name=TARGET_TABLE_NAME, schema=TARGET_TABLE_SCHEMA
    )

    # EXTRACT
    bronze_data_df = read_table(spark, INPUT_TABLE_NAME)
    newly_arrived_bronze_df = filter_for_newly_arrived_data(spark, bronze_data_df)
    deduplicated_bronze_df = deduplicate_dataset(newly_arrived_bronze_df)

    # TRANSFORM
    with_mean_and_stddev_df = add_mean_and_std_dev_to_df(
        deduplicated_bronze_df, column_name="power_output", partition_by=["turbine_id"]
    )
    filled_nulls_df = fill_nulls_with_mean_value(
        df=with_mean_and_stddev_df, colum_name="power_output"
    )
    boundaries_2_stddev_df = add_upper_and_lower_boundaries_of_2_std_dev_to_df(
        df=filled_nulls_df, column_name="power_output"
    )
    corrected_outliers_df = replace_outliers_with_mean_value(
        df=boundaries_2_stddev_df, column_name="power_output"
    )
    cleaned_df = drop_helper_columns(df=corrected_outliers_df)

    # LOAD

    merge_all_dataset_into_table(
        spark=spark,
        df=cleaned_df,
        target_table_name=TARGET_TABLE_NAME,
        merge_cols=["timestamp", "turbine_id"],
    )


if __name__ == "__main__":

    main()
    # missing measurements = less than 24 entries per day
    # missing data = power output is null
    # outliers = power output value outside 2 std deviations from mean
    # Q1: deal with data quality for wind speed and direction as well to provide a ready to use dataset for ML?
    # Q2: apply the same condition as power output (ie: outside of 2 std dev) for wind speed and direction?
