from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def read_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.table(table_name)


def _build_merge_condition(merge_cols: list[str]) -> str:
    merge_condition = " and ".join(
        [f"source.{col} = target.{col}" for col in merge_cols]
    )
    return merge_condition


def merge_all_dataset_into_table(
    spark: SparkSession, df: DataFrame, target_table_name: str, merge_cols: list[str]
) -> None:

    merge_condition = _build_merge_condition(merge_cols)
    if not merge_condition:
        raise ValueError("No merge columns provided")
    try:
        target_delta_table = DeltaTable.forName(
            sparkSession=spark, tableOrViewName=target_table_name
        )
    except Exception as e:
        raise ValueError(f"Table {target_table_name} not found. Make sure table name is correct and it exists") from e

    target_delta_table.alias("target").merge(
        df.alias("source"),
        condition=merge_condition,
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
