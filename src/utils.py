from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def get_spark_session():

    delta_config = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }

    if not SparkSession.getActiveSession():
        builder = (
            SparkSession.builder.master("local[1]")
            .appName("EnergyMonitorApp")
            .config(map=delta_config)
            .config("spark.driver.host", "localhost")
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        session = SparkSession.getActiveSession()
        session.conf.set("spark.sql.extensions", delta_config["spark.sql.extensions"])
        session.conf.set(
            "spark.sql.catalog.spark_catalog",
            delta_config["spark.sql.catalog.spark_catalog"],
        )
    return session


def read_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.table(table_name)


def create_delta_table_if_not_exists(
    spark: SparkSession, table_name: str, schema: str, recreate: bool = False
) -> None:
    if recreate:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema}) USING DELTA")


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
        raise ValueError(
            f"Table {target_table_name} not found. Make sure table name is correct and it exists"
        ) from e

    target_delta_table.alias("target").merge(
        df.alias("source"),
        condition=merge_condition,
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
