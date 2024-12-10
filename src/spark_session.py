from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import tempfile

builder = (
    SparkSession.builder.master('local[*]').appName("TestScopedSession")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config('spark.driver.host', 'localhost')
    .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()