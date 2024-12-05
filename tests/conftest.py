import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope='session')
def spark_session():
    if x := SparkSession.getActiveSession():
        x.stop()
    builder = SparkSession.builder.appName("TestScopedSession") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('spark.driver.host', 'localhost')
    configure_spark_with_delta_pip(builder).getOrCreate()
    # SparkSession.builder.appName("TestScopedSession").getOrCreate()
    s = SparkSession.getActiveSession()
    return s