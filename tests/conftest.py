import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import tempfile


@pytest.fixture(scope='session', autouse=True)
def spark_session():
    # if x := SparkSession.getActiveSession():
    #     x.stop()
    builder = (
        SparkSession.builder.master('local[*]').appName("TestScopedSession")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config('spark.driver.host', 'localhost')
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
    )
    configure_spark_with_delta_pip(builder).getOrCreate()
    # SparkSession.builder.appName("TestScopedSession").getOrCreate()
    s = SparkSession.getActiveSession()
    return s