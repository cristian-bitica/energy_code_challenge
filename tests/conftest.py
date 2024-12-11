import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session", autouse=True)
def spark_session():

    temp_dir = tempfile.TemporaryDirectory().name
    delta_config = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.warehouse.dir": temp_dir,
    }

    if not SparkSession.getActiveSession():
        builder = (
            SparkSession.builder.master("local[1]")
            .appName("TestScopedSession")
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
