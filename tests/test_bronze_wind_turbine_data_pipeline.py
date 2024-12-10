import csv
import tempfile
from datetime import datetime

import pytest
from pyspark.sql.functions import lit
from pyspark.testing.utils import assertDataFrameEqual
from pytest import MonkeyPatch

import bronze_wind_turbine_data_pipeline as test_subject


@pytest.fixture(scope="module")
def temp_csv_file_path():
    data = [
        ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"],
        ["2022-03-01 00:00:00", 1, 11.8, 169, 2.7],
        ["2022-03-01 00:00:00", 2, 11.6, 24, 2.2],
        ["2022-03-01 00:00:00", 3, 13.8, 335, 2.3],
    ]

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
        csv.writer(f).writerows(data)
        f.flush()
        f.close()
    yield f.name


def test_read_csv_with_header_and_infered_schema(spark_session, temp_csv_file_path):

    # WHEN
    actual_df = test_subject.read_csv_with_header_and_infered_schema(
        spark_session, temp_csv_file_path
    )
    # THEN
    expected_data = [
        [datetime(2022, 3, 1, 0, 0, 0), 1, 11.8, 169, 2.7],
        [datetime(2022, 3, 1, 0, 0, 0), 2, 11.6, 24, 2.2],
        [datetime(2022, 3, 1, 0, 0, 0), 3, 13.8, 335, 2.3],
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        "timestamp timestamp, turbine_id integer, wind_speed double, wind_direction integer, power_output double",
    )
    assertDataFrameEqual(actual=actual_df, expected=expected_df)


def test_append_dataset_to_delta_table(spark_session):
    # GIVEN

    table_name = "bronze_wind_turbine_data"

    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark_session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (`timestamp` timestamp, turbine_id integer, wind_speed double, wind_direction integer, power_output double)
        USING DELTA
        """
    )
    spark_session.sql(
        f"""
        INSERT INTO {table_name} VALUES
        (to_timestamp('2022-03-01 00:00:00'), 1, 11.8, 169, 2.7),
        (to_timestamp('2022-03-01 00:00:00'), 2, 11.6, 24, 2.2),
        (to_timestamp('2022-03-01 00:00:00'), 3, 13.8, 335, 2.3)
        """
    )

    # WHEN
    input_data = [
        [datetime(2022, 3, 1, 1, 0, 0), 1, 12.8, 190, 2.5],
        [datetime(2022, 3, 1, 1, 0, 0), 2, 13.6, 12, 2.0],
        [datetime(2022, 3, 1, 1, 0, 0), 3, 10.8, 335, 2.9],
    ]
    input_df = spark_session.createDataFrame(
        input_data,
        "timestamp timestamp, turbine_id integer, wind_speed double, wind_direction integer, power_output double",
    )
    test_subject.append_dataset_to_delta_table(input_df, table_name)
    # THEN
    expected_data = [
        [datetime(2022, 3, 1, 0, 0, 0), 1, 11.8, 169, 2.7],
        [datetime(2022, 3, 1, 0, 0, 0), 2, 11.6, 24, 2.2],
        [datetime(2022, 3, 1, 0, 0, 0), 3, 13.8, 335, 2.3],
        [datetime(2022, 3, 1, 1, 0, 0), 1, 12.8, 190, 2.5],
        [datetime(2022, 3, 1, 1, 0, 0), 2, 13.6, 12, 2.0],
        [datetime(2022, 3, 1, 1, 0, 0), 3, 10.8, 335, 2.9],
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        "timestamp timestamp, turbine_id integer, wind_speed double, wind_direction integer, power_output double",
    ).orderBy("timestamp")
    actual_df = spark_session.read.table(table_name).orderBy("timestamp")
    assertDataFrameEqual(actual=actual_df, expected=expected_df)
    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_main(spark_session, temp_csv_file_path, monkeypatch):

    created_date = datetime(2024, 12, 5, 1, 30, 0)
    monkeypatch.setattr(
        "bronze_wind_turbine_data_pipeline.current_timestamp",
        lambda: lit(created_date).cast("timestamp"),
    )
    test_subject.INPUT_DATA_LOCATION = temp_csv_file_path
    test_subject.main()
    actual_df = spark_session.read.table(test_subject.TARGET_TABLE_NAME)

    expected_data = [
        [datetime(2022, 3, 1, 0, 0, 0), 1, 11.8, 169, 2.7, created_date],
        [datetime(2022, 3, 1, 0, 0, 0), 2, 11.6, 24, 2.2, created_date],
        [datetime(2022, 3, 1, 0, 0, 0), 3, 13.8, 335, 2.3, created_date],
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        "timestamp timestamp, turbine_id integer, wind_speed double, wind_direction integer, power_output double, date_created timestamp",
    )
    assertDataFrameEqual(actual=actual_df, expected=expected_df)
