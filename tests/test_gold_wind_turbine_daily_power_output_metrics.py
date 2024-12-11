from datetime import datetime

from pyspark.testing import assertDataFrameEqual
from pytest import MonkeyPatch

import gold_wind_turbine_daily_power_output_metrics
from gold_wind_turbine_daily_power_output_metrics import (
    add_daily_power_output_metrics,
    add_date_column,
    main,
)


def test_add_date_column(spark_session):
    input_data = [
        [datetime(2024, 1, 1, 0, 0, 0), 2.3],
        [datetime(2024, 1, 1, 1, 0, 0), 2.2],
        [datetime(2024, 1, 1, 2, 0, 0), 2.5],
    ]
    input_df = spark_session.createDataFrame(
        input_data, "`timestamp` timestamp, power_output double"
    )
    expected_data = [
        [datetime(2024, 1, 1, 0, 0, 0), 2.3, datetime(2024, 1, 1)],
        [datetime(2024, 1, 1, 1, 0, 0), 2.2, datetime(2024, 1, 1)],
        [datetime(2024, 1, 1, 2, 0, 0), 2.5, datetime(2024, 1, 1)],
    ]
    expected_df = spark_session.createDataFrame(
        expected_data, "`timestamp` timestamp, power_output double, date date"
    )
    actual_df = add_date_column(input_df)
    assertDataFrameEqual(actual_df, expected_df)


def test_add_daily_power_output_metrics(spark_session):
    input_data = [
        [datetime(2024, 1, 1), 1, 2.3],
        [datetime(2024, 1, 1), 1, 2.2],
        [datetime(2024, 1, 1), 1, 2.5],
        [datetime(2024, 1, 1), 2, 1.8],
        [datetime(2024, 1, 1), 2, 1.5],
        [datetime(2024, 1, 1), 2, 2.1],
        [datetime(2024, 1, 2), 1, 3.1],
        [datetime(2024, 1, 2), 1, 3.4],
        [datetime(2024, 1, 2), 1, 2.5],
        [datetime(2024, 1, 2), 2, 4.0],
        [datetime(2024, 1, 2), 2, 3.8],
        [datetime(2024, 1, 2), 2, 4.3],
    ]
    input_df = spark_session.createDataFrame(
        input_data, "`date` date, turbine_id integer, power_output double"
    )
    expected_data = [
        [1, datetime(2024, 1, 1), 2.2, 2.5, 2.33],
        [2, datetime(2024, 1, 1), 1.5, 2.1, 1.8],
        [1, datetime(2024, 1, 2), 2.5, 3.4, 3.0],
        [2, datetime(2024, 1, 2), 3.8, 4.3, 4.03],
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        "turbine_id integer, `date` date, power_output_daily_min double, power_output_daily_max double, power_output_daily_mean double",
    )
    actual_df = add_daily_power_output_metrics(input_df)
    assertDataFrameEqual(actual_df, expected_df)


def test_main_e2e(spark_session, monkeypatch: MonkeyPatch):
    input_data = [
        [
            datetime(2022, 3, 1, 1, 0, 0),
            1,
            12.8,
            190,
            2.5,
            datetime(2022, 3, 2, 1, 0, 0),
        ],
        [
            datetime(2022, 3, 1, 1, 0, 0),
            2,
            13.6,
            12,
            2.0,
            datetime(2022, 3, 2, 1, 0, 0),
        ],
        [
            datetime(2022, 3, 1, 1, 0, 0),
            3,
            10.8,
            335,
            2.9,
            datetime(2022, 3, 2, 1, 0, 0),
        ],
    ]
    input_df = spark_session.createDataFrame(
        input_data,
        "timestamp timestamp, turbine_id integer, wind_speed double, wind_direction integer, power_output double, date_created timestamp",
    )

    monkeypatch.setattr(
        "gold_wind_turbine_daily_power_output_metrics.read_table",
        lambda x, y: input_df,
    )

    spark_session.sql(
        f"DROP TABLE IF EXISTS {gold_wind_turbine_daily_power_output_metrics.TARGET_TABLE_NAME}"
    )
    spark_session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_wind_turbine_daily_power_output_metrics.TARGET_TABLE_NAME}
        (turbine_id integer, `date` date, power_output_daily_min double, power_output_daily_max double, power_output_daily_mean double)
        USING DELTA
        """
    )
    main()

    actual_df = spark_session.read.table(
        gold_wind_turbine_daily_power_output_metrics.TARGET_TABLE_NAME
    )
    expected_data = [
        [1, datetime(2022, 3, 1), 2.5, 2.5, 2.5],
        [2, datetime(2022, 3, 1), 2.0, 2.0, 2.0],
        [3, datetime(2022, 3, 1), 2.9, 2.9, 2.9],
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        "turbine_id integer, `date` date, power_output_daily_min double, power_output_daily_max double, power_output_daily_mean double",
    )

    assertDataFrameEqual(actual=actual_df, expected=expected_df)
