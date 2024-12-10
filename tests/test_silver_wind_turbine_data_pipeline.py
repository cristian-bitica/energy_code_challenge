from datetime import datetime

import pytest
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pytest import MonkeyPatch
from pytest_mock import MockerFixture

import silver_wind_turbine_data_pipeline
from silver_wind_turbine_data_pipeline import (
    DeltaTable,
    add_mean_and_std_dev_to_df,
    add_upper_and_lower_boundaries_of_2_std_dev_to_df,
    deduplicate_dataset,
    drop_helper_columns,
    fill_nulls_with_mean_value,
    filter_for_newly_arrived_data,
    main,
    merge_dataset_into_table,
    replace_outliers_with_mean_value,
)


@pytest.mark.parametrize(
    argnames="input_data",
    argvalues=[
        [
            [1, datetime(2024, 3, 7, 1, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
            [1, datetime(2024, 3, 7, 2, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
            # duplicate measurement here
            [1, datetime(2024, 3, 7, 2, 0, 0), datetime(2024, 3, 9, 3, 0, 0)],
            [2, datetime(2024, 3, 7, 1, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
            [2, datetime(2024, 3, 7, 2, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
        ],
        [
            [1, datetime(2024, 3, 7, 1, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
            [1, datetime(2024, 3, 7, 2, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
            [2, datetime(2024, 3, 7, 1, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
            [2, datetime(2024, 3, 7, 2, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
        ],
    ],
)
def test_deduplicate_dataset(spark_session, input_data):
    input_df = spark_session.createDataFrame(
        input_data,
        "turbine_id integer, timestamp timestamp, date_created timestamp",
    )
    expected_df = spark_session.createDataFrame(
        [
            [1, datetime(2024, 3, 7, 1, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
            [1, datetime(2024, 3, 7, 2, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
            [2, datetime(2024, 3, 7, 1, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
            [2, datetime(2024, 3, 7, 2, 0, 0), datetime(2024, 3, 8, 2, 0, 0)],
        ],
        "turbine_id integer, timestamp timestamp, date_created timestamp",
    )
    actual_df = deduplicate_dataset(input_df)

    assertDataFrameEqual(actual=actual_df, expected=expected_df)


@pytest.mark.parametrize(
    argnames="date_created",
    argvalues=[
        # test case when table is empty and unix time is returned
        datetime(1970, 1, 1, 2, 0),
        # test case when table is not empty and date_created in silver is higher than bronze
        datetime(2024, 3, 1, 2, 0, 0),
    ],
)
def test_filter_for_newly_arrived_data_target_table_is_empty(
    spark_session, monkeypatch, date_created
):

    monkeypatch.setattr(
        "silver_wind_turbine_data_pipeline.SparkSession.sql",
        lambda x: [date_created],
    )
    input_data = [
        [
            datetime(2022, 3, 1, 1, 0, 0),
            1,
            12.8,
            190,
            2.5,
            datetime(2024, 3, 1, 1, 0, 0),
        ],
        [
            datetime(2022, 3, 1, 1, 0, 0),
            2,
            13.6,
            12,
            2.0,
            datetime(2024, 3, 1, 1, 0, 0),
        ],
        [
            datetime(2022, 3, 1, 1, 0, 0),
            3,
            10.8,
            335,
            2.9,
            datetime(2024, 3, 1, 1, 0, 0),
        ],
    ]
    input_df = spark_session.createDataFrame(
        input_data,
        "timestamp timestamp, turbine_id integer, wind_speed double, wind_direction integer, power_output double, date_created timestamp",
    )

    actual_df = filter_for_newly_arrived_data(spark=spark_session, df=input_df)

    assertDataFrameEqual(actual=actual_df, expected=input_df)


@pytest.mark.parametrize(
    argnames="input_data",
    argvalues=[
        # test case when dataset has nulls that must be replaced
        [[1, 2.5], [2, 2.5], [3, 2.5], [4, 2.5], [5, 2.5], [None, 2.5]],
        # test case when dataset does not have null and nothing should be changed
        [[1, 2.5], [2, 2.5], [3, 2.5], [4, 2.5], [5, 2.5]],
    ],
)
def test_fill_nulls_with_mean_value(spark_session, input_data):

    input_df = spark_session.createDataFrame(
        input_data,
        "power_output integer, power_output_mean double",
    )

    expected_df = spark_session.createDataFrame(
        [[1, 2.5], [2, 2.5], [3, 2.5], [4, 2.5], [5, 2.5], [2.5, 2.5]],
        "power_output integer, power_output_mean double",
    )
    actual_df = fill_nulls_with_mean_value(df=input_df, colum_name="power_output")

    assertDataFrameEqual(actual=actual_df, expected=expected_df)


def test_add_mean_and_std_dev_to_df(spark_session):

    input_data = [
        [1, 2.5],
        [1, 2.7],
        [2, 1.5],
        [2, 3.5],
        [3, 2.5],
    ]
    input_df = spark_session.createDataFrame(
        input_data,
        "turbine_id integer, power_output double",
    )
    expected_df = spark_session.createDataFrame(
        [
            [1, 2.5, 2.6, 0.2],
            [1, 2.7, 2.6, 0.2],
            [2, 1.5, 2.0, 1.0],
            [2, 3.5, 2.0, 1.0],
            [3, 2.5, 2.5, 0.0],
        ],
        "turbine_id integer, power_output double, power_output_mean double, power_output_std_dev double",
    )
    actual_df = add_mean_and_std_dev_to_df(
        df=input_df, column_name="power_output", partition_by=["turbine_id"]
    )
    assertDataFrameEqual(actual=actual_df, expected=expected_df)


def test_add_upper_and_lower_boundaries_of_2_std_dev_to_df(spark_session):

    input_data = [
        [1, 2.5, 2.6, 0.2],
        [1, 2.7, 2.6, 0.2],
        [2, 1.5, 2.0, 1.0],
        [2, 3.5, 2.0, 1.0],
        [3, 2.5, 2.5, 0.0],
    ]
    input_df = spark_session.createDataFrame(
        input_data,
        "turbine_id integer, power_output double, power_output_mean double, power_output_std_dev double",
    )
    expected_df = spark_session.createDataFrame(
        [
            [1, 2.5, 2.6, 0.2, 3.4, 1.4],
            [1, 2.7, 2.6, 0.2, 3.4, 1.4],
            [2, 1.5, 2.0, 1.0, 3.0, 0.0],
            [2, 3.5, 2.0, 1.0, 3.0, 0.0],
            [3, 2.5, 2.5, 0.0, 2.5, 2.5],
        ],
        "turbine_id integer, power_output double, power_output_mean double, power_output_std_dev double, power_output_2_stddev_upper double, power_output_2_stddev_lower double",
    )
    actual_df = add_upper_and_lower_boundaries_of_2_std_dev_to_df(
        df=input_df, column_name="power_output"
    )

    assertDataFrameEqual(actual=actual_df, expected=expected_df)


def test_replace_outliers_with_mean_value_dataset_has_outliers(spark_session):
    input_df = spark_session.createDataFrame(
        [
            [1, 2.5, 2.6, 0.2, 3.4, 1.4],
            [1, 2.7, 2.6, 0.2, 3.4, 1.4],
            [2, 1.5, 2.0, 1.0, 3.0, 0.0],
            # outlier here 3.5 > 3.0 upper boundary
            [2, 3.5, 2.0, 1.0, 3.0, 0.0],
            [3, 2.5, 2.5, 0.0, 2.5, 2.5],
        ],
        "turbine_id integer, power_output double, power_output_mean double, power_output_std_dev double, power_output_2_stddev_upper double, power_output_2_stddev_lower double",
    )
    expected_df = spark_session.createDataFrame(
        [
            [1, 2.5, 2.6, 0.2, 3.4, 1.4],
            [1, 2.7, 2.6, 0.2, 3.4, 1.4],
            [2, 1.5, 2.0, 1.0, 3.0, 0.0],
            # outlier replaced with mean here 3.5 > 3.0 upper boundary
            [2, 2.0, 2.0, 1.0, 3.0, 0.0],
            [3, 2.5, 2.5, 0.0, 2.5, 2.5],
        ],
        "turbine_id integer, power_output double, power_output_mean double, power_output_std_dev double, power_output_2_stddev_upper double, power_output_2_stddev_lower double",
    )

    actual_df = replace_outliers_with_mean_value(
        df=input_df, column_name="power_output"
    )
    assertDataFrameEqual(actual=actual_df, expected=expected_df)


def test_replace_outliers_with_mean_value_dataset_no_outliers(spark_session):
    input_df = spark_session.createDataFrame(
        [
            [1, 2.5, 2.6, 0.2, 3.4, 1.4],
            [1, 2.7, 2.6, 0.2, 3.4, 1.4],
            [2, 1.5, 2.0, 1.0, 3.0, 0.0],
            [2, 2.8, 2.0, 1.0, 3.0, 0.0],
            [3, 2.5, 2.5, 0.0, 2.5, 2.5],
        ],
        "turbine_id integer, power_output double, power_output_mean double, power_output_std_dev double, power_output_2_stddev_upper double, power_output_2_stddev_lower double",
    )
    expected_df = spark_session.createDataFrame(
        [
            [1, 2.5, 2.6, 0.2, 3.4, 1.4],
            [1, 2.7, 2.6, 0.2, 3.4, 1.4],
            [2, 1.5, 2.0, 1.0, 3.0, 0.0],
            [2, 2.8, 2.0, 1.0, 3.0, 0.0],
            [3, 2.5, 2.5, 0.0, 2.5, 2.5],
        ],
        "turbine_id integer, power_output double, power_output_mean double, power_output_std_dev double, power_output_2_stddev_upper double, power_output_2_stddev_lower double",
    )

    actual_df = replace_outliers_with_mean_value(
        df=input_df, column_name="power_output"
    )
    assertDataFrameEqual(actual=actual_df, expected=expected_df)


@pytest.mark.parametrize(
    argnames="dataset_schema",
    argvalues=[
        # test case when dataset columns that must be dropped
        """turbine_id integer, power_output double, power_output_mean double,
        power_output_std_dev double, power_output_2_stddev_upper double, power_output_2_stddev_lower double"""
        # test case when dataset does not have columns to drop
        "turbine_id integer, power_output double"
    ],
)
def test_drop_helper_columns(spark_session, dataset_schema):
    input_df = spark_session.createDataFrame(
        [],
        dataset_schema,
    )
    expected_df = spark_session.createDataFrame(
        [],
        "turbine_id integer, power_output double",
    )

    actual_df = drop_helper_columns(df=input_df)

    assertDataFrameEqual(actual=actual_df, expected=expected_df)


def test_merge_dataset_into_table_hits_expected_calls(
    spark_session, mocker: MockerFixture
):

    input_df = spark_session.createDataFrame(
        [],
        "`timestamp` timestamp, turbine_id integer, power_output double",
    )
    mocked_delta_table = mocker.Mock()
    mocker.patch.object(DeltaTable, "read", mocked_delta_table)

    expected_calls = [
        mocker.call.alias("target"),
        mocker.call.alias().merge(
            input_df,
            "source.turbine_id = target.turbine_id and source.timestamp = target.timestamp",
        ),
        mocker.call.alias().merge().whenMatchedUpdateAll(),
        mocker.call.alias().merge().whenMatchedUpdateAll().whenNotMatchedInsertAll(),
        mocker.call.alias()
        .merge()
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute(),
    ]

    mocked_delta_table.assert_has_calls(expected_calls)


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
        "silver_wind_turbine_data_pipeline.read_table",
        lambda _, _: input_df,
    )

    spark_session.sql(
        f"DROP TABLE IF EXISTS {silver_wind_turbine_data_pipeline.TARGET_TABLE_NAME}"
    )
    spark_session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_wind_turbine_data_pipeline.TARGET_TABLE_NAME}
        (`timestamp` timestamp, turbine_id integer, wind_speed double, wind_direction integer, power_output double, date_created timestamp)
        USING DELTA
        """
    )
    main()

    actual_df = spark_session.read.table(
        silver_wind_turbine_data_pipeline.TARGET_TABLE_NAME
    )
    expected_df = spark_session.createDataFrame(
        input_data,
        "timestamp timestamp, turbine_id integer, wind_speed double, wind_direction integer, power_output double, date_created timestamp",
    )

    assertDataFrameEqual(actual=actual_df, expected=expected_df)
