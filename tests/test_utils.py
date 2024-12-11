import pytest
from pytest_mock import MockerFixture

from utils import (
    DeltaTable,
    _build_merge_condition,
    merge_all_dataset_into_table,
)


def test__build_merge_condition_single_col():
    assert (
        _build_merge_condition(["turbine_id"])
        == "source.turbine_id = target.turbine_id"
    )


def test__build_merge_condition_multiple_cols():
    assert (
        _build_merge_condition(["turbine_id", "timestamp"])
        == "source.turbine_id = target.turbine_id and source.timestamp = target.timestamp"
    )


def test__build_merge_condition_with_empty_list():
    assert _build_merge_condition([]) == ""


def test_merge_all_dataset_into_table_raises_value_error_missing_merge_cols(
    spark_session,
):
    input_df = spark_session.createDataFrame(
        [],
        "`timestamp` timestamp, turbine_id integer, power_output double",
    )

    with pytest.raises(ValueError):
        merge_all_dataset_into_table(
            spark=spark_session, df=input_df, target_table_name="", merge_cols=[]
        )


def test_merge_all_dataset_into_table_raises_value_error_table_not_found(spark_session):
    input_df = spark_session.createDataFrame(
        [],
        "`timestamp` timestamp, turbine_id integer, power_output double",
    )

    with pytest.raises(ValueError):
        merge_all_dataset_into_table(
            spark=spark_session,
            df=input_df,
            target_table_name="",
            merge_cols=["turbine_id"],
        )


def test_merge_all_dataset_into_table_hits_expected_calls(
    spark_session, mocker: MockerFixture, monkeypatch
):
    monkeypatch.setattr(
        "utils._build_merge_condition",
        lambda x: 'source.turbine_id = target.turbine_id and source.timestamp = target.timestamp'
    )
    input_df = spark_session.createDataFrame(
        [],
        "`timestamp` timestamp, turbine_id integer, power_output double",
    )
    mocked_delta_table = mocker.Mock()
    mocker.patch.object(DeltaTable, "forName", lambda sparkSession, tableOrViewName: mocked_delta_table)

    merge_all_dataset_into_table(
        spark=spark_session,
        df=input_df,
        target_table_name="dummy_table",
        merge_cols=["turbine_id", "timestamp"]
    )

    expected_calls = [
        mocker.call.alias('target'),
        mocker.call.alias().merge(
            input_df,
            condition='source.turbine_id = target.turbine_id and source.timestamp = target.timestamp'
        ),
        mocker.call.alias().merge().whenMatchedUpdateAll(),
        mocker.call.alias().merge().whenMatchedUpdateAll().whenNotMatchedInsertAll(),
        mocker.call.alias().merge().whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    ]

    mocked_delta_table.assert_has_calls(expected_calls)
