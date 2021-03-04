from unittest.mock import Mock

from butterfree.clients import SparkClient
from butterfree.testing.dataframe import assert_dataframe_equality
from butterfree.transform import FeatureSet
from legiti_challenge.butterfree_customs import (
    DatasetWriter,
    LocalHistoricalFSConfig,
    LocalHistoricalFSWriter,
    NotCheckSchemaHook,
    ZeroFillHook,
)


def test_zero_fill_hook(spark_session):
    # arrange
    hook = ZeroFillHook()
    input_df = spark_session.sql(
        "select 1 as id, int(null) as orders, int(null) as chargebacks"
    )
    expected_df = spark_session.sql("select 1 as id, 0 as orders, 0 as chargebacks")

    # act
    output_df = hook.run(input_df)

    # assert
    assert_dataframe_equality(output_df, expected_df)


def test_not_check_schema_hook(spark_session):
    # arrange
    hook = NotCheckSchemaHook()
    input_df = spark_session.sql(
        "select 1 as id, int(null) as orders, int(null) as chargebacks"
    )

    # act
    output_df = hook.run(input_df)

    # assert
    assert_dataframe_equality(output_df, input_df)


def test_local_historical_fs_config():
    # arrange
    local_historical_fs_config = LocalHistoricalFSConfig()
    expected_options = {
        "mode": "overwrite",
        "format_": "parquet",
        "path": "data/feature_store/historical/user/user_chargebacks",
    }

    # act
    output_options = local_historical_fs_config.get_options(
        "historical/user/user_chargebacks"
    )

    # assert
    assert output_options == expected_options


def test_local_historical_fs_writer():
    # arrange
    local_fs_writer = LocalHistoricalFSWriter()
    expected_properties = (LocalHistoricalFSConfig().get_options("key"), True, False)

    # act
    output_properties = (
        local_fs_writer.db_config.get_options("key"),
        local_fs_writer.interval_mode,
        local_fs_writer.debug_mode,
    )
    local_fs_writer.validate(0, 0, 0)  # nothing should happen
    local_fs_writer.check_schema(0, 0, 0, 0)  # nothing should happen

    # assert
    assert output_properties == expected_properties
    assert local_fs_writer.__name__ == "Local Historical Feature Store Writer"


def test_dataset_writer(spark_session):
    # arrange
    dataset_writer = DatasetWriter()

    mock_feature_set = Mock(spec=FeatureSet)
    mock_feature_set.name = "feature_set"

    mock_spark_client = Mock(spec=SparkClient)
    mock_spark_client.write_dataframe = Mock()
    mock_spark_client.write_dataframe.return_value = True

    input_df = spark_session.sql("select 1")

    # act
    dataset_writer.check_schema(0, 0, 0, 0)  # nothing should happen
    dataset_writer.write(
        feature_set=mock_feature_set, dataframe=input_df, spark_client=mock_spark_client
    )
    args = mock_spark_client.write_dataframe.call_args[1]

    # assert
    assert_dataframe_equality(args["dataframe"], input_df)
    assert args["format_"] == "csv"
    assert args["mode"] == "overwrite"
    assert args["path"] == "data/datasets/feature_set"
    assert args["header"] is True
    assert dataset_writer.__name__ == "Dataset Writer"
