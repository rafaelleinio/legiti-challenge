import pytest
from meli_challenge.core import SparkClient
from pyspark.sql.session import SparkSession


class TestSparkClient:
    def test_create_session(self):
        # arrange
        spark_client = SparkClient()

        # act
        spark_client.create_session()
        session = spark_client.session

        # assert
        assert isinstance(session, SparkSession)

    def test_session_without_create(self):
        # arrange
        spark_client = SparkClient()

        # act and assert
        with pytest.raises(AttributeError, match="Please create session first."):
            _ = spark_client.session
