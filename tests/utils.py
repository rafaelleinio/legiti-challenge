from json import dumps
from typing import List

from legiti_challenge.core import SparkClient
from legiti_challenge.core.input import Input
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

_client = SparkClient()
_client.create_session()

SPARK_SESSION = _client.session
SPARK_CONTEXT = _client.session.sparkContext


class TestMockInput(Input):
    def read(self, spark_client: SparkClient) -> DataFrame:
        return create_df_from_collection(
            data=[
                {"source": "c1", "target": "c2", "weight": 3, "book": 1},
                {"source": "c3", "target": "c2", "weight": 3, "book": 3},
            ],
            spark_session=SPARK_SESSION,
            spark_context=SPARK_CONTEXT,
        )


def assert_dataframe_equality(output_df: DataFrame, target_df: DataFrame):
    """Dataframe comparison method."""
    if not (
        output_df.count() == target_df.count()
        and len(target_df.columns) == len(output_df.columns)
    ):
        raise AssertionError(
            f"DataFrame shape mismatch: \n"
            f"output_df shape: {len(output_df.columns)} columns and "
            f"{output_df.count()} rows\n"
            f"target_df shape: {len(target_df.columns)} columns and "
            f"{target_df.count()} rows."
        )

    select_cols = [col(c) for c in output_df.schema.fieldNames()]

    output_data = sorted(output_df.select(*select_cols).collect())
    output_data = [row.asDict(recursive=True) for row in output_data]

    target_data = sorted(target_df.select(*select_cols).collect())
    target_data = [row.asDict(recursive=True) for row in target_data]

    if not output_data == target_data:
        raise AssertionError(
            "DataFrames have different values:\n"
            f"output_df records: {output_data}\n"
            f"target_data records: {target_data}."
        )


def create_df_from_collection(
    data: List[dict],
    spark_context: SparkContext,
    spark_session: SparkSession,
    schema=None,
):
    """Creates a dataframe from a list of dicts."""
    return spark_session.read.json(
        spark_context.parallelize(data, 1).map(lambda x: dumps(x)), schema=schema
    )
