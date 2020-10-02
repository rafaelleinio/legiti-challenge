"""json_input module."""
from meli_challenge.core.constants import KNOWLEDGE_BASE_SCHEMA
from meli_challenge.core.input.input import Input
from meli_challenge.core.spark_client import SparkClient
from pyspark.sql import DataFrame


class JsonInput(Input):
    """Input entity to handle JSON knowledge base source.

    Args:
        path: path to csv file(s).
        stream: flag to trigger the Spark's streaming read instead of batch read.

    """

    def __init__(self, path: str, stream=False):
        self.path = path
        self.stream = stream

    def read(self, spark_client: SparkClient) -> DataFrame:
        """Get data from Json data.

        Is expected for the JSON to have the following keys:
            - source (string): name of character 1.
            - target (string): name of character2.
            - weight (int): number of interactions between both characters.
            - book (int): book number in which these interactions happen.

        Args:
            spark_client: spark client to connect and read from the source.

        Returns:
            Dataframe with columns "source", "target", "weight" and "book"

        """
        schema = ", ".join(
            [
                f"{key} {value['spark_type']}"
                for key, value in KNOWLEDGE_BASE_SCHEMA.items()
            ]
        )
        if self.stream:
            stream_df = spark_client.session.readStream.json(self.path, schema=schema)
            stream_df.writeStream.format("memory").queryName(
                "stream_json_input"
            ).start()
            return spark_client.session.table("stream_json_input")
        return spark_client.session.read.json(self.path, schema=schema)
