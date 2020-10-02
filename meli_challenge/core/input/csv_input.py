"""csv_input module."""

from meli_challenge.core.input.input import Input
from meli_challenge.core.spark_client import SparkClient
from pyspark.sql import DataFrame


class CsvInput(Input):
    """Input entity to handle CSV knowledge base source.

    Args:
        path: path to csv file(s).

    """

    def __init__(self, path: str):
        self.path = path

    def read(self, spark_client: SparkClient) -> DataFrame:
        """Get data from csv source.

        Is expected for the csv to be headless and to have the following ordered schema:
            - source (string): name of character 1.
            - target (string): name of character2.
            - weight (int): number of interactions between both characters.
            - book (int): book number in which these interactions happen.

        Args:
            spark_client: spark client to connect and read from the source.

        Returns:
            Dataframe with columns "source", "target", "weight" and "book"

        """
        return spark_client.session.read.csv(
            self.path,
            schema="source string, target string, weight int, book int",
            header=False,
        )
