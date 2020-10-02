"""input module."""

from abc import ABC, abstractmethod
from typing import Tuple

from meli_challenge.core.spark_client import SparkClient
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

INPUT_SCHEMA = ["source", "target", "weight", "book"]


class Input(ABC):
    """Abstract Input entity."""

    @abstractmethod
    def read(self, spark_client: SparkClient) -> DataFrame:
        """Get data from defined source.

        Args:
            spark_client: spark client to connect and read from the source.

        Returns:
            Dataframe with columns "source", "target", "weight" and "book".

        """

    def build(self, spark_client: SparkClient) -> Tuple[DataFrame, DataFrame]:
        """Build vertices and edges dataframes from defined data source.

        Args:
            spark_client: spark client to connect and read from the source.

        Returns:
            vertices: Dataframe with characters names.
                Columns: id.
            edges: Dataframe with characters relationships.
                Columns: src, dst, weight, book

        """
        input_df = self.read(spark_client)
        input_df_schema = input_df.columns
        if not all(col in input_df_schema for col in INPUT_SCHEMA):
            raise ValueError(
                f"input_df don't have target schema. Expected schema: {INPUT_SCHEMA}"
            )

        vertices = (
            input_df.select("source")
            .union(input_df.select("target"))
            .withColumnRenamed("source", "id")
            .distinct()
        )

        edges = input_df.withColumnRenamed("source", "src").withColumnRenamed(
            "target", "dst"
        )
        inverted_edges = (
            edges.withColumn("src_", col("src"))
            .withColumn("src", col("dst"))
            .withColumn("dst", col("src_"))
            .drop("src_")
        )

        return vertices, edges.union(inverted_edges)
