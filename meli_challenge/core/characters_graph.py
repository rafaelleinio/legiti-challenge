"""characters_graph module."""

from __future__ import annotations

from typing import List

from meli_challenge.core.constants import EDGE_COLUMNS, VERTEX_COLUMNS
from meli_challenge.core.input import Input
from meli_challenge.core.spark_client import SparkClient
from pyspark.sql import DataFrame, functions


class CharactersGraph:
    """Entity holding the graph abstraction representing characters interactions.

    Args:
        spark_client: Spark client to use to instantiate the Graphframe.
        knowledge_base_input: Input entity to use to import the knowledge base.
            It's essential for the construction of the graph.

    """

    def __init__(self, knowledge_base_input: Input, spark_client: SparkClient = None):
        self.spark_client = spark_client or SparkClient()
        self.spark_client.create_session()

        vertices, edges = knowledge_base_input.build(self.spark_client)
        self.graphframe = self.spark_client.graphframe(vertices, edges)

    def add_vertices(self, vertices_df: DataFrame) -> CharactersGraph:
        """Add in-memory vertices to the Graphframe.

        The method does not add data to the configured input knowledge base.

        Args:
            vertices_df: dataframe with new vertices definitions.

        Returns:
            Rebuilt CharactersGraph with new vertices.

        Raises:
            ValueError: if the vertices_df has an unexpected schema.
        """
        self._check_vertices_df(vertices_df)
        new_vertices = vertices_df.union(self.graphframe.vertices).distinct()
        self.graphframe = self.spark_client.graphframe(
            new_vertices, self.spark_client.graphframe.edges
        )
        return self

    def add_edges(self, edges_df: DataFrame) -> CharactersGraph:
        """Add in-memory edges to the Graphframe.

        The method does not add data to the configured input knowledge base.

        Args:
            edges_df: dataframe with new edges definitions.

        Returns:
            Rebuilt CharactersGraph with new edges.

        Raises:
            ValueError: if the edges_df has an unexpected schema.

        """
        self._check_edges_df(edges_df)
        new_edges = edges_df.union(self.graphframe.edges).distinct()
        self.graphframe = self.spark_client.graphframe(
            self.spark_client.graphframe.vertices, new_edges
        )
        return self

    def summarize_characters_interactions(self, books: List[int]) -> DataFrame:
        """Compute the aggregated sum of interactions for all characters in graph.

        Args:
            books: list books to use in aggregations.
                Each book will generate a new aggregated column.

        Returns:
            Dataframe with the results.

        """
        # summarize isolated vertex interactions
        vertices_df: DataFrame = self.graphframe.vertices
        vertices_with_degrees: DataFrame = self.graphframe.degrees.withColumnRenamed(
            "id", "id2"
        )
        isolated_vertex = (
            vertices_df.join(
                vertices_with_degrees,
                vertices_df.id == vertices_with_degrees.id2,
                "left_outer",
            )
            .where("id2 is null")
            .selectExpr(
                "id",
                "0 as sum_interactions",
                *[f"0 as sum_interactions_book_{book}" for book in books],
            )
        )

        # summarize connected vertex interactions
        connected_vertex = (
            self.graphframe.edges.groupBy("src")
            .agg(
                functions.coalesce(functions.sum("weight"), functions.lit(0)).alias(
                    "sum_interactions"
                ),
                *[
                    functions.sum(
                        functions.coalesce(
                            functions.when(
                                functions.expr(f"book = {book}"),
                                functions.col("weight"),
                            ),
                            functions.lit(0),
                        )
                    ).alias(f"sum_interactions_book_{book}")
                    for book in books
                ],
            )
            .selectExpr(
                "src as name",
                "sum_interactions",
                *[f"sum_interactions_book_{book}" for book in books],
            )
        )

        return connected_vertex.union(isolated_vertex)

    def summarize_characters_mutual_friendships(self) -> DataFrame:
        """Compute the set aggregation of mutual friends for all characters in graph.

        Returns:
            Dataframe with the results.

        """
        return (
            self.graphframe.find("(a)-[e]->(c); (b)-[e2]->(c)")
            .where("b != a")
            .groupBy("a", "b")
            .agg(functions.expr("collect_set(c.id)").alias("common_friends"))
            .selectExpr("a.id as c1", "b.id as c2", "common_friends")
        )

    def get_characters_mutual_friendships(self, c1: str, c2: str) -> DataFrame:
        """Compute all mutual friends between two specific characters.

        Args:
            c1: character 1 name.
            c2: character 2 name.

        Returns:
            Dataframe with results.

        """
        return (
            self.summarize_characters_mutual_friendships()
            .where(f"(lcase(c1) = lcase('{c1}') and lcase(c2) = lcase('{c2}'))")
            .select("common_friends")
        )

    @staticmethod
    def _check_vertices_df(vertices_df):
        if not all(col in VERTEX_COLUMNS for col in vertices_df):
            raise ValueError(
                "vertices_df has a incompatible schema. "
                f"Expected vertex schema = {VERTEX_COLUMNS}"
            )

    @staticmethod
    def _check_edges_df(edges_df):
        if not all(col in EDGE_COLUMNS for col in edges_df):
            raise ValueError(
                "edges_df has a incompatible schema. "
                f"Expected edge schema = {EDGE_COLUMNS}"
            )
