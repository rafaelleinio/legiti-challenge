"""SparkClient entity."""
import os
from typing import Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkClient:
    """Handle Spark session connection."""

    def __init__(self) -> None:
        self._session: Optional[SparkSession] = None

    @property
    def session(self) -> SparkSession:
        """Get a created an SparkSession.

        Returns:
            Spark session.

        Raises:
            AttributeError: if the session is not created yet.

        """
        if not self._session:
            raise AttributeError("Please create session first.")
        return self._session

    def create_session(self):
        """Create or get a live Spark Session for the SparkClient.

        When creating the session the function installs the Graphframes extension if not
        presented.

        """
        if not self._session:
            os.environ[
                "PYSPARK_SUBMIT_ARGS"
            ] = "--packages graphframes:graphframes:0.8.1-spark2.4-s_2.11 pyspark-shell"
            conf = SparkConf()
            conf.set("spark.logConf", "true")
            self._session = (
                SparkSession.builder.config(conf=conf)
                .appName("meli-challenge")
                .getOrCreate()
            )
            self._session.sparkContext.setLogLevel("ERROR")

            from graphframes import GraphFrame  # noqa

            self.graphframe = GraphFrame
