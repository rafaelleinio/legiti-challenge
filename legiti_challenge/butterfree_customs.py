"""Module containing butterfree's customizations for this project."""
import os
from typing import Any, Dict, Optional

from butterfree.clients import SparkClient
from butterfree.configs.db import MetastoreConfig
from butterfree.hooks import Hook
from butterfree.load.writers.historical_feature_store_writer import (
    HistoricalFeatureStoreWriter,
)
from butterfree.load.writers.writer import Writer
from butterfree.transform import FeatureSet
from pyspark.sql.dataframe import DataFrame


class ZeroFillHook(Hook):
    """Simple Hook for filling null values with zeroes in the aggregated features."""

    def run(self, dataframe: DataFrame) -> DataFrame:
        """Execute the hook."""
        return dataframe.fillna(0)


class NotCheckSchemaHook(Hook):
    """Skipping check schema on OnlineFeatureStoreWriter."""

    def run(self, dataframe: DataFrame) -> DataFrame:
        """Execute the hook."""
        return dataframe


class LocalHistoricalFSConfig(MetastoreConfig):
    """Simplification of MetastoreConfig for writing to local path."""

    def __init__(self):
        super().__init__()

    def get_options(self, key: str) -> Dict[Optional[str], Optional[str]]:
        """Declare options for writing."""
        return {
            "mode": self.mode,
            "format_": self.format_,
            "path": os.path.join("data/feature_store", key),
        }


class LocalHistoricalFSWriter(HistoricalFeatureStoreWriter):
    """Simplification for the HistoricalFeatureStoreWriter for writing to local path."""

    def __init__(self, debug_mode: bool = False):
        super().__init__(
            db_config=LocalHistoricalFSConfig(),
            interval_mode=True,
            debug_mode=debug_mode,
        )

    def validate(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ) -> None:
        """Skipping validating method."""
        pass

    def check_schema(
        self, client: Any, dataframe: DataFrame, table_name: str, database: str = None
    ) -> DataFrame:
        """Skipping check_schema method."""
        return dataframe


class DatasetWriter(Writer):
    """Simplification of feature store Writers for writing datasets."""

    def __init__(self):
        super().__init__()

    def write(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ) -> Any:
        """Write output to single file CSV dataset."""
        path = f"data/datasets/{feature_set.name}"
        spark_client.write_dataframe(
            dataframe=dataframe.coalesce(1),
            format_="csv",
            mode="overwrite",
            path=path,
            header=True,
        )

    def check_schema(
        self, client: Any, dataframe: DataFrame, table_name: str, database: str = None
    ) -> DataFrame:
        """Not checking schema for writing datasets."""
        pass

    def validate(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ) -> Any:
        """Not validating data for writing datasets."""
        pass
