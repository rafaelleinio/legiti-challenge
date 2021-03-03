"""Module defining pipeline for the user orders feature set."""
from butterfree.constants.data_type import DataType
from butterfree.extract import Source
from butterfree.extract.readers import FileReader
from butterfree.load import Sink
from butterfree.load.writers import OnlineFeatureStoreWriter
from butterfree.pipelines import FeatureSetPipeline
from butterfree.transform.aggregated_feature_set import AggregatedFeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import AggregatedTransform
from butterfree.transform.utils.function import Function
from legiti_challenge.butterfree_customs import (
    LocalHistoricalFSWriter,
    NotCheckSchemaHook,
    ZeroFillHook,
)
from pyspark.sql import functions


class UserOrdersPipeline(FeatureSetPipeline):
    """Feature set declaration for user_orders from user entity."""

    def __init__(self):
        super(UserOrdersPipeline, self).__init__(
            source=Source(
                readers=[
                    FileReader(
                        id="order_events",
                        path="data/order_events/input.csv",
                        format="csv",
                        format_options={"header": True},
                    )
                ],
                query=(
                    """
                    select
                        cpf,
                        timestamp(order_timestamp) as timestamp,
                        order_id
                    from
                        order_events
                    """
                ),
            ),
            feature_set=AggregatedFeatureSet(
                name="user_orders",
                entity="user",
                description="Aggregates the total of orders from users in different "
                "time windows.",
                keys=[
                    KeyFeature(
                        name="cpf",
                        description="User unique identifier, entity key.",
                        dtype=DataType.STRING,
                    )
                ],
                timestamp=TimestampFeature(),
                features=[
                    Feature(
                        name="cpf_orders",
                        description="Total of offers registered on user's CPF",
                        transformation=AggregatedTransform(
                            functions=[Function(functions.count, DataType.INTEGER)]
                        ),
                        from_column="order_id",
                    ),
                ],
            )
            .with_windows(definitions=["3 days", "7 days", "30 days"])
            .add_post_hook(ZeroFillHook()),
            sink=Sink(
                writers=[
                    LocalHistoricalFSWriter(),
                    OnlineFeatureStoreWriter(
                        interval_mode=True,
                        check_schema_hook=NotCheckSchemaHook(),
                        debug_mode=True,
                    ),
                ]
            ),
        )
