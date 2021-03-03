"""Module defining awesome dataset pipeline."""
from butterfree.constants.data_type import DataType
from butterfree.extract import Source
from butterfree.extract.readers import FileReader
from butterfree.load import Sink
from butterfree.pipelines.feature_set_pipeline import FeatureSetPipeline
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature

from legiti_challenge.butterfree_customs import DatasetWriter


class AwesomeDatasetPipeline(FeatureSetPipeline):
    """Pipeline definition for building the Awesome Dataset."""

    def __init__(self):
        super(AwesomeDatasetPipeline, self).__init__(
            source=Source(
                readers=[
                    FileReader(
                        id="order_events",
                        path="data/order_events/input.csv",
                        format="csv",
                        format_options={"header": True},
                    ),
                    FileReader(
                        id="user_chargebacks",
                        path="data/feature_store/historical/user/user_chargebacks",
                        format="parquet",
                    ),
                    FileReader(
                        id="user_orders",
                        path="data/feature_store/historical/user/user_orders",
                        format="parquet",
                    ),
                ],
                query="""
with feature_sets_merge as(
    select
        user_orders.cpf,
        user_orders.timestamp,
        user_chargebacks.timestamp as chargeback_timestamp,
        cpf_orders__count_over_3_days_rolling_windows,
        cpf_orders__count_over_7_days_rolling_windows,
        cpf_orders__count_over_30_days_rolling_windows,
        cpf_chargebacks__count_over_3_days_rolling_windows,
        cpf_chargebacks__count_over_7_days_rolling_windows,
        cpf_chargebacks__count_over_30_days_rolling_windows,
        row_number() over (
            partition by (user_orders.cpf, user_orders.timestamp)
            order by user_chargebacks.timestamp desc
        ) as rn
    from
        user_orders
        left join user_chargebacks
            on  user_orders.cpf = user_chargebacks.cpf
            and user_orders.timestamp >= user_chargebacks.timestamp
),
feature_sets_rn_filter as(
    select
        *
    from
        feature_sets_merge
    where
        rn = 1
),
orders_with_feature_sets as(
    select
        order_events.order_id,
        timestamp(order_events.order_timestamp) as timestamp,
        timestamp(order_events.chargeback_timestamp) as chargeback_timestamp,
        order_events.cpf,
        feature_sets_rn_filter.cpf_orders__count_over_3_days_rolling_windows,
        feature_sets_rn_filter.cpf_orders__count_over_7_days_rolling_windows,
        feature_sets_rn_filter.cpf_orders__count_over_30_days_rolling_windows,
        feature_sets_rn_filter.cpf_chargebacks__count_over_3_days_rolling_windows,
        feature_sets_rn_filter.cpf_chargebacks__count_over_7_days_rolling_windows,
        feature_sets_rn_filter.cpf_chargebacks__count_over_30_days_rolling_windows,
        row_number() over (
            partition by (order_events.cpf, order_events.order_timestamp)
            order by feature_sets_rn_filter.timestamp desc
        ) as rn
    from
        order_events
        join feature_sets_rn_filter
            on order_events.cpf = feature_sets_rn_filter.cpf
            and timestamp(order_events.order_timestamp) >=
            feature_sets_rn_filter.timestamp
)
select
    order_id,
    timestamp,
    chargeback_timestamp,
    cpf,
    cpf_orders__count_over_3_days_rolling_windows,
    cpf_orders__count_over_7_days_rolling_windows,
    cpf_orders__count_over_30_days_rolling_windows,
    coalesce(
        cpf_chargebacks__count_over_3_days_rolling_windows,
    0) as cpf_chargebacks__count_over_3_days_rolling_windows,
    coalesce(
        cpf_chargebacks__count_over_7_days_rolling_windows,
    0) as cpf_chargebacks__count_over_7_days_rolling_windows,
    coalesce(
        cpf_chargebacks__count_over_30_days_rolling_windows,
    0) as cpf_chargebacks__count_over_30_days_rolling_windows
from
    orders_with_feature_sets
where
    rn = 1
                """,
            ),
            feature_set=FeatureSet(
                name="awesome_dataset",
                entity="user",
                description="Dataset enriching orders events with aggregated features "
                "on total of orders and chargebacks by user.",
                keys=[
                    KeyFeature(
                        name="order_id",
                        description="Orders unique identifier.",
                        dtype=DataType.STRING,
                    )
                ],
                timestamp=TimestampFeature(),
                features=[
                    Feature(
                        name="chargeback_timestamp",
                        description="Timestamp for the order creation.",
                        dtype=DataType.TIMESTAMP,
                    ),
                    Feature(
                        name="cpf",
                        description="User unique identifier, user entity key.",
                        dtype=DataType.STRING,
                    ),
                    Feature(
                        name="cpf_orders__count_over_3_days_rolling_windows",
                        description="Count of orders over 3 days rolling windows group "
                        "by user (identified by CPF)",
                        dtype=DataType.INTEGER,
                    ),
                    Feature(
                        name="cpf_orders__count_over_7_days_rolling_windows",
                        description="Count of orders over 7 days rolling windows group "
                        "by user (identified by CPF)",
                        dtype=DataType.INTEGER,
                    ),
                    Feature(
                        name="cpf_orders__count_over_30_days_rolling_windows",
                        description="Count of orders over 30 days rolling windows group"
                        " by user (identified by CPF)",
                        dtype=DataType.INTEGER,
                    ),
                    Feature(
                        name="cpf_chargebacks__count_over_3_days_rolling_windows",
                        description="Count of chargebacks over 3 days rolling windows "
                        "group by user (identified by CPF)",
                        dtype=DataType.INTEGER,
                    ),
                    Feature(
                        name="cpf_chargebacks__count_over_7_days_rolling_windows",
                        description="Count of chargebacks over 7 days rolling windows "
                        "group by user (identified by CPF)",
                        dtype=DataType.INTEGER,
                    ),
                    Feature(
                        name="cpf_chargebacks__count_over_30_days_rolling_windows",
                        description="Count of chargebacks over 30 days rolling windows "
                        "group by user (identified by CPF)",
                        dtype=DataType.INTEGER,
                    ),
                ],
            ),
            sink=Sink(writers=[DatasetWriter()]),
        )
