"""CLI module with all the commands implementations."""
from pprint import pformat

import click
from butterfree.reports.metadata import Metadata
from legiti_challenge.dataset_pipelines import AwesomeDatasetPipeline
from legiti_challenge.feature_store_pipelines.user import (
    UserChargebacksPipeline,
    UserOrdersPipeline,
)
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAll(
    [
        ("spark.sql.session.timeZone", "UTC"),
        ("spark.sql.sources.partitionOverwriteMode", "dynamic"),
    ]
)
conf.set("spark.logConf", "true")

pipelines = {
    "feature_store.user_orders": UserOrdersPipeline(),
    "feature_store.user_chargebacks": UserChargebacksPipeline(),
    "dataset.awesome_dataset": AwesomeDatasetPipeline(),
}


def create_spark_session():
    """Creates the Spark session."""
    return (
        SparkSession.builder.config(conf=conf).appName("legiti-challenge").getOrCreate()
    )


@click.group(context_settings=dict(max_content_width=250))
def cli():
    """All you need for running you feature store pipelines!"""


@cli.command()
@click.argument("pipeline-name", type=click.STRING, required=True)
@click.option(
    "--start-date",
    type=click.STRING,
    default=None,
    help="Lower time bound reference for the execution.",
)
@click.option(
    "--end-date",
    type=click.STRING,
    default=None,
    help="Upper time bound reference for the execution.",
)
def execute(pipeline_name: str, start_date: str, end_date: str):
    """Executes a defined pipeline."""
    click.echo(f">>> {pipeline_name} pipeline execution initiated...")
    spark = create_spark_session()
    pipelines[pipeline_name].run(start_date=start_date, end_date=end_date)
    click.echo(">>> Pipeline execution finished!!!")

    pipeline_type, pipeline_title = pipeline_name.split(".")
    if pipeline_type == "feature_store":
        click.echo(">>> Virtual Online Feature Store result:")
        spark.table(f"online_feature_store__{pipeline_title}").orderBy(
            "timestamp"
        ).show()
        click.echo(
            ">>> Local Historical Feature Store results at data/feature_store/"
            f"historical/user/{pipeline_title}"
        )
        return
    click.echo(f">>> Dataset results at data/datasets/{pipeline_title}")


@cli.command()
def list_pipelines():
    """List all available pipelines to execute."""
    click.echo(f"The available pipelines are the following:\n{list(pipelines.keys())}")


@cli.command()
@click.argument("pipeline-name", type=click.STRING, required=True)
def describe(pipeline_name):
    """Show pipeline details and metadata."""
    output = pformat(Metadata(pipelines[pipeline_name]).to_json())
    click.echo(f"Pipeline definition:\n{output}")


if __name__ == "__main__":
    cli()  # pragma: no cover
