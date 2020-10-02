from meli_challenge.core import CharactersGraph, SparkClient
from meli_challenge.core.input import CsvInput
from tests.utils import assert_dataframe_equality, create_df_from_collection

client = SparkClient()
client.create_session()


def test_core_top_interactions(spark_context, spark_session):
    # arrange
    graph = CharactersGraph(knowledge_base_input=CsvInput(path="data/dataset.csv"))
    target_df = create_df_from_collection(
        data=[
            {
                "name": "Tyrion-Lannister",
                "sum_interactions": 2261,
                "sum_interactions_book_1": 650,
                "sum_interactions_book_2": 829,
                "sum_interactions_book_3": 782,
            },
            {
                "name": "Jon-Snow",
                "sum_interactions": 1900,
                "sum_interactions_book_1": 784,
                "sum_interactions_book_2": 360,
                "sum_interactions_book_3": 756,
            },
            {
                "name": "Joffrey-Baratheon",
                "sum_interactions": 1649,
                "sum_interactions_book_1": 422,
                "sum_interactions_book_2": 629,
                "sum_interactions_book_3": 598,
            },
        ],
        spark_session=spark_session,
        spark_context=spark_context,
    )

    # act
    output_df = (
        graph.summarize_characters_interactions(books=[1, 2, 3])
        .coalesce(1)
        .orderBy("sum_interactions", ascending=False)
        .limit(3)
    )

    # assert
    assert_dataframe_equality(output_df, target_df)
