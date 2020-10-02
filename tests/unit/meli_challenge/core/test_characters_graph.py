from meli_challenge.core import CharactersGraph, SparkClient
from tests.utils import (
    TestMockInput,
    assert_dataframe_equality,
    create_df_from_collection,
)

client = SparkClient()
client.create_session()


class TestCharactersGraph:
    def test_summarize_characters_interactions(self, spark_session, spark_context):
        # arrange
        graph = CharactersGraph(knowledge_base_input=TestMockInput())
        target_df = create_df_from_collection(
            data=[
                {
                    "name": "c1",
                    "sum_interactions": 3,
                    "sum_interactions_book_1": 3,
                    "sum_interactions_book_2": 0,
                    "sum_interactions_book_3": 0,
                },
                {
                    "name": "c2",
                    "sum_interactions": 6,
                    "sum_interactions_book_1": 3,
                    "sum_interactions_book_2": 0,
                    "sum_interactions_book_3": 3,
                },
                {
                    "name": "c3",
                    "sum_interactions": 3,
                    "sum_interactions_book_1": 0,
                    "sum_interactions_book_2": 0,
                    "sum_interactions_book_3": 3,
                },
            ],
            spark_session=spark_session,
            spark_context=spark_context,
        )

        # act
        output_df = graph.summarize_characters_interactions(books=[1, 2, 3])

        # assert
        assert_dataframe_equality(output_df, target_df)

    def test_summarize_characters_mutual_friendships(
        self, spark_session, spark_context
    ):
        # arrange
        graph = CharactersGraph(knowledge_base_input=TestMockInput())
        target_df = create_df_from_collection(
            data=[
                {"c1": "c3", "c2": "c1", "common_friends": ["c2"]},
                {"c1": "c1", "c2": "c3", "common_friends": ["c2"]},
            ],
            spark_session=spark_session,
            spark_context=spark_context,
        )

        # act
        output_df = graph.summarize_characters_mutual_friendships()

        # assert
        assert_dataframe_equality(output_df, target_df)

    def test_get_characters_mutual_friendships(self, spark_session, spark_context):
        # arrange
        graph = CharactersGraph(knowledge_base_input=TestMockInput())
        target_df = create_df_from_collection(
            data=[{"common_friends": ["c2"]}],
            spark_session=spark_session,
            spark_context=spark_context,
        )

        # act
        output_df = graph.get_characters_mutual_friendships(c1="c3", c2="c1")

        # assert
        assert_dataframe_equality(output_df, target_df)
