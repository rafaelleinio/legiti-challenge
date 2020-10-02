from meli_challenge.core import SparkClient
from tests.utils import (
    TestMockInput,
    assert_dataframe_equality,
    create_df_from_collection,
)

client = SparkClient()
client.create_session()


class TestInput:
    def test_build(self, spark_session, spark_context):
        # arrange
        input = TestMockInput()
        target_vertices_df = create_df_from_collection(
            data=[{"id": "c1"}, {"id": "c2"}, {"id": "c3"}],
            spark_session=spark_session,
            spark_context=spark_context,
        )
        target_edges_df = create_df_from_collection(
            data=[
                {"src": "c1", "dst": "c2", "weight": 3, "book": 1},
                {"src": "c2", "dst": "c1", "weight": 3, "book": 1},
                {"src": "c3", "dst": "c2", "weight": 3, "book": 3},
                {"src": "c2", "dst": "c3", "weight": 3, "book": 3},
            ],
            spark_session=spark_session,
            spark_context=spark_context,
        )

        # act
        vertices_df, edges_df = input.build(client)

        # assert
        assert_dataframe_equality(vertices_df, target_vertices_df)
        assert_dataframe_equality(edges_df, target_edges_df)
