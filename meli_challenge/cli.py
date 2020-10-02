"""cli module."""
from argparse import ArgumentParser
from typing import List

from meli_challenge.core import CharactersGraph
from meli_challenge.core.input import CsvInput


def summarize_interactions(csv_path: str, books: List[int]):
    """Compute the aggregated sum of interactions for all characters in graph.

    The function print all the result records after the computation.

    Args:
        csv_path: path to the csv file to use as input knowledge base for the graph.
        books: list books to use in aggregations.
            Each book will generate a new aggregated column.

    """
    graph = CharactersGraph(knowledge_base_input=CsvInput(path=csv_path))
    result_df = (
        graph.summarize_characters_interactions(books=books)
        .coalesce(1)
        .orderBy("sum_interactions", ascending=False)
    )
    result_dict = result_df.toPandas().to_dict("records")
    for row in result_dict:
        interactions_str = ",".join(
            [str(row[f"sum_interactions_book_{book}"]) for book in books]
            + [str(row["sum_interactions"])]
        )
        print(f"{row['name']}\t{interactions_str}")


def summarize_mutual_friendships(csv_path: str):
    """Compute the set aggregation of mutual friends for all characters in graph.

    The function print all the result records after the computation.

    Args:
        csv_path: path to the csv file to use as input knowledge base for the graph.

    """
    graph = CharactersGraph(knowledge_base_input=CsvInput(path=csv_path))
    result_df = graph.summarize_characters_mutual_friendships()
    result_dict = result_df.toPandas().to_dict("records")
    for row in result_dict:
        common_friends_str = ",".join(row["common_friends"])
        print(f"{row['c1']}\t{row['c2']}\t{common_friends_str}")


if __name__ == "__main__":
    parser = ArgumentParser()

    # Subparser for different execution modes
    subparsers = parser.add_subparsers(help="Desired action to perform", dest="action")

    # Parent argparser for knowledge base input
    parent_parser = ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--csv",
        help="Knowledge base input on CSV format.",
        required=True,
        dest="csv_path",
    )

    # Subparsers for interactions summarization mode
    parser_summarize_interactions = subparsers.add_parser(
        "summarize_interactions",
        parents=[parent_parser],
        help="Display the sum of interactions over defined books for all characters.",
    )
    parser_summarize_interactions.add_argument(
        "--books",
        dest="books",
        help="Book numbers to query for.",
        nargs="+",
        type=int,
        required=True,
    )

    # Subparsers for mutual friendships summarization mode
    parser_summarize_mutual_friendships = subparsers.add_parser(
        "summarize_mutual_friendships",
        parents=[parent_parser],
        help="Display mutual friendships between all pair of characters.",
    )

    # arg parsing
    args = parser.parse_args()
    if args.action == "summarize_interactions":
        summarize_interactions(csv_path=args.csv_path, books=args.books)

    if args.action == "summarize_mutual_friendships":
        summarize_mutual_friendships(csv_path=args.csv_path)
