"""api module."""
import json
from time import time_ns
from typing import Optional

from flask import Flask, Response, jsonify, request
from meli_challenge.core import CharactersGraph
from meli_challenge.core.constants import KNOWLEDGE_BASE_SCHEMA
from meli_challenge.core.input import JsonInput

app = Flask(__name__)

PATH = "data/server_output"
ALLOWED_BOOKS = [4]

GRAPH = CharactersGraph(knowledge_base_input=JsonInput(path=PATH, stream=True))


def validate_payload(payload_dict: dict) -> Optional[dict]:
    """Validates the payload checking if it has the expected schema."""
    if not (payload_dict.keys() == KNOWLEDGE_BASE_SCHEMA.keys()) or not all(
        type(payload_dict[key]) == KNOWLEDGE_BASE_SCHEMA[key]["python_type"]
        for key in payload_dict.keys()
    ):
        return None
    return payload_dict


def write_payload(payload_dict: dict):
    """Write payload as a new record to the knowledge base path as a new record."""
    file_name = f"{PATH}/{time_ns()}_payload.json"
    with open(file_name, "w") as file:
        json.dump(payload_dict, file)


@app.route("/interaction", methods=["POST"])
def interaction():
    """POST endpoint to register new characters interactions.

    After validating the payload, the function writes (append) it to the same path as
    the configured knowledge base path. As the Input entity in the Graph is configured
    to read in streaming mode, as new records are written to the path they are processed
    and integrated to the Graph in real time.

    Returns:
        Status 201: if everything went ok.
        Status 400: if the payload fails the validations.
        Status 400: if tries to register a "not allowed" book.

    """
    validated_payload_dict = validate_payload(request.json)
    if not validated_payload_dict:
        return Response(
            "Error in payload schema validation.",
            status=400,
            mimetype="application/json",
        )

    if validated_payload_dict["book"] not in ALLOWED_BOOKS:
        return Response(
            f"Only the books {ALLOWED_BOOKS} are allowed.",
            status=400,
            mimetype="application/json",
        )

    write_payload(validated_payload_dict)
    return Response("Ok", status=201, mimetype="application/json",)


@app.route("/common-friends", methods=["GET"])
def common_friends():
    """Endpoint to query the graph for mutual friends between a pair of characters.

    Returns:
        Status 400: if not pass the source and target args.
        Status 200 and result payload: if the query went ok.

    """
    source = request.args.get("source", default=None, type=str)
    target = request.args.get("target", default=None, type=str)
    if (not source) or (not target):
        return Response(
            f"source and target args are required.",
            status=400,
            mimetype="application/json",
        )

    result = (
        GRAPH.get_characters_mutual_friendships(c1=source, c2=target)
        .toPandas()
        .to_dict(orient="records")
    )
    if not result:
        return jsonify({"common_friends": []}), 200
    return jsonify(result[0]), 200


if __name__ == "__main__":
    app.run()
