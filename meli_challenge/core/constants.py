"""constants module."""
KNOWLEDGE_BASE_SCHEMA = {
    "source": {"python_type": str, "spark_type": "string"},
    "target": {"python_type": str, "spark_type": "string"},
    "weight": {"python_type": int, "spark_type": "int"},
    "book": {"python_type": int, "spark_type": "int"},
}

VERTEX_COLUMNS = ["id"]

EDGE_COLUMNS = ["src", "dst", "weight", "book"]
