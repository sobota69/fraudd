"""Infrastructure configuration – loads settings from environment variables."""

from __future__ import annotations

import os


def get_neo4j_config() -> dict[str, str]:
    """Return Neo4j connection parameters from env vars with sensible defaults."""
    return {
        "uri": os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        "user": os.environ.get("NEO4J_USER", "neo4j"),
        "password": os.environ.get("NEO4J_PASSWORD", "capgemini"),
        "database": os.environ.get("NEO4J_DATABASE", "neo4j"),
    }
