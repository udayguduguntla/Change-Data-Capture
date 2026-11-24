"""Pipeline bookkeeping for each tenant database, kept OUT of the tenant graph.

One ``(:PipelineState {database})`` node per tenant database, stored in the metadata
database (``pool.metadata_database``). Holds what used to be copied onto business
nodes:

    schema_fingerprint   provisioning is skipped while this is unchanged
    embed_model          model tag of every vector in the database, e.g. BAAI/bge-large-en-v1.5@1024
    embed_dims           vector size of the active index
    embed_next_model     set only while a model switch is running
    embed_next_dims
    embed_fields         JSON {label: [fields]} last embedded, to re-queue a label whose fields change
"""
from __future__ import annotations

import json
from typing import Any

LABEL = "PipelineState"


def _meta(pool) -> str:
    return getattr(pool, "metadata_database", "neo4j")


def get_state(pool, database: str, instance: str | None = None) -> dict[str, Any]:
    with pool.get(instance).session(database=_meta(pool)) as s:
        rec = s.run(f"MATCH (p:{LABEL} {{database: $db}}) RETURN properties(p) AS p",
                    db=database).single()
    state = dict(rec["p"]) if rec and rec["p"] else {}
    if isinstance(state.get("embed_fields"), str):
        try:
            state["embed_fields"] = json.loads(state["embed_fields"])
        except ValueError:
            state["embed_fields"] = {}
    return state


def set_state(pool, database: str, instance: str | None = None, **fields) -> None:
    """Set fields; a value of None removes the field."""
    clean = {}
    for k, v in fields.items():
        clean[k] = json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else v
    with pool.get(instance).session(database=_meta(pool)) as s:
        s.run(f"MERGE (p:{LABEL} {{database: $db}}) SET p += $f, p.updated_at = datetime()",
              db=database, f=clean).consume()


def parse_tag(tag: str | None) -> tuple[str | None, int | None]:
    """'Qwen/Qwen3-Embedding-0.6B@1024' -> ('Qwen/Qwen3-Embedding-0.6B', 1024)."""
    if not tag or "@" not in tag:
        return tag, None
    model, _, dims = tag.rpartition("@")
    try:
        return model, int(dims)
    except ValueError:
        return tag, None
