"""Switching the embedding model without breaking search.

Vectors from two models live in different spaces and cannot be compared, so a new model
means re-embedding every node. This does it in the background, next to the old vectors:

    start    record the new model as "next", create its vector index, queue every node
    (run)    a worker started with the NEW model writes _embedding_next;
             the worker with the OLD model keeps serving live changes to _embedding
             and re-queues each change for the new model too
    status   nodes still waiting per label
    finish   swap _embedding_next into _embedding, rebuild the index, make the new model active
    abort    drop everything the switch created; the old model stays active

A dims-only change of a Matryoshka-trained model (Qwen3-Embedding, EmbeddingGemma)
needs no re-embedding: ``truncate`` shortens and re-normalises the stored vectors.
"""
from __future__ import annotations

import logging

from .embedder import model_tag
from .embedding_worker import NEXT_PENDING, NEXT_SUFFIX, PENDING, _q, targets_for
from .graph_state import get_state, parse_tag, set_state

log = logging.getLogger(__name__)

MATRYOSHKA_FAMILIES = ("qwen3-embedding", "embeddinggemma")


def index_name(label: str, prop: str) -> str:
    return f"{label.lower()}_{prop}_vector"


def _create_index(s, label: str, prop: str, dims: int) -> None:
    s.run(f"CREATE VECTOR INDEX `{index_name(label, prop)}` IF NOT EXISTS "
          f"FOR (n:{_q(label)}) ON (n.{_q(prop)}) OPTIONS {{indexConfig: {{"
          f"`vector.dimensions`: {int(dims)}, `vector.similarity_function`: 'cosine'}}}}").consume()


def _drop_index(s, label: str, prop: str) -> None:
    s.run(f"DROP INDEX `{index_name(label, prop)}` IF EXISTS").consume()


def _session(pool, org):
    return pool.get(org.instance).session(database=org.database)


def start(pool, loader, org, new_cfg: dict, labels: list[str] | None = None) -> dict:
    new_tag = model_tag(new_cfg)
    dims = int(new_cfg["dimensions"])
    st = get_state(pool, org.database, org.instance)
    if st.get("embed_model") == new_tag:
        raise ValueError(f"{org.database} already uses {new_tag}")
    if st.get("embed_next_model") and st["embed_next_model"] != new_tag:
        raise ValueError(f"{org.database}: a switch to {st['embed_next_model']} is running; "
                         "finish or abort it first")
    set_state(pool, org.database, org.instance, embed_next_model=new_tag, embed_next_dims=dims)
    out = {}
    with _session(pool, org) as s:
        for t in targets_for(loader, org.database):
            if labels and t.label not in labels:
                continue
            _create_index(s, t.label, t.prop + NEXT_SUFFIX, dims)
            rec = s.run(f"MATCH (n:{_q(t.label)}) "
                        f"CALL (n) {{ SET n.{NEXT_PENDING} = true }} IN TRANSACTIONS OF 5000 ROWS "
                        "RETURN count(*) AS c").single()
            out[t.label] = rec["c"] if rec else 0
    log.info("database=%s switch to %s started: %s", org.database, new_tag, out)
    return out


def status(pool, loader, org) -> dict:
    st = get_state(pool, org.database, org.instance)
    remaining = {}
    with _session(pool, org) as s:
        for t in targets_for(loader, org.database):
            rec = s.run(f"MATCH (n:{_q(t.label)}) WHERE n.{NEXT_PENDING} = true "
                        "RETURN count(n) AS c").single()
            remaining[t.label] = rec["c"] if rec else 0
    return {"active": st.get("embed_model"), "next": st.get("embed_next_model"),
            "remaining": {k: v for k, v in remaining.items() if v} if st.get("embed_next_model") else {}}


def finish(pool, loader, org, force: bool = False) -> dict:
    st = get_state(pool, org.database, org.instance)
    new_tag, dims = st.get("embed_next_model"), st.get("embed_next_dims")
    if not new_tag:
        raise ValueError(f"{org.database}: no model switch is running")
    left = status(pool, loader, org)["remaining"]
    if left and not force:
        raise ValueError(f"{org.database}: {sum(left.values())} nodes still waiting for the new "
                         f"model {left}; let the new worker finish or pass --force")
    out = {}
    with _session(pool, org) as s:
        for t in targets_for(loader, org.database):
            nxt = t.prop + NEXT_SUFFIX
            _drop_index(s, t.label, t.prop)
            # Never waited-for by the new model (only with --force): drop the old vector.
            s.run(f"MATCH (n:{_q(t.label)}) WHERE n.{NEXT_PENDING} = true AND n.{_q(nxt)} IS NULL "
                  f"CALL (n) {{ REMOVE n.{_q(t.prop)} }} IN TRANSACTIONS OF 5000 ROWS").consume()
            rec = s.run(
                f"MATCH (n:{_q(t.label)}) WHERE n.{_q(nxt)} IS NOT NULL "
                f"CALL (n) {{ SET n.{_q(t.prop)} = n.{_q(nxt)} REMOVE n.{_q(nxt)} }} "
                "IN TRANSACTIONS OF 5000 ROWS RETURN count(*) AS c").single()
            # Still waiting (only with --force): keep whatever it has and let the new
            # model, now active, embed it again from the normal queue.
            s.run(f"MATCH (n:{_q(t.label)}) WHERE n.{NEXT_PENDING} = true "
                  f"CALL (n) {{ REMOVE n.{NEXT_PENDING} SET n.{PENDING} = true }} "
                  "IN TRANSACTIONS OF 5000 ROWS").consume()
            # A vector of the old size can only be an old-model vector: never keep it.
            s.run(f"MATCH (n:{_q(t.label)}) WHERE n.{_q(t.prop)} IS NOT NULL "
                  f"AND size(n.{_q(t.prop)}) <> $dims "
                  f"CALL (n) {{ REMOVE n.{_q(t.prop)} SET n.{PENDING} = true }} "
                  "IN TRANSACTIONS OF 5000 ROWS", dims=int(dims)).consume()
            _drop_index(s, t.label, nxt)
            _create_index(s, t.label, t.prop, int(dims))
            out[t.label] = rec["c"] if rec else 0
    set_state(pool, org.database, org.instance, embed_model=new_tag, embed_dims=int(dims),
              embed_next_model=None, embed_next_dims=None)
    log.info("database=%s switch finished; active model %s", org.database, new_tag)
    return out


def abort(pool, loader, org) -> dict:
    out = {}
    with _session(pool, org) as s:
        for t in targets_for(loader, org.database):
            nxt = t.prop + NEXT_SUFFIX
            rec = s.run(f"MATCH (n:{_q(t.label)}) "
                        f"WHERE n.{_q(nxt)} IS NOT NULL OR n.{NEXT_PENDING} IS NOT NULL "
                        f"CALL (n) {{ REMOVE n.{_q(nxt)}, n.{NEXT_PENDING} }} "
                        "IN TRANSACTIONS OF 5000 ROWS RETURN count(*) AS c").single()
            _drop_index(s, t.label, nxt)
            out[t.label] = rec["c"] if rec else 0
    set_state(pool, org.database, org.instance, embed_next_model=None, embed_next_dims=None)
    return out


def truncate(pool, loader, org, new_dims: int) -> dict:
    """Shorten every vector of a Matryoshka model to ``new_dims``; no model run needed."""
    st = get_state(pool, org.database, org.instance)
    model, dims = parse_tag(st.get("embed_model"))
    if not model or not any(f in model.lower() for f in MATRYOSHKA_FAMILIES):
        raise ValueError(f"{org.database}: {model} is not trained for shorter vectors; "
                         "use --switch-model to re-embed instead")
    if st.get("embed_next_model"):
        raise ValueError(f"{org.database}: finish or abort the running switch first")
    if not dims or new_dims >= dims:
        raise ValueError(f"{org.database}: new size {new_dims} must be smaller than {dims}")
    out = {}
    with _session(pool, org) as s:
        for t in targets_for(loader, org.database):
            p = _q(t.prop)
            _drop_index(s, t.label, t.prop)
            rec = s.run(
                f"MATCH (n:{_q(t.label)}) WHERE n.{p} IS NOT NULL AND size(n.{p}) > $d "
                f"CALL (n) {{ "
                f"  WITH n, n.{p}[0..$d] AS v "
                f"  WITH n, v, sqrt(reduce(a = 0.0, x IN v | a + x * x)) AS norm "
                f"  SET n.{p} = [x IN v | x / CASE WHEN norm = 0 THEN 1 ELSE norm END] "
                f"}} IN TRANSACTIONS OF 5000 ROWS RETURN count(*) AS c", d=int(new_dims)).single()
            _create_index(s, t.label, t.prop, int(new_dims))
            out[t.label] = rec["c"] if rec else 0
    set_state(pool, org.database, org.instance, embed_model=f"{model}@{int(new_dims)}",
              embed_dims=int(new_dims))
    return out
