"""Run the real pipeline path against a real Cypher engine.

row --Router._build--> event --build_all--> Cypher --> embedded FalkorDB graph

FalkorDB is not Neo4j, but it executes the same openCypher clauses the writer uses
(MERGE, OPTIONAL MATCH, FOREACH, CALL {...}, pattern comprehensions, label SET/REMOVE),
so these tests prove what the generated queries actually do to a graph instead of
matching their text. Queries are built with subquery_syntax="legacy"; production on
Neo4j 5.23+ uses the scoped form of the same queries.

Skipped automatically when falkordblite is not installed.
"""
from __future__ import annotations

import itertools
import tempfile
from pathlib import Path

import pytest

falkordb = pytest.importorskip("redislite.falkordb_client")

from src.config_loader import ConfigLoader        # noqa: E402
from src.cypher_builder import build_all          # noqa: E402
from src.router import Router                     # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
_counter = itertools.count()
_server = None


def _db():
    global _server
    if _server is None:
        _server = falkordb.FalkorDB(str(Path(tempfile.mkdtemp()) / "engine.db"))
    return _server


class Engine:
    def __init__(self, loader: ConfigLoader | None = None, database: str = "org1000"):
        self.loader = loader or ConfigLoader(ROOT / "config")
        self.database = database
        self.eff = self.loader.effective(database)
        self.router = Router(self.loader, None, "cdc", "accounts.organizations", None, False,
                             "organization_id")
        self.g = _db().select_graph(f"t{next(_counter)}")
        self.queries: list[str] = []

    # -- writing -------------------------------------------------------------------

    def write(self, table: str, row: dict, op: str = "upsert", ts: int | None = None):
        m = self.eff.get(table)
        built = self.router._build(dict(row), m, op, {"ts_ms": ts}, self.database)
        if not isinstance(built, tuple):
            raise AssertionError(f"{table}: router did not build an event: {built}")
        event, _key, _attr, out_op = built
        q = build_all(m, "legacy")["upsert" if out_op == "upsert" else "delete"]
        self.queries.append(q)
        self.g.query(q, {"batch": [event]})
        return event

    def upsert(self, table: str, ts: int = 1, **row):
        return self.write(table, row, "upsert", ts)

    def delete(self, table: str, ts: int = 99, **row):
        return self.write(table, row, "delete", ts)

    # -- reading -------------------------------------------------------------------

    def rows(self, cypher: str, **params) -> list:
        return self.g.query(cypher, params).result_set

    def labels(self, label: str, key: str, value) -> set[str]:
        r = self.rows(f"MATCH (n:`{label}` {{`{key}`: $v}}) RETURN labels(n)", v=value)
        return set(r[0][0]) if r else set()

    def count(self, pattern: str) -> int:
        return self.rows(f"MATCH {pattern} RETURN count(*)")[0][0]

    def snapshot(self) -> tuple:
        """Whole graph as comparable data: nodes (labels + props) and edges."""
        nodes = sorted(
            (tuple(sorted(r[0])), tuple(sorted((k, str(v)) for k, v in r[1].items() if k != "_stub_since")))
            for r in self.rows("MATCH (n) RETURN labels(n), properties(n)"))
        edges = sorted(
            (tuple(sorted(r[0])), str(r[1]), r[2], tuple(sorted(r[3])), str(r[4]))
            for r in self.rows(
                "MATCH (a)-[r]->(b) RETURN labels(a), coalesce(a.uid, a.id, a.cdp_id, a.key), "
                "type(r), labels(b), coalesce(b.uid, b.id, b.cdp_id, b.key)"))
        return tuple(nodes), tuple(edges)

    def product_label_problems(self) -> list:
        """Product nodes without exactly one product label, or unscoped nodes with one."""
        products = set(self.eff.all_scopes)
        unscoped = set(self.eff.unscoped_labels)
        bad = []
        for labels, props in self.rows("MATCH (n) RETURN labels(n), properties(n)"):
            n = len([l for l in labels if l in products])
            if (set(labels) & unscoped and n) or (not set(labels) & unscoped and n != 1):
                bad.append((labels, props))
        return bad
