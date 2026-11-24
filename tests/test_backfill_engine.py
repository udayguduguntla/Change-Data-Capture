"""Backfill through the real Backfiller, router and builder into a real Cypher engine."""
import types

import pytest

from tests.engine import Engine

from src.backfill import Backfiller, dependency_order   # noqa: E402
from src.cypher_builder import build_all                 # noqa: E402

ORG_ID, TENANT = "1000", 1000


class FakePg:
    """Just enough of psycopg for the Backfiller: tenant-column check, DB clock, SELECT *."""

    def __init__(self, tables: dict, clock_ms: int):
        self.tables, self.clock_ms = tables, clock_ms

    def __call__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self, name=None):
        return _Cursor(self)


class _Cursor:
    def __init__(self, pg):
        self.pg, self.rows, self.description, self.itersize = pg, [], [], 1000

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        if "information_schema.columns" in sql:
            schema, table, col = params
            rows = self.pg.tables.get(f"{schema}.{table}", [])
            self.rows = [(1,)] if rows and col in rows[0] else []
        elif "clock_timestamp" in sql:
            self.rows = [(self.pg.clock_ms,)]
        else:
            table = sql.split(" FROM ")[1].split()[0]
            rows = [r for r in self.pg.tables.get(table, [])
                    if not params or "tenant" not in params
                    or str(r.get("organization_id")) == params["tenant"]]
            cols = list(rows[0]) if rows else []
            self.description = [types.SimpleNamespace(name=c) for c in cols]
            self.rows = [tuple(r[c] for c in cols) for r in rows]

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class EngineWriter:
    """The Writer's contract (add + flush_all), executing on the engine graph."""
    node_batch_size = 500

    def __init__(self, engine):
        self.E, self.pending = engine, []

    def add(self, ev):
        self.pending.append(ev)

    def flush_all(self):
        n = 0
        for ev in self.pending:
            m = self.E.eff.get(ev.table)
            self.E.g.query(build_all(m, "legacy")[ev.op], {"batch": [ev.row]})
            n += 1
        self.pending = []
        return n


def _backfill(E, tables, clock_ms=1_000):
    org = types.SimpleNamespace(organization_id=ORG_ID, database="org1000", instance=None,
                                tenant_id=TENANT, products={"platform", "cdp", "ticketing", "crm",
                                                            "ccaas", "campaign", "webtracking"},
                                routable=lambda: True)
    registry = types.SimpleNamespace(all=lambda: [org], active=lambda: [org])
    E.router.tenant_column = "organization_id"
    bf = Backfiller(E.loader, registry, E.router, EngineWriter(E), FakePg(tables, clock_ms))
    return {
        t: bf._backfill_table_with_retry(org, t, 500, None) for t in
        dependency_order(E.eff, list(tables))}


def test_backfill_builds_labels_products_states_and_collections():
    E = Engine()
    stats = _backfill(E, {
        "cdp.customer_profile": [dict(cdp_id="C1", organization_id=TENANT, first_name="Ana")],
        "ticketing.tickets": [dict(id=10, organization_id=TENANT, customer_cdp_id="C1",
                                   status="open", subject="s", priority="high", assignee_id=5)],
        "ticketing.agents": [dict(id=5, organization_id=TENANT, role="agent", status="active")],
        "crm.deals": [dict(id=7, organization_id=TENANT, title="d", contact_id="C1")],
    })
    assert all(not v.get("error") for v in stats.values()), stats
    assert E.product_label_problems() == []
    assert {"Ticket", "Support", "OpenTicket"} <= E.labels("Ticket", "id", 10)
    assert E.count("(:Contact {cdp_id: 'C1'})-[:HAS]->(:TicketHistory)-[:HAS]->(:Ticket)") == 1
    assert E.count("(:Contact {cdp_id: 'C1'})-[:HAS]->(:DealHistory)-[:HAS]->(:Deal)") == 1
    # The agent row filled in the stub the ticket created: one SupportAgent, not two.
    assert E.rows("MATCH (s:SupportAgent) RETURN s.id, s.role, s._source_version IS NOT NULL") == [[5, "agent", True]]


def test_live_change_newer_than_the_backfill_read_wins():
    E = Engine()
    # A live CDC update committed after the backfill started reading (ts 2_000 > 1_000)...
    E.upsert("ticketing.tickets", ts=2_000, id=10, organization_id=TENANT, customer_cdp_id="C1",
             status="resolved", subject="s", priority="high")
    # ...is not overwritten by the backfill's older snapshot of the same row.
    _backfill(E, {"ticketing.tickets": [dict(id=10, organization_id=TENANT, customer_cdp_id="C1",
                                             status="open", subject="s", priority="high")]},
              clock_ms=1_000)
    assert "ResolvedTicket" in E.labels("Ticket", "id", 10)
    assert "OpenTicket" not in E.labels("Ticket", "id", 10)


def test_dependency_order_puts_parents_first():
    E = Engine()
    order = dependency_order(E.eff, list(E.eff.tables))
    pos = {t: i for i, t in enumerate(order)}
    assert pos["ccass.queues"] < pos["ccass.conversations"]
    assert pos["ticketing.sla_policies"] < pos["ticketing.tickets"]
    assert len(order) == len(E.eff.tables)
    assert order == dependency_order(E.eff, list(reversed(list(E.eff.tables))))   # stable
