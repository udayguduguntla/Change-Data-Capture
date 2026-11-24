"""`main.py integrity` on a real engine: pipeline output is clean, breakage is caught."""
from src import integrity
from tests.engine import Engine

ORG = 1000


class Session:
    """neo4j-session shaped wrapper around the engine graph."""

    def __init__(self, E):
        self.E = E

    def run(self, q, **params):
        res = self.E.g.query(q, params)
        cols = [c[1] if isinstance(c, (list, tuple)) else c for c in res.header]
        rows = [dict(zip(cols, r)) for r in res.result_set]

        class R(list):
            def single(self):
                return self[0] if self else None
        return R(rows)


def _build_sample(E):
    E.upsert("cdp.customer_profile", ts=1, cdp_id="C1", organization_id=ORG)
    E.upsert("ticketing.tickets", ts=1, id=10, organization_id=ORG, customer_cdp_id="C1",
             status="open", subject="s", priority="high", assignee_id=5)
    E.upsert("crm.deals", ts=1, id=7, organization_id=ORG, title="d", contact_id="C1")
    E.upsert("cdp.customer_events", ts=1, event_id="e1", cdp_id="C1", organization_id=ORG,
             occurred_at="2026-01-01T00:00:00")


def _problems(E, **kw):
    rep = integrity.run_checks(Session(E), E.eff, E.loader.scopes_cfg, **kw)
    return {k: v["count"] for k, v in rep.items() if v["count"]}


def test_pipeline_output_is_clean():
    E = Engine()
    _build_sample(E)
    assert _problems(E) == {}


def test_breakage_is_reported():
    E = Engine()
    _build_sample(E)
    E.g.query("MATCH (t:Ticket {id: 10}) SET t:Sales")                              # two products
    E.g.query("CREATE (:DealHistory:Sales {uid: 'DealHistory:X'})")                  # orphan + empty
    E.g.query("CREATE (:User:Marketing {id: 999})")                                  # setup node with a product
    E.g.query("CREATE (:SupportHub {organization_id: 1})")                           # leftover hub
    p = _problems(E)
    for check in ("product_labels", "collections_orphan", "collections_empty", "hub_nodes"):
        assert check in p, (check, p)


def test_old_stubs_are_reported_and_removable():
    E = Engine()
    _build_sample(E)                                  # SupportAgent 5 is a stub (no agent row)
    E.g.query("MATCH (s:SupportAgent {id: 5}) SET s._stub_since = 0")
    assert _problems(E, stub_days=7).get("stubs_old") == 1
    assert integrity.remove_old_stubs(Session(E), E.eff, 7) == 1
    assert "stubs_old" not in _problems(E)


def test_fix_removes_empty_and_orphan_collections():
    E = Engine()
    _build_sample(E)
    E.g.query("CREATE (:DealHistory:Sales {uid: 'DealHistory:X'})")
    assert integrity.fix_collections(Session(E), E.eff) >= 1
    assert _problems(E) == {}
