"""v4.3: lean nodes, product scopes, deletes, model switch."""
import copy
import types
from pathlib import Path

import pytest

from src import model_switch as ms
from src.config_loader import ConfigLoader, _parse_core_table
from src.cypher_builder import build_node_delete, build_node_upsert
from src.embedding_worker import NEXT_PENDING, PENDING, EmbeddingWorker
from src.errors import ConfigError
from src.router import Router
from src.scope import apply_scopes, rbac_markdown, record_in_scope

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def loader():
    return ConfigLoader(ROOT / "config")


@pytest.fixture(scope="module")
def eff(loader):
    return loader.effective("org1000")


def _router(loader):
    return Router(loader, None, "cdc", "accounts.organizations", None, False, "organization_id")


# -- node shape -------------------------------------------------------------------

def test_upsert_writes_no_pipeline_properties(eff):
    q = build_node_upsert(eff.get("ticketing.tickets"), "scoped")
    for gone in ("_owner", "_deleted_at", "_embed_model", "_embed_text"):
        assert gone not in q
    assert "n._source_version" in q
    assert "SET n:`Support`" in q


def test_money_scale_is_not_written_on_nodes(loader):
    m = loader.get_mapping("org1000", "cdp.identity_keys")
    row = {"key_type": "email", "key_value": "a@b.c", "organization_id": 1000,
           "cdp_id": "x", "confidence_score": 95}
    ev = _router(loader)._build(row, m, "upsert", {"ts_ms": 1}, "org1000")[0]
    assert not any(k.endswith("_scale") for k in ev["props"])


def test_embed_pending_is_a_flag_only(loader):
    m = loader.get_mapping("org1000", "ticketing.tickets")
    ev = _router(loader)._build({"id": 1, "subject": "Refund", "organization_id": 1000},
                                m, "upsert", {"ts_ms": 1}, "org1000")[0]
    assert ev["props"][PENDING] is True
    assert not any(k.startswith("_embed_") and k != PENDING for k in ev["props"])


def test_toasted_text_missing_from_the_event_is_not_a_change(loader):
    m = loader.get_mapping("org1000", "ticketing.tickets")
    before = {"id": 1, "subject": "Refund", "description": "long text", "status": "open"}
    # Debezium left out the unchanged TOASTed description; only status changed.
    after = {"id": 1, "subject": "Refund", "status": "closed", "organization_id": 1000}
    ev = _router(loader)._build(after, m, "upsert", {"ts_ms": 2, "before": before}, "org1000")[0]
    assert PENDING not in ev["props"]
    after["subject"] = "Refund please"
    ev = _router(loader)._build(dict(after), m, "upsert", {"ts_ms": 3, "before": before}, "org1000")[0]
    assert ev["props"][PENDING] is True


# -- product scopes -------------------------------------------------------------------

def test_product_tables_are_scoped_setup_tables_are_not(eff):
    for m in eff.tables.values():
        if m.mode == "node" and m.label and not m.raw_cypher:
            unscoped = (m.access or {}).get("domain") == "common"
            assert bool(m.scope_labels) != unscoped, m.table


def test_scope_filter_allows_own_products_and_unlabelled_setup_nodes():
    node = lambda *labels: types.SimpleNamespace(labels=frozenset(labels), element_id="x")
    every = {"Support", "Sales", "Marketing", "ContactCentre", "CDP", "WebTracking"}
    ok = lambda rec: record_in_scope(rec, {"Support", "CDP"}, every)
    assert ok([node("Ticket", "Support"), node("Contact", "CDP"), 3])
    assert ok([node("User")])                                            # setup node: readable
    assert not ok([node("User"), node("MarketingAgent", "Marketing")])   # passing through stops
    assert not ok([[node("Deal", "Sales")]])


def test_rbac_doc_lists_products_and_unlabelled_setup(eff):
    doc = rbac_markdown(eff)
    assert "| `Ticket` | Support |" in doc and "| `Contact` | CDP |" in doc
    assert "| `User` | (none) |" in doc

def test_delete_removes_the_node(eff):
    assert "DETACH DELETE n" in build_node_delete(eff.get("ticketing.tickets"))


def test_secondary_table_delete_keeps_the_owners_node(eff):
    q = build_node_delete(eff.get("accounts.setup_email_configs"))
    assert "DETACH DELETE" not in q and "n.`email_domain`" in q and q.count("REMOVE") == 1
    assert "channel_config_id" not in q and "organization_id" not in q


def test_remove_own_properties_needs_merges_into():
    with pytest.raises(ConfigError):
        _parse_core_table("x.y", {"label": "X", "key": "id", "on_delete": "remove_own_properties"})


def test_app_soft_delete_becomes_a_graph_delete(loader):
    import src.router as R
    m = copy.deepcopy(loader.get_mapping("org1000", "ticketing.tickets"))
    m.delete_when = {"deleted_at": "not_null"}
    assert R._evaluate_when(m.delete_when, {"deleted_at": "2026-01-01"})
    assert not R._evaluate_when(m.delete_when, {"deleted_at": None})


# -- embedding model switch -------------------------------------------------------------

class _Res(list):
    def single(self):
        return self[0] if self else None

    def consume(self):
        return None


def _worker(loader, state, nodes, queries):
    class S:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def run(self, q, **kw):
            queries.append(q)
            if "PipelineState" in q:
                if "MERGE" in q:
                    state.update({k: v for k, v in kw["f"].items()})
                    return _Res()
                return _Res([{"p": dict(state)}])
            if "RETURN elementId(n)" in q:
                flag = NEXT_PENDING if NEXT_PENDING in q.split("WHERE")[1].split("RETURN")[0] else PENDING
                return _Res({"id": n["id"], "v": 1, "f": n} for n in nodes if n.get(flag))
            for n in nodes:
                if n["id"] in {r["id"] for r in kw.get("rows", [])}:
                    n.update({kw["prop"]: [0.1]})
                    if f"REMOVE n.{NEXT_PENDING}" in q:
                        n.pop(NEXT_PENDING, None)
                    if f"REMOVE n.{PENDING}" in q:
                        n.pop(PENDING, None)
                    if f"SET n.{NEXT_PENDING} = true" in q:
                        n[NEXT_PENDING] = True
            return _Res([{"c": len(kw.get("rows", []))}])

        def execute_write(self, fn):
            return fn(self)

    from tests.test_ontology import FakeService
    from src import embedder as E
    pool = types.SimpleNamespace(get=lambda _i: types.SimpleNamespace(session=lambda database: S()),
                                 metadata_database="neo4j")
    cfg = E.embedding_config(loader)
    return EmbeddingWorker(loader, None, pool, cfg, embedder=FakeService(cfg))


ORG = types.SimpleNamespace(database="org1000", instance=None)


def test_worker_adopts_its_model_when_the_database_has_none(loader):
    state, nodes, qs = {}, [], []
    w = _worker(loader, state, nodes, qs)
    assert w.mode(ORG) == ("normal", False)
    assert state["embed_model"] == w.tag


def test_old_worker_feeds_the_switch_and_new_worker_writes_shadow(loader):
    nodes = [{"id": "t1", "subject": "Refund", PENDING: True}]
    state, qs = {}, []
    old = _worker(loader, state, nodes, qs)
    state.update(embed_model=old.tag, embed_next_model="Qwen/Qwen3-Embedding-0.6B@1024")
    old.process_page(ORG, next(t for t in __import__("src.embedding_worker", fromlist=["x"])
                               .targets_for(loader, "org1000") if t.label == "Ticket"))
    assert PENDING not in nodes[0] and nodes[0][NEXT_PENDING] is True   # queued for the new model
    new = _worker(loader, state, nodes, qs)
    new.tag = "Qwen/Qwen3-Embedding-0.6B@1024"
    assert new.mode(ORG) == ("shadow", True)
    t = [t for t in __import__("src.embedding_worker", fromlist=["x"]).targets_for(loader, "org1000")
         if t.label == "Ticket"][0]
    new.process_page(ORG, t)
    assert "_embedding_next" in nodes[0] and NEXT_PENDING not in nodes[0]


def test_worker_refuses_a_database_on_another_model(loader):
    state, nodes, qs = {"embed_model": "some/other-model@768"}, [{"id": "t", PENDING: True}], []
    w = _worker(loader, state, nodes, qs)
    assert w.mode(ORG) == ("refuse", False)
    assert w.process_page(ORG, types.SimpleNamespace(label="Ticket", prop="_embedding",
                                                     fields=("subject",))) == 0


def test_truncate_only_for_matryoshka_models(loader, monkeypatch):
    monkeypatch.setattr(ms, "get_state", lambda *a: {"embed_model": "BAAI/bge-large-en-v1.5@1024"})
    with pytest.raises(ValueError):
        ms.truncate(None, loader, ORG, 512)


def test_finish_refuses_while_nodes_are_waiting(loader, monkeypatch):
    monkeypatch.setattr(ms, "get_state", lambda *a: {"embed_model": "a@1024",
                                                     "embed_next_model": "b@1024",
                                                     "embed_next_dims": 1024})
    monkeypatch.setattr(ms, "status", lambda *a: {"remaining": {"Ticket": 3}})
    with pytest.raises(ValueError):
        ms.finish(None, loader, ORG)


def test_worker_skips_a_database_that_does_not_exist_yet(loader):
    """After a reset the pipeline re-creates tenant databases one by one; the embedder
    must wait for them instead of crashing."""
    class NotFound(Exception):
        code = "Neo.ClientError.Database.DatabaseNotFound"

    class S:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def run(self, q, **kw):
            raise NotFound("Database does not exist. Database name: 'org1020'.")

    from src import embedder as E
    from tests.test_ontology import FakeService
    pool = types.SimpleNamespace(get=lambda _i: types.SimpleNamespace(session=lambda database: S()),
                                 metadata_database="neo4j")
    cfg = E.embedding_config(loader)
    w = EmbeddingWorker(loader, None, pool, cfg, embedder=FakeService(cfg))
    org = types.SimpleNamespace(database="org1020", instance=None)
    assert w.mark(org, "missing") == {}
    assert w.backlog(org) == {}
    assert w.run_once([org]) == 0


def test_does_not_exist_yet_warnings_are_dropped_but_others_kept():
    import logging
    import src.drivers  # noqa: F401 - installs the filter
    lg = logging.getLogger("neo4j.notifications")
    seen = []
    h = logging.Handler()
    h.emit = lambda r: seen.append(r.getMessage())
    lg.addHandler(h)
    try:
        lg.warning("<GqlStatusObject gql_status='01N52', status_description='warn: property key does not exist'>")
        lg.warning("<GqlStatusObject gql_status='01N50', status_description='warn: label does not exist'>")
        lg.warning("<GqlStatusObject gql_status='01N00', status_description='warn: feature deprecated'>")
    finally:
        lg.removeHandler(h)
    assert len(seen) == 1 and "01N00" in seen[0]


def test_conversation_summary_arrives_renamed_and_requeues_the_embedding(loader):
    m = loader.get_mapping("org1000", "ccass.conversation_memory")
    ev = _router(loader)._build({"id": 1, "organization_id": 1000, "conversation_id": "v1",
                                 "rolling_summary": "asked about refunds"},
                                m, "upsert", {"ts_ms": 2}, "org1000")[0]
    assert ev["key"] == "v1"
    assert ev["props"]["summary"] == "asked about refunds" and "rolling_summary" not in ev["props"]
    assert ev["props"][PENDING] is True
