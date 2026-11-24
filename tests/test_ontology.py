"""Rules of the current ontology, checked against the real config/ files."""
import re
import types
from pathlib import Path

import numpy as np
import pytest

from src import embedder as E
from src.config_loader import ConfigLoader
from src.cypher_builder import build_node_upsert
from src.embedding_worker import EmbeddingWorker, targets_for
from src.router import Router
from src.validation import validate_all

ROOT = Path(__file__).resolve().parent.parent
PACKS = sorted((ROOT / "config" / "mappings" / "domains").glob("*.yaml"))


@pytest.fixture(scope="module")
def loader():
    return ConfigLoader(ROOT / "config")


def router(loader):
    return Router(loader, None, "cdc", "accounts.organizations", None, True, "organization_id")


def build(loader, table, row, before=None):
    m = loader.get_mapping("org1000", table)
    payload = {"op": "u" if before else "c", "ts_ms": 1, "before": before}
    return router(loader)._build(dict(row), m, "upsert", payload, "org1000")[0]


def test_config_validates(loader):
    report = validate_all(loader)
    assert report.errors == []


@pytest.mark.parametrize("pack", PACKS, ids=[p.stem for p in PACKS])
def test_every_industry_pack_composes(pack):
    loader = ConfigLoader(ROOT / "config", domain_overlay=str(pack))
    eff = loader.effective("org1000")
    for m in eff.tables.values():
        if m.mode == "node" and m.emit_to_graph:
            assert "MERGE (n:" in build_node_upsert(m, "scoped")


def test_contact_has_exactly_one_role(loader):
    q = build_node_upsert(loader.get_mapping("org1000", "cdp.customer_profile"), "scoped")
    assert "MERGE (n:Contact " in q
    assert "REMOVE n:`Customer`:`Lead`:`Prospect`" in q


def test_ticket_status_labels_are_exclusive(loader):
    q = build_node_upsert(loader.get_mapping("org1000", "ticketing.tickets"), "scoped")
    remove = next(l for l in q.splitlines() if l.strip().startswith("REMOVE n:") and "OpenTicket" in l)
    for lbl in ("OpenTicket", "InProgressTicket", "ResolvedTicket", "OverdueTicket"):
        assert f"`{lbl}`" in remove
    ev = build(loader, "ticketing.tickets", {"id": 1, "organization_id": 1000, "status": "pending"})
    assert "InProgressTicket" in ev["dynamic_labels"]


def test_static_sublabels_apply(loader):
    assert "SupportStaff" in loader.get_mapping("org1000", "ticketing.agents").extra_labels


def test_stub_targets_are_created_by_key_only(loader):
    """Key-based edges create their target as a stub when it has not arrived yet, so the
    graph does not depend on event order (the owner's row fills the stub in, and
    `main.py integrity` reports stubs that never fill). A stub is always made from one
    key; field-matched edges (a ticket's SLA by priority) never create anything."""
    for t, m in loader.core_tables.items():
        if m.mode != "node" or not m.emit_to_graph:
            continue
        q = build_node_upsert(m, "scoped")
        for props in re.findall(r"MERGE \(t\d+:[`\w:]+ \{([^}]*)\}\)", q):
            assert props.count(":") == 1, f"{t} would create a stub from several fields: {props}"

def test_every_merged_label_has_a_constraint(loader):
    labels = loader.effective("org1000").all_labels()
    for name in ("LogPeriod", "EventHistory", "ContactProfile", "DealHistory", "ProductCategory",
                 "UserDirectory", "MarketingAgent"):
        assert name in labels
    assert not [l for l in labels if l.endswith("Hub")]          # hubs are gone


def test_removed_tables_are_archived_not_mapped(loader):
    from src.config_loader import archived_tables
    for t in ("campaign.branches", "ccass.conversation_messages", "cdp.anomaly_threshold_state"):
        assert t in archived_tables() and t not in loader.core_tables
    assert loader.core_tables["accounts.setup_channel_configs"].emit_to_graph is True

def test_no_synonyms_in_packs():
    for pack in PACKS:
        text = pack.read_text()
        for key in ("naming:", "naming_overrides:", "aliases:", "synonyms:"):
            assert f"\n  {key}" not in text and f"\n    {key}" not in text, (pack.name, key)


def test_industry_selection_is_exact(loader):
    loader.set_industry("org1001", "therapist clinic")
    assert loader.effective("org1001").industry != "saas"
    loader.set_industry("org1002", "saas")
    assert loader.effective("org1002").industry == "saas"


def test_embed_text_names_fields_and_skips_state(loader):
    ev = build(loader, "ticketing.tickets",
               {"id": 1, "organization_id": 1000, "subject": "Login fails", "status": "Open"})
    assert "_embed_text" not in ev["props"]
    assert ev["props"]["_embed_pending"] is True
    from src.embedder import build_embed_text
    text = build_embed_text("Ticket", ["subject", "description"], ev["props"].get)
    assert text == "Ticket. subject: Login fails"


def test_no_catalog_in_the_graph(loader):
    eff = loader.effective("org1000")
    assert "EntityType" not in eff.all_labels()
    assert "EntityType" not in {t.label for t in targets_for(loader, "org1000")}


class FakeService(E.EmbeddingService):
    def _load_st(self):
        pass

    def _encode_raw(self, texts):
        return np.stack([np.random.default_rng(abs(hash(t)) % 2**32).standard_normal(1024)
                         for t in texts]).astype(np.float32)


def test_embedder_prompts_cache_and_mrl():
    svc = FakeService({**E.DEFAULTS, "dimensions": 512})
    assert svc.query_prompt.startswith("Represent this sentence")
    a, b, c = svc.encode_documents(["x", "y", "x"])
    assert len(a) == 512 and a == c and abs(np.linalg.norm(a) - 1) < 1e-5


def test_worker_embeds_and_clears_queue(loader):
    nodes = [{"id": "1", "label": "Contact", "_source_version": 1, "_embed_pending": True,
              "first_name": "Ann"},
             {"id": "2", "label": "Contact", "_source_version": 1, "_embed_pending": True}]

    class Result(list):
        def single(self):
            return self[0] if self else None

        def consume(self):
            return None

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def run(self, q, **kw):
            if "RETURN elementId(n)" in q:
                label = re.search(r"MATCH \(n:`(\w+)`\)", q).group(1)
                return Result({"id": n["id"], "v": n["_source_version"], "f": n}
                              for n in nodes if n["label"] == label and n.get("_embed_pending"))
            ids = {r["id"] for r in kw.get("rows", [])} | set(kw.get("ids", []))
            for n in nodes:
                if n["id"] in ids:
                    n.pop("_embed_pending", None)
            return Result([{"c": len(kw.get("rows", []))}])

        def execute_write(self, fn):
            return fn(self)

    pool = types.SimpleNamespace(get=lambda _i: types.SimpleNamespace(session=lambda database: Session()))
    org = types.SimpleNamespace(database="org1000", instance=None)
    cfg = E.embedding_config(loader)
    worker = EmbeddingWorker(loader, None, pool, cfg, embedder=FakeService(cfg))
    worker.mode = lambda _org: ("normal", False)
    worker.run_once([org])
    assert worker.stats == {"embedded": 1, "skipped_empty": 1, "skipped_race": 0}
    assert not any(n.get("_embed_pending") for n in nodes)


def test_collections_hang_off_their_owner(loader):
    q = build_node_upsert(loader.get_mapping("org1000", "cdp.customer_events"), "scoped")
    # The owner is created as a stub if needed, so a collection is never left unattached.
    assert "MERGE (o:`Contact` {`cdp_id`: e.`_c0`})" in q
    assert "MERGE (o)-[:`HAS`]->(col)" in q
    assert "uid: 'EventHistory:'" in q
    # The collection carries the product of the events it holds, and only that one.
    assert "SET col:`WebTracking`" in q          # web behaviour collected into cdp
