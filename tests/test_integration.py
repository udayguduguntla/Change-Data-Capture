"""The thirteen acceptance tests from spec section 14."""

import os

import pytest

from src.quarantine import Quarantine
from src.router import Quarantined, RoutedEvent, Router
from src.writer import Writer
from tests.fakes import FakePool, FakeRegistry, envelope, value_schema

CONTACT_SCHEMA = value_schema({
    "contact_id": "int64", "tenant_id": "string",
    "created_at": "io.debezium.time.MicroTimestamp",
})

NEEDS_SERVER = pytest.mark.skipif(
    not os.environ.get("NEO4J_TEST_URI"),
    reason="set NEO4J_TEST_URI/USER/PASSWORD to run against a real Neo4j Enterprise",
)


@pytest.fixture
def rig(loader, org_a, org_b):
    """Router + Writer over a shared fake DBMS."""
    registry = FakeRegistry([org_a, org_b])
    pool = FakePool()
    router = Router(loader, registry, "cdc", "accounts.organisations")
    writer = Writer(pool, loader, loader.connections)
    return {"router": router, "writer": writer, "pool": pool, "registry": registry}


def feed(rig, topic, message, partition=0, offset=0):
    d = rig["router"].route(topic, message, partition, offset)
    if isinstance(d, RoutedEvent):
        rig["writer"].add(d)
    return d


def contact(tenant="tenant-alpha", cid=1, **extra):
    row = {"contact_id": cid, "tenant_id": tenant, "first_name": "Ada",
           "email": "ada@example.com", "status": "active", "account_id": 500}
    row.update(extra)
    return row


# --- 5  RELEASE BLOCKER ----------------------------------------------------

def test_05_replaying_the_same_batch_leaves_the_node_count_unchanged(rig):
    """
    Idempotency. MERGE on a stable business key means a crash-replay produces no duplicates, which is what makes at-least-once Kafka delivery safe.
    """
    batch = [envelope(after=contact(cid=i), schema=CONTACT_SCHEMA) for i in range(100)]
    for i, msg in enumerate(batch):
        feed(rig, "cdc.crm.contacts", msg, offset=i)
    rig["writer"].flush_all()
    g = rig["pool"].graph("orga1b2c3d4")
    before_nodes, before_edges = len(g.nodes), len(g.edges)

    for i, msg in enumerate(batch):                       # exact replay
        feed(rig, "cdc.crm.contacts", msg, offset=i)
    rig["writer"].flush_all()

    assert len(g.nodes) == before_nodes
    assert len(g.edges) == before_edges


# --- 6  RELEASE BLOCKER ----------------------------------------------------


def test_06b_every_write_names_an_explicit_database(rig):
    """No write may ever go to the driver's default database."""
    feed(rig, "cdc.crm.contacts", envelope(after=contact(), schema=CONTACT_SCHEMA))
    feed(rig, "cdc.crm.contacts",
         envelope(after=contact("tenant-beta"), schema=CONTACT_SCHEMA), offset=1)
    rig["writer"].flush_all()
    for database, _, _ in rig["pool"].log:
        assert database in ("orga1b2c3d4", "orgb5e6f7a8")


def test_07_unknown_tenant_is_quarantined_and_nothing_is_written(rig, tmp_path):
    q = Quarantine(tmp_path / "q")
    d = feed(rig, "cdc.crm.contacts",
             envelope(after=contact("tenant-ghost"), schema=CONTACT_SCHEMA))
    assert isinstance(d, Quarantined)
    q.write("cdc.crm.contacts", {}, d.reason, d.detail, 0, 0)
    rig["writer"].flush_all()
    assert rig["pool"].log == []
    assert len(list((tmp_path / "q").glob("*.jsonl"))) == 1


def test_12b_nested_value_is_also_caught_at_validation_time(tmp_path):
    """
    Spec 7.4: fail at validation time, not at write time. A declared type of 'string' on a column that holds an object is what validation can catch.
    """
    from src.config_loader import PropertySpec
    from src.coercion import extract_custom
    from src.errors import CoercionError
    with pytest.raises(CoercionError):
        extract_custom({"c": {"x": [1, 2]}}, "c", [PropertySpec("x", "string")])


# --- 13  RELEASE BLOCKER (needs a real server) -----------------------------

@NEEDS_SERVER
def test_13_profile_shows_a_unique_index_seek_not_a_label_scan(loader):
    """
    Without the constraint, MERGE is a full label scan whose cost grows with the graph. This is the single largest performance factor in the system.
    """
    from src.drivers import DriverPool
    from src.provision import Provisioner

    cfg = dict(loader.section("neo4j"))
    cfg["instances"] = {"instance-1": {
        "uri": os.environ["NEO4J_TEST_URI"],
        "user": os.environ.get("NEO4J_TEST_USER", "neo4j"),
        "password": os.environ["NEO4J_TEST_PASSWORD"],
    }}
    pool = DriverPool(cfg)
    prov = Provisioner(pool, loader)
    prov.check_enterprise()
    db = "orgtestprofile"
    prov.ensure_database(db)
    prov.ensure_constraints(db)

    results = prov.verify_index_usage(db)
    for label, r in results.items():
        assert r["index_seek"], f"{label}: no index seek, operators={r['operators']}"
        assert not r["label_scan"], f"{label}: NodeByLabelScan present"
    pool.close()


@NEEDS_SERVER
def test_06_real_server_isolation(loader):
    """Cross-database isolation against a real DBMS: a query in A cannot see B."""
    from src.drivers import DriverPool
    cfg = dict(loader.section("neo4j"))
    cfg["instances"] = {"instance-1": {
        "uri": os.environ["NEO4J_TEST_URI"],
        "user": os.environ.get("NEO4J_TEST_USER", "neo4j"),
        "password": os.environ["NEO4J_TEST_PASSWORD"],
    }}
    pool = DriverPool(cfg)
    driver = pool.get()
    with driver.session(database="orga1b2c3d4") as s:
        s.run("MERGE (n:Contact {contact_id: 'isolation-probe'})")
    with driver.session(database="orgb5e6f7a8") as s:
        found = s.run(
            "MATCH (n:Contact {contact_id: 'isolation-probe'}) RETURN count(n) AS c"
        ).single()["c"]
    assert found == 0
    pool.close()
