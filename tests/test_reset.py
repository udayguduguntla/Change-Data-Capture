"""reset: Enterprise drops the tenant database; otherwise it wipes in place."""
import types

from src.reset import reset_database, shared_database


class _Res(list):
    def single(self):
        return self[0] if self else None

    def consume(self):
        return None


class FakeDriver:
    def __init__(self, edition="enterprise", databases=("org1000",), drop_ok=True):
        self.edition, self.databases, self.drop_ok, self.log = edition, set(databases), drop_ok, []
        self.nodes = 25

    def session(self, database=None):
        drv = self

        class S:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def run(self, q, **kw):
                drv.log.append((database, q))
                if "dbms.components" in q:
                    return _Res([{"edition": drv.edition}])
                if "SHOW DATABASES" in q:
                    return _Res([{"c": 1 if kw.get("n") in drv.databases else 0}])
                if q.startswith("DROP DATABASE"):
                    if not drv.drop_ok:
                        raise RuntimeError("not allowed")
                    return _Res()
                if "count(n) AS c" in q:
                    return _Res([{"c": drv.nodes}])
                if "DETACH DELETE n RETURN count" in q:
                    n, drv.nodes = min(drv.nodes, 10000), max(0, drv.nodes - 10000)
                    return _Res([{"c": n}])
                if q.startswith("SHOW CONSTRAINTS"):
                    return _Res([{"name": "c1"}])
                if q.startswith("SHOW INDEXES"):
                    return _Res([{"name": "i1"}])
                return _Res()
        return S()


def _pool(driver):
    return types.SimpleNamespace(get=lambda _i: driver, metadata_database="neo4j")


def test_enterprise_drops_the_database_and_clears_state():
    d = FakeDriver()
    r = reset_database(_pool(d), "org1000")
    assert r["result"] == "database dropped"
    assert any(q.startswith("DROP DATABASE `org1000`") for _, q in d.log)
    assert any("PipelineState" in q for db, q in d.log if db == "neo4j")


def test_falls_back_to_deleting_in_place():
    d = FakeDriver(drop_ok=False)
    r = reset_database(_pool(d), "org1000")
    assert r["result"] == "wiped in place" and r["nodes_deleted"] == 25
    assert r["constraints_dropped"] == 1 and r["indexes_dropped"] == 1


def test_dry_run_changes_nothing():
    d = FakeDriver()
    r = reset_database(_pool(d), "org1000", dry_run=True)
    assert r["nodes"] == 25
    assert not [q for _, q in d.log if q.startswith(("DROP", "MATCH (p:PipelineState"))]


def test_community_is_detected_as_one_shared_database():
    assert shared_database(_pool(FakeDriver(edition="community")), []) is True
    assert shared_database(_pool(FakeDriver(edition="enterprise")), []) is False
