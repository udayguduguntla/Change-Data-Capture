import time

import pytest

from src.router import RoutedEvent
from src.writer import OffsetTracker, Writer
from tests.fakes import FakePool


@pytest.fixture
def writer(loader):
    return Writer(FakePool(), loader, loader.connections)


def ev(key, op="upsert", table="crm.contacts", db="orga1b2c3d4", offset=0,
       partition=0, mode="node", props=None, collapse=None, attr=None):
    row = {"key": key, "props": props or {"email": f"{key}@x.com"}, "ts": 1}
    if op == "delete":
        row = {"key": key, "ts": 1}
    return RoutedEvent(database=db, instance="instance-1", table=table, op=op, mode=mode,
                       row=row, collapse_key=collapse or ("n", key), attr_name=attr,
                       topic=f"cdc.{table}", partition=partition, offset=offset)


# --- flush ordering --------------------------------------------------------


def test_size_threshold_triggers_a_flush(loader):
    cfg = dict(loader.connections)
    cfg["batching"] = {"node_batch_size": 10, "relationship_batch_size": 5,
                       "flush_interval_seconds": 3600}
    w = Writer(FakePool(), loader, cfg)
    for i in range(10):
        w.add(ev(i, offset=i))
    assert w.due() == [("orga1b2c3d4", "crm.contacts")]


def test_time_threshold_triggers_a_flush_for_low_traffic_tenants(loader):
    cfg = dict(loader.connections)
    cfg["batching"] = {"node_batch_size": 10000, "relationship_batch_size": 5000,
                       "flush_interval_seconds": 0}
    w = Writer(FakePool(), loader, cfg)
    w.add(ev(1, offset=0))
    time.sleep(0.01)
    assert w.due(), "a low-traffic tenant must not sit unwritten indefinitely"


# --- cypher caching --------------------------------------------------------


def test_warm_cache_covers_every_table(writer):
    n = writer.warm_cache("orga1b2c3d4")
    assert n >= len(writer.loader.effective("orga1b2c3d4").tables)


def test_watermark_holds_at_the_lowest_unwritten_offset():
    t = OffsetTracker()
    for o in range(5):
        t.observe("topic", 0, o)
    t.hold("topic", 0, 2)
    t.hold("topic", 0, 4)
    # Offsets 0,1,3 are done; 2 is still buffered, so nothing past 2 may commit.
    assert t.commit_points()[("topic", 0)] == 2


def test_watermark_advances_past_the_high_water_mark_when_nothing_pends():
    t = OffsetTracker()
    for o in range(5):
        t.observe("topic", 0, o)
    assert t.commit_points()[("topic", 0)] == 5      # next offset to consume


def test_release_advances_the_watermark():
    t = OffsetTracker()
    for o in range(3):
        t.observe("topic", 0, o)
    t.hold("topic", 0, 1)
    assert t.commit_points()[("topic", 0)] == 1
    t.release([("topic", 0, 1)])
    assert t.commit_points()[("topic", 0)] == 3


def test_partitions_are_tracked_independently():
    t = OffsetTracker()
    t.observe("topic", 0, 10)
    t.observe("topic", 1, 20)
    t.hold("topic", 1, 20)
    points = t.commit_points()
    assert points[("topic", 0)] == 11
    assert points[("topic", 1)] == 20


def test_skipped_events_do_not_hold_the_watermark():
    """
    Quarantined and skipped events are durable immediately (or discarded), so they must never stall a partition.
    """
    t = OffsetTracker()
    t.observe("topic", 0, 0)
    t.observe("topic", 0, 1)
    t.observe("topic", 0, 2)
    assert t.commit_points()[("topic", 0)] == 3


# --- deadlock retry --------------------------------------------------------

def test_deadlock_is_retried_then_succeeds(loader):
    from neo4j.exceptions import TransientError

    class FlakySession:
        def __init__(self):
            self.attempts = 0

        def execute_write(self, fn):
            self.attempts += 1
            if self.attempts < 3:
                raise TransientError("deadlock detected")
            return None

    w = Writer(FakePool(), loader, loader.connections)
    w.backoff_base_ms = 0
    s = FlakySession()
    w._execute_with_retry(s, "UNWIND $batch AS e RETURN e", [{}], "orga1b2c3d4")
    assert s.attempts == 3


def test_deadlock_gives_up_after_max_attempts(loader):
    from neo4j.exceptions import TransientError

    class AlwaysDeadlocks:
        def execute_write(self, fn):
            raise TransientError("deadlock detected")

    w = Writer(FakePool(), loader, loader.connections)
    w.backoff_base_ms = 0
    w.max_attempts = 3
    with pytest.raises(TransientError):
        w._execute_with_retry(AlwaysDeadlocks(), "q", [{}], "orga1b2c3d4")
