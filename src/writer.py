"""Buffering and writing.  Spec sections 8.4, 8.5, 8.6."""

from __future__ import annotations

import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field

from .cypher_builder import (
    build_node_delete,
    build_node_upsert,
    build_properties_on_remove,
    build_properties_on_upsert,
    build_relationship_delete,
    build_relationship_upsert,
)

log = logging.getLogger(__name__)

RELATIONSHIP_MODES = ("relationship_only",)
FLUSH_ORDER = ("upsert", "relationship", "delete")


class OffsetTracker:
    """Tracks which offsets are buffered but not yet durable in Neo4j."""

    def __init__(self) -> None:
        self._pending: dict[tuple[str, int], set[int]] = defaultdict(set)
        self._max_seen: dict[tuple[str, int], int] = {}

    def observe(self, topic: str, partition: int, offset: int) -> None:
        """Record that an offset was polled, whether or not it produced a write."""
        if partition is None or offset is None:
            return
        tp = (topic, partition)
        self._max_seen[tp] = max(self._max_seen.get(tp, -1), offset)

    def hold(self, topic: str, partition: int, offset: int) -> None:
        """Record an offset whose data is buffered and not yet written."""
        if partition is None or offset is None:
            return
        self.observe(topic, partition, offset)
        self._pending[(topic, partition)].add(offset)

    def release(self, coords) -> None:
        for topic, partition, offset in coords:
            if partition is None or offset is None:
                continue
            self._pending[(topic, partition)].discard(offset)

    def commit_points(self) -> dict[tuple[str, int], int]:
        out: dict[tuple[str, int], int] = {}
        for tp, high in self._max_seen.items():
            pending = self._pending.get(tp)
            out[tp] = min(pending) if pending else high + 1
        return out

    def pending_count(self) -> int:
        return sum(len(s) for s in self._pending.values())


@dataclass
class Buffer:
    database: str
    table: str
    events: dict = field(default_factory=dict)      # collapse_key -> RoutedEvent
    coords: list = field(default_factory=list)      # EVERY offset added, not just survivors
    created_at: float = field(default_factory=time.monotonic)

    def add(self, event) -> None:
        self.events[event.collapse_key] = event
        # Offsets are tracked separately from events.
        self.coords.append((event.topic, event.partition, event.offset))

    def __len__(self) -> int:
        return len(self.events)


class Writer:
    def __init__(self, pool, loader, config, metrics=None, tenant_resolver=None):
        self.pool = pool
        self.loader = loader
        self.metrics = metrics
        self.tenant_resolver = tenant_resolver
        batching = config.get("batching", {})
        self.node_batch_size = batching.get("node_batch_size", 10000)
        self.relationship_batch_size = batching.get("relationship_batch_size", 5000)
        self.flush_interval = batching.get("flush_interval_seconds", 2)
        retry = config.get("retry", {})
        self.max_attempts = retry.get("deadlock_max_attempts", 5)
        self.backoff_base_ms = retry.get("deadlock_backoff_base_ms", 50)
        self.subquery_syntax = config.get("cypher", {}).get("subquery_syntax", "scoped")

        self._buffers: dict[tuple[str, str], Buffer] = {}
        self._query_cache: dict[tuple, str] = {}
        self.offsets = OffsetTracker()
        self._last_flush = time.monotonic()
        self._watermarks: dict[str, int] = {}
        self._last_watermark_write = time.monotonic()

    # -- cypher cache -------------------------------------------------------

    def _cypher(self, database: str, table: str, op: str, attr_name: str | None = None) -> str:
        """Generated once per (database, table, op) and cached.  Never in the loop."""
        cache_key = (database, table, op, attr_name)
        if cache_key in self._query_cache:
            return self._query_cache[cache_key]
        m = self.loader.get_mapping(database, table)
        if m.raw_cypher:
            q = m.raw_cypher
        elif m.mode == "node":
            q = build_node_upsert(m, self.subquery_syntax) if op == "upsert" else build_node_delete(m)
        elif m.mode == "relationship_only":
            q = build_relationship_upsert(m) if op == "upsert" else build_relationship_delete(m)
        elif m.mode == "properties_on":
            q = (
                build_properties_on_upsert(m)
                if op == "upsert"
                else build_properties_on_remove(m, attr_name or "")
            )
        elif m.mode == "lookup_source" or not getattr(m, "emit_to_graph", True):
            return ""
        else:
            raise ValueError(f"unknown mode {m.mode!r}")
        self._query_cache[cache_key] = q
        return q

    def warm_cache(self, database: str) -> int:
        """
        Pre-generate every query for a database, so startup fails on a bad mapping rather than the first event of the day.
        """
        eff = self.loader.effective(database)
        n = 0
        for table, m in eff.tables.items():
            if m.mode == "lookup_source" or not getattr(m, "emit_to_graph", True):
                continue
            for op in ("upsert", "delete"):
                if m.mode == "properties_on" and op == "delete":
                    continue                       # name is dynamic; built per flush
                self._cypher(database, table, op)
                n += 1
        return n


    def add(self, event) -> None:
        companion = getattr(event, "companion", None)
        if companion is not None:
            self._add_one(companion)        # removal first, then the upsert
        self._add_one(event)

    def _add_one(self, event) -> None:
        key = event.buffer_key
        buf = self._buffers.get(key)
        if buf is None:
            buf = self._buffers[key] = Buffer(event.database, event.table)
        buf.add(event)
        self.offsets.hold(event.topic, event.partition, event.offset)

    def _limit_for(self, database: str, table: str) -> int:
        m = self.loader.get_mapping(database, table)
        return (
            self.relationship_batch_size
            if m and m.mode in RELATIONSHIP_MODES
            else self.node_batch_size
        )

    def due(self) -> list[tuple[str, str]]:
        """Buffer keys that should flush now: size threshold OR interval elapsed."""
        now = time.monotonic()
        interval_due = (now - self._last_flush) >= self.flush_interval
        out = []
        for key, buf in self._buffers.items():
            if interval_due or len(buf) >= self._limit_for(buf.database, buf.table):
                out.append(key)
        return out

    def flush_all(self) -> int:
        written = 0
        for key in list(self._buffers):
            written += self.flush(key)
        self._last_flush = time.monotonic()
        self._write_watermarks()
        return written

    def flush_due(self) -> int:
        keys = self.due()
        written = 0
        for key in keys:
            written += self.flush(key)
        if keys:
            self._last_flush = time.monotonic()
        self._write_watermarks()
        return written

    def _write_watermarks(self):
        now = time.monotonic()
        if now - self._last_watermark_write >= 5.0 and self._watermarks:
            from src import graph_state
            import datetime
            for db, ts in list(self._watermarks.items()):
                dt = datetime.datetime.fromtimestamp(ts / 1000.0, datetime.timezone.utc).isoformat()
                graph_state.set_state(self.pool, db, data_watermark=dt)
            self._watermarks.clear()
            self._last_watermark_write = now


    def flush(self, key: tuple[str, str]) -> int:
        buf = self._buffers.pop(key, None)
        if not buf or not buf.events:
            return 0
        database, table = key
        events = list(buf.events.values())

        buckets: dict[str, list] = {"upsert": [], "relationship": [], "delete": []}
        max_ts = 0
        for e in events:
            bucket = "relationship" if e.mode in RELATIONSHIP_MODES and e.op == "upsert" else e.op
            buckets[bucket].append(e)
            if "ts" in e.row:
                ts = e.row["ts"]
                if isinstance(ts, int) and ts > max_ts:
                    max_ts = ts
                    
        if max_ts > 0:
            current_max = self._watermarks.get(database, 0)
            if max_ts > current_max:
                self._watermarks[database] = max_ts

        coords = buf.coords                     # all offsets, including superseded ones
        total = 0
        try:
            for bucket in FLUSH_ORDER:
                group = buckets[bucket]
                if not group:
                    continue
                op = "delete" if bucket == "delete" else "upsert"
                total += self._write_group(database, table, op, group)
        except Exception:
            # Restore the unwritten buffer so it can be retried on subsequent flush cycles
            existing = self._buffers.get(key)
            if existing:
                for ev_key, ev in buf.events.items():
                    if ev_key not in existing.events:
                        existing.events[ev_key] = ev
                existing.coords.extend(buf.coords)
            else:
                self._buffers[key] = buf

            log.exception("flush failed database=%s table=%s events=%d",
                          database, table, len(events))
            if self.metrics:
                self.metrics.inc("flush_failures_total", database=database, table=table)
            raise

        self.offsets.release(coords)
        if self.metrics:
            self.metrics.inc("events_written_total", total, database=database, table=table)
        return total

    def _write_group(self, database: str, table: str, op: str, group: list) -> int:
        m = self.loader.get_mapping(database, table)
        if m.mode == "properties_on" and op == "delete":
            by_attr: dict[str, list] = defaultdict(list)
            for e in group:
                by_attr[e.attr_name].append(e)
            written = 0
            for attr, events in by_attr.items():
                written += self._run(database, table, op, events, attr)
            return written
        return self._run(database, table, op, group, None)

    def _run(self, database: str, table: str, op: str, events: list, attr: str | None) -> int:
        start_time = time.time()
        cypher = self._cypher(database, table, op, attr)
        rows = [e.row for e in events]


        instance = events[0].instance
        driver = self.pool.get(instance)

        timer = (
            self.metrics.timer("flush_duration_seconds", database=database, table=table)
            if self.metrics
            else _NullTimer()
        )
        with timer:
            try:
                with driver.session(database=database) as session:
                    self._execute_with_retry(session, cypher, rows, database)
            except Exception as e:
                if "DatabaseNotFound" in str(e) or "Graph not found" in str(e):
                    # Fallback to default neo4j database for Community Edition
                    with driver.session(database="neo4j") as session:
                        self._execute_with_retry(session, cypher, rows, "neo4j")
                else:
                    raise
        log.info("Successfully wrote to Neo4j: database=%s, table=%s, op=%s, rows=%s, took %.4f seconds", database, table, op, len(rows), time.time() - start_time)

        return len(rows)

    def _execute_with_retry(self, session, cypher: str, rows: list, database: str) -> None:
        """
        Concurrent relationship merges lock both endpoint nodes and will occasionally deadlock; the loser is rolled back and must retry.
        """
        from neo4j.exceptions import TransientError

        for attempt in range(self.max_attempts):
            try:
                session.execute_write(lambda tx: tx.run(cypher, batch=rows).consume())
                return
            except TransientError:
                if attempt == self.max_attempts - 1:
                    raise
                if self.metrics:
                    self.metrics.inc("deadlock_retries_total", database=database)
                time.sleep(
                    (self.backoff_base_ms / 1000) * (2 ** attempt) + random.uniform(0, 0.05)
                )


class _NullTimer:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False
