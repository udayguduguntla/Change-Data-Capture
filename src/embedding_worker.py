"""Durable embedding worker.  Runs as its own process: ``python main.py embed``."""
from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass

from .embedder import build_embed_text, embedding_config, get_embedder, model_tag
from .graph_state import get_state, parse_tag, set_state

log = logging.getLogger(__name__)

PENDING = "_embed_pending"            # on a node only while it waits; removed after embedding
NEXT_PENDING = "_embed_next_pending"  # only during a model switch (see src/model_switch.py)
NEXT_SUFFIX = "_next"                 # _embedding_next holds the new model's vector during a switch
STATE_TTL = 30.0                      # seconds a database's model state is cached


@dataclass(frozen=True)
class Target:
    label: str
    prop: str
    fields: tuple[str, ...]


def targets_for(loader, database: str) -> list[Target]:
    """One Target per (label, property). Tables merged into one label union their fields."""
    eff = loader.effective(database)
    merged: dict[tuple[str, str], list[str]] = {}
    for _table, m in sorted(eff.tables.items()):
        if m.mode != "node" or not getattr(m, "embed", None) or not m.label:
            continue
        prop = m.embed.get("property", "_embedding")
        fields = merged.setdefault((m.label, prop), [])
        for f in m.embed.get("fields", []):
            if f not in fields:
                fields.append(f)
    return [Target(label, prop, tuple(fields)) for (label, prop), fields in merged.items()]


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


class EmbeddingWorker:
    def __init__(self, loader, registry, pool, cfg: dict | None = None, embedder=None):
        self.loader = loader
        self.registry = registry
        self.pool = pool
        self.cfg = cfg or embedding_config(loader)
        self.embedder = embedder or get_embedder(self.cfg)
        self.tag = model_tag(self.cfg)
        self.page = int(self.cfg.get("worker_page_size", 256))
        self.idle_sleep = float(self.cfg.get("worker_idle_seconds", 5))
        self._running = False
        self.stats = {"embedded": 0, "skipped_empty": 0, "skipped_race": 0}
        self._state: dict[str, tuple[float, str, bool]] = {}
        self._refused: set[str] = set()
        self._missing_db: set[str] = set()

    def ready(self, org) -> bool:
        """False while the tenant database does not exist yet (e.g. the pipeline is still
        creating it after a reset). The database is checked again on the next pass, so
        work starts by itself once it exists."""
        try:
            with self.pool.get(org.instance).session(database=org.database) as s:
                s.run("RETURN 1").consume()
        except Exception as exc:                        # noqa: BLE001 - only "not found" is expected
            code = str(getattr(exc, "code", "") or "")
            if "DatabaseNotFound" in code or "does not exist" in str(exc) or "22N51" in str(exc):
                if org.database not in self._missing_db:
                    log.info("database=%s does not exist yet; skipping it until it is created",
                             org.database)
                self._missing_db.add(org.database)
                return False
            raise
        if org.database in self._missing_db:
            log.info("database=%s now exists; embedding it", org.database)
            self._missing_db.discard(org.database)
        return True

    # -- which vector this worker writes for a database ----------------------

    def mode(self, org) -> tuple[str, bool]:
        """("normal", switching) | ("shadow", True) | ("refuse", False).

        normal  the database's active model is this worker's model: write _embedding
        shadow  a switch to this worker's model is running: write _embedding_next
        refuse  the database uses another model; embedding here would mix vector spaces
        """
        now = time.time()
        hit = self._state.get(org.database)
        if hit and now - hit[0] < STATE_TTL:
            return hit[1], hit[2]
        st = get_state(self.pool, org.database, org.instance)
        active, nxt = st.get("embed_model"), st.get("embed_next_model")
        if not active:
            _m, dims = parse_tag(self.tag)
            set_state(self.pool, org.database, org.instance, embed_model=self.tag, embed_dims=dims)
            result = ("normal", False)
        elif active == self.tag:
            result = ("normal", bool(nxt))
        elif nxt == self.tag:
            result = ("shadow", True)
        else:
            result = ("refuse", False)
            if org.database not in self._refused:
                self._refused.add(org.database)
                log.error("database=%s uses embedding model %s but this worker runs %s; "
                          "skipping it. Start a switch with `python main.py embed "
                          "--switch-model ...` or run the worker with the active model.",
                          org.database, active, self.tag)
        self._state[org.database] = (now, *result)
        return result

    # -- queue maintenance -------------------------------------------------

    def mark(self, org, mode: str) -> dict[str, int]:
        """Put existing nodes on the queue."""
        if not self.ready(org):
            return {}
        out = {}
        with self.pool.get(org.instance).session(database=org.database) as s:
            for t in targets_for(self.loader, org.database):
                where = "true" if mode == "all" else f"n.{_q(t.prop)} IS NULL"
                # Auto-commit tx: required by CALL {..} IN TRANSACTIONS.
                rec = s.run(
                    f"MATCH (n:{_q(t.label)}) WHERE {where} "
                    f"CALL (n) {{ SET n.{PENDING} = true }} IN TRANSACTIONS OF 5000 ROWS "
                    f"RETURN count(*) AS c",
                ).single()
                out[t.label] = rec["c"] if rec else 0
                log.info("database=%s label=%s marked=%d (mode=%s)",
                         org.database, t.label, out[t.label], mode)
        return out

    def backlog(self, org) -> dict[str, int]:
        if not self.ready(org):
            return {}
        out = {}
        with self.pool.get(org.instance).session(database=org.database) as s:
            for t in targets_for(self.loader, org.database):
                rec = s.run(f"MATCH (n:{_q(t.label)}) "
                            f"RETURN count(CASE WHEN n.{PENDING} = true THEN 1 END) AS c, "
                            f"count(CASE WHEN n.{NEXT_PENDING} = true THEN 1 END) AS nx").single()
                out[t.label] = rec["c"] if rec else 0
                if rec and rec["nx"]:
                    out[f"{t.label} (switch)"] = rec["nx"]
        return out

    # -- the work -----------------------------------------------------------

    def _text(self, rec, t: Target) -> str:
        """Built from the node's current properties; no text is stored in the graph."""
        return build_embed_text(t.label, t.fields, lambda f: rec["f"].get(f))

    def process_page(self, org, t: Target) -> int:
        """Embed up to one page of pending nodes for one label. Returns nodes handled."""
        mode, switching = self.mode(org)
        if mode == "refuse":
            return 0
        flag = NEXT_PENDING if mode == "shadow" else PENDING
        prop = t.prop + NEXT_SUFFIX if mode == "shadow" else t.prop
        # During a switch every fresh text change must also reach the new model.
        also = f"SET n.{NEXT_PENDING} = true " if (mode == "normal" and switching) else ""
        driver = self.pool.get(org.instance)
        with driver.session(database=org.database) as s:
            recs = list(s.run(
                f"MATCH (n:{_q(t.label)}) WHERE n.{flag} = true "
                f"RETURN elementId(n) AS id, n._source_version AS v, "
                f"n {{ {', '.join('.' + _q(f) for f in t.fields) or '.__none__'} }} AS f "
                f"LIMIT $limit",
                limit=self.page,
            ))
            if not recs:
                return 0

            rows, empty = [], []
            for r in recs:
                text = self._text(r, t)
                (rows if text else empty).append((r["id"], r["v"], text))

            if empty:
                s.execute_write(lambda tx: tx.run(
                    f"UNWIND $ids AS id MATCH (n) WHERE elementId(n) = id "
                    f"REMOVE n.{flag}", ids=[e[0] for e in empty]).consume())
                self.stats["skipped_empty"] += len(empty)

            if rows:
                t0 = time.time()
                vecs = self.embedder.encode_documents([r[2] for r in rows])
                enc_s = time.time() - t0
                payload = [{"id": i, "v": v, "vec": vec} for (i, v, _), vec in zip(rows, vecs)]

                def write(tx):
                    res = tx.run(
                        f"UNWIND $rows AS r "
                        f"MATCH (n) WHERE elementId(n) = r.id "
                        f"  AND coalesce(n._source_version, -1) = coalesce(r.v, -1) "
                        f"CALL db.create.setNodeVectorProperty(n, $prop, r.vec) "
                        f"{also}"
                        f"REMOVE n.{flag} "
                        f"RETURN count(n) AS c",
                        rows=payload, prop=prop)
                    return res.single()["c"]

                written = s.execute_write(write)
                self.stats["embedded"] += written
                self.stats["skipped_race"] += len(rows) - written
                log.info("database=%s label=%s embedded=%d encode=%.2fs (%.1f ms/text)",
                         org.database, t.label, written, enc_s, 1000 * enc_s / len(rows))
            return len(recs)

    def run_once(self, orgs) -> int:
        handled = 0
        for org in orgs:
            if not self.ready(org):
                continue
            for t in targets_for(self.loader, org.database):
                try:
                    handled += self.process_page(org, t)
                except Exception:
                    # Nothing is lost: the nodes are still pending and are retried on the next pass.
                    log.exception("embedding page failed database=%s label=%s",
                                  org.database, t.label)
        return handled

    def run_forever(self, orgs_fn) -> None:
        self._running = True
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._stop)
        log.info("embedding worker started: model=%s", self.tag)
        while self._running:
            if self.run_once(orgs_fn()) == 0:
                time.sleep(self.idle_sleep)
        log.info("embedding worker stopped: %s", self.stats)

    def _stop(self, *_):
        self._running = False
