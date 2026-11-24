"""In-memory FK lookup cache for derived columns declared in core.yaml derived:."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LookupSpec:
    table: str
    match_local: str
    match_remote: str
    select: str
    scope: str | None = None      # column that must match between row and lookup table (e.g. organization_id)


class DerivedLookupCache:
    """Maps (lookup_table, match_remote_value) -> select_value from CDC rows."""

    def __init__(self) -> None:
        self._forward: dict[tuple[str, Any], Any] = {}
        self._reverse: dict[tuple[str, Any], Any] = {}
        # fallback(spec, local_value, scope_value) -> value | None. Set by the runtime to a
        # Postgres lookup, so a restart (empty cache) or a table that is not streamed still
        # resolves. Results are cached.
        self.fallback = None
        self._misses: dict = {}      # key -> when a fallback last found nothing
        self.miss_ttl = 600.0        # seconds before asking Postgres again for a missing value

    def ingest(self, table: str, row: dict[str, Any], spec: LookupSpec) -> None:
        remote = row.get(spec.match_remote)
        selected = row.get(spec.select)
        if remote is None:
            return
        key = (spec.table, remote)
        if selected is not None:
            self._forward[key] = selected
            self._reverse[(spec.table, selected)] = remote

    def evict(self, table: str, row: dict[str, Any], spec: LookupSpec) -> None:
        remote = row.get(spec.match_remote)
        if remote is None:
            return
        key = (spec.table, remote)
        selected = self._forward.pop(key, None)
        if selected is not None:
            self._reverse.pop((spec.table, selected), None)

    def resolve(self, spec: LookupSpec, local_value: Any, scope_value: Any = None) -> Any | None:
        if local_value is None:
            return None
        key = (spec.table, local_value) if not spec.scope else (spec.table, scope_value, local_value)
        if key in self._forward:
            return self._forward[key]
        if not spec.scope and (spec.table, local_value) in self._forward:
            return self._forward[(spec.table, local_value)]
        if self.fallback is None:
            return None
        import time as _time
        missed = self._misses.get(key)
        if missed is not None and _time.monotonic() - missed < self.miss_ttl:
            return None                        # asked recently and not found: do not ask again yet
        value = self.fallback(spec, local_value, scope_value)
        if value is not None:
            self._forward[key] = value
            self._misses.pop(key, None)
        else:
            if len(self._misses) > 200_000:
                self._misses.clear()
            self._misses[key] = _time.monotonic()
        return value


def parse_lookup_spec(raw: dict[str, Any]) -> LookupSpec | None:
    lookup = raw.get("lookup")
    if not isinstance(lookup, dict):
        return None
    required = ("table", "match_local", "match_remote", "select")
    if not all(lookup.get(k) for k in required):
        return None
    return LookupSpec(
        table=str(lookup["table"]),
        match_local=str(lookup["match_local"]),
        match_remote=str(lookup["match_remote"]),
        select=str(lookup["select"]),
        scope=str(lookup["scope"]) if lookup.get("scope") else None,
    )


def postgres_fallback(pg_factory):
    """A lookup fallback that reads the lookup table in Postgres (one short query per miss)."""
    import logging
    import re as _re
    log = logging.getLogger(__name__)
    ident = _re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$")
    state = {"conn": None}

    def lookup(spec: LookupSpec, local_value, scope_value):
        for name in (spec.table, spec.match_remote, spec.select, spec.scope or "x"):
            if not ident.match(name):
                log.warning("lookup %s: unsafe identifier %r; skipped", spec.table, name)
                return None
        sql = f"SELECT {spec.select} FROM {spec.table} WHERE {spec.match_remote}::text = %s"
        params = [str(local_value)]
        if spec.scope:
            if scope_value is None:
                return None
            sql += f" AND {spec.scope}::text = %s"
            params.append(str(scope_value))
        sql += " LIMIT 1"
        try:
            if state["conn"] is None or state["conn"].closed:
                state["conn"] = pg_factory()
                state["conn"].autocommit = True
            with state["conn"].cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return row[0] if row else None
        except Exception as exc:                      # noqa: BLE001 - a lookup must never stop the pipeline
            log.warning("lookup %s failed: %s", spec.table, exc)
            try:
                state["conn"].close()
            except Exception:                         # noqa: BLE001
                pass
            state["conn"] = None
            return None
    return lookup