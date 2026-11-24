"""Backfill.  Spec section 9."""

from __future__ import annotations

import logging
import time

from .coercion import coerce_native_row
from .errors import CoercionError
from .router import RoutedEvent

log = logging.getLogger(__name__)

DEFAULT_FETCH = 10000


def dependency_order(eff, names: list[str]) -> list[str]:
    """Order tables so each comes after the tables it points at.

    1. lookup-only tables (they fill the cache other tables resolve through)
    2. node tables, parents before children (topological over relationship targets);
       key-based edges would work in any order thanks to stub targets, but field-matched
       edges (a ticket's SLA by priority) need their target to exist already
    3. relationship-only and properties-on tables
    Cycles are broken alphabetically, so the order is always the same.
    """
    names = sorted(names)
    owner_of: dict[str, set[str]] = {}
    for n in names:
        m = eff.get(n)
        if m.mode == "node" and m.label:
            owner_of.setdefault(m.label, set()).add(n)

    def rank(n: str) -> int:
        mode = eff.get(n).mode
        return 0 if mode in ("lookup_source", "view") else 1 if mode == "node" else 2

    deps: dict[str, set[str]] = {}
    for n in names:
        m = eff.get(n)
        d: set[str] = set()
        for rel in getattr(m, "relationships", None) or []:
            d |= owner_of.get(rel.target_label, set())
        for lbl in (getattr(m, "from_label", None), getattr(m, "to_label", None),
                    getattr(m, "target_label", None) if m.mode == "properties_on" else None):
            if lbl:
                d |= owner_of.get(lbl, set())
        d.discard(n)
        deps[n] = {x for x in d if rank(x) <= rank(n)}

    out: list[str] = []
    done: set[str] = set()
    for r in (0, 1, 2):
        pending = [n for n in names if rank(n) == r]
        while pending:
            ready = [n for n in pending if deps[n] <= done | {x for x in names if rank(x) < r}]
            pick = ready[0] if ready else pending[0]         # cycle: take the first
            out.append(pick)
            done.add(pick)
            pending.remove(pick)
    return out


class Backfiller:
    def __init__(self, loader, registry, router, writer, pg_conn_factory, tenant_resolver=None):
        self.loader = loader
        self.registry = registry
        self.router = router
        self.writer = writer
        self.pg_conn_factory = pg_conn_factory
        self.tenant_resolver = tenant_resolver

    def _tables_for(self, org, product: str | None, table: str | None) -> list[str]:
        eff = self.loader.effective(org.database)
        out = []
        for name, m in eff.tables.items():
            if table and name != table:
                continue
            if m.product and m.product not in org.products:
                continue                       # not entitled
            if product and m.product != product:
                continue
            out.append(name)
        return dependency_order(eff, out)

    def run(self, organization_id: str, product: str | None = None, table: str | None = None,
            resume_from=None, chunk_size: int | None = None) -> dict:
        org = next(
            (o for o in self.registry.all() if o.organization_id == str(organization_id)),
            None,
        )
        if org is None:
            raise ValueError(f"organisation {organization_id!r} is not in the registry")
        if not org.routable():
            raise ValueError(
                f"organisation {organization_id!r} is inactive or unprovisioned; "
                "provision it before backfilling"
            )

        chunk = chunk_size or self.writer.node_batch_size
        tables = self._tables_for(org, product, table)
        if not tables:
            log.warning("no tables to backfill for organisation %s", organization_id)
        stats: dict[str, dict] = {}
        for name in tables:
            stats[name] = self._backfill_table_with_retry(org, name, chunk, resume_from)
        self.writer.flush_all()
        return stats

    def _backfill_table_with_retry(self, org, table: str, chunk: int, resume_from, max_attempts: int = 3) -> dict:
        for attempt in range(1, max_attempts + 1):
            try:
                with self.pg_conn_factory() as conn:
                    conn.autocommit = True           # no transaction outlives a single read
                    return self._backfill_table(conn, org, table, chunk, resume_from)
            except Exception as exc:
                log.warning("Attempt %d/%d backfilling table %s failed: %s", attempt, max_attempts, table, exc)
                if attempt < max_attempts:
                    time.sleep(attempt * 2.0)
                else:
                    log.error("Failed to backfill table %s after %d attempts: %s", table, max_attempts, exc)
                    return {
                        "read": 0, "written": 0, "quarantined": 0,
                        "elapsed_seconds": 0, "last_primary_key": None,
                        "error": str(exc)
                    }

    @staticmethod
    def _primary_key(conn, table: str) -> list[str]:
        """The table's real primary key columns, in order ([] when it has none)."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT a.attname FROM pg_index i "
                "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                "WHERE i.indrelid = %s::regclass AND i.indisprimary "
                "ORDER BY array_position(i.indkey, a.attnum)", (table,))
            return [r[0] for r in cur.fetchall()]

    @staticmethod
    def _db_now_ms(conn) -> int:
        with conn.cursor() as cur:
            cur.execute("SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint")
            return int(cur.fetchone()[0])

    def _read_chunks(self, conn, table: str, where: list, params: dict, chunk: int):
        """Yield (snapshot_ts, columns, rows) chunk by chunk.

        With a primary key: keyset pages (`WHERE (pk...) > (last...) ORDER BY pk LIMIT n`), each
        its own short statement on an autocommit connection, so no lock or snapshot is held
        between chunks (DDL such as ALTER TABLE and VACUUM are never blocked for long). Every
        chunk is stamped with the database clock just before it is read: a live change
        committed later carries a later stamp and wins; one committed earlier is in the chunk.

        Without a primary key: one server-side cursor inside an explicit transaction that is
        always closed when the table is done.
        """
        import psycopg
        try:
            pk = self._primary_key(conn, table)
        except psycopg.errors.UndefinedTable:
            log.warning("Table %s does not exist in postgres, skipping.", table)
            return
        base_where = list(where)
        if pk:
            cols_sql = ", ".join(pk)
            last = None
            while True:
                w, p = list(base_where), dict(params)
                if last is not None:
                    w.append(f"({cols_sql}) > ({', '.join(f'%(k{i})s' for i in range(len(pk)))})")
                    p.update({f"k{i}": v for i, v in enumerate(last)})
                sql = (f"SELECT * FROM {table}" + (f" WHERE {' AND '.join(w)}" if w else "")
                       + f" ORDER BY {cols_sql} LIMIT {int(chunk)}")
                ts = self._db_now_ms(conn)
                with conn.cursor() as cur:
                    cur.execute(sql, p)
                    rows = cur.fetchall()
                    columns = [d.name for d in cur.description] if cur.description else []
                if not rows:
                    return
                yield ts, columns, rows
                if len(rows) < chunk:
                    return
                idx = [columns.index(c) for c in pk]
                last = tuple(rows[-1][i] for i in idx)
        else:
            sql = f"SELECT * FROM {table}" + (f" WHERE {' AND '.join(base_where)}" if base_where else "")
            with conn.transaction():
                ts = self._db_now_ms(conn)
                with conn.cursor(name=f"backfill_{table.replace('.', '_')}") as cur:
                    cur.itersize = chunk
                    cur.execute(sql, params)
                    columns = None
                    while True:
                        rows = cur.fetchmany(chunk)
                        if not rows:
                            break
                        if columns is None:
                            columns = [d.name for d in cur.description]
                        yield ts, columns, rows

    def _backfill_table(self, conn, org, table: str, chunk: int, resume_from) -> dict:
        m = self.loader.get_mapping(org.database, table)
        started = time.monotonic()
        read = written = quarantined = skipped = 0

        pk = self._primary_key_column(m, self._derived_columns(m.table))
        tenant_col = self.router.tenant_column

        # Check if the table actually has the tenant column
        has_tenant_col = False
        with conn.cursor() as check_cur:
            schema, tablename = m.table.split('.') if '.' in m.table else ('public', m.table)
            check_cur.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s",
                (schema, tablename, tenant_col)
            )
            has_tenant_col = check_cur.fetchone() is not None

        if has_tenant_col:
            where = [f"{tenant_col}::text = %(tenant)s"]
            params = {"tenant": str(org.tenant_id)}
        else:
            where = []
            params = {}

        if resume_from is not None and pk:
            where.append(f"{pk} > %(resume)s")
            params["resume"] = resume_from

        props_by_name = {p.name: p for p in m.properties}
        last_pk = None
        batch = []

        for snapshot_ts, columns, records in self._read_chunks(conn, m.table, where, params, chunk):
            for record in records:
                raw = dict(zip(columns, record))
                read += 1
                if not has_tenant_col:
                    is_global = m.tenant and m.tenant.get("scope") == "global"
                    if not is_global:
                        resolved_tenant = raw.get(tenant_col)
                        if not resolved_tenant and table == self.router.control_table:
                            resolved_tenant = raw.get(self.loader.section("control_plane").get("organization_id_column", "id"))
                        if not resolved_tenant and self.router.tenant_resolver:
                            resolved_tenant = self.router.tenant_resolver.resolve(table, raw)
                        if str(resolved_tenant) != str(org.tenant_id):
                            continue
                if pk:
                    last_pk = raw.get(pk, last_pk)
                try:
                    row = coerce_native_row(raw, props_by_name)
                    built = self.router._build(row, m, "upsert", {"ts_ms": snapshot_ts}, org.database)
                except CoercionError as exc:
                    log.error("backfill %s: %s", table, exc)
                    quarantined += 1
                    continue
                if not isinstance(built, tuple):
                    if type(built).__name__ == "Skipped":
                        skipped += 1           # e.g. lookup-only tables: cache warmed, no node
                    else:
                        quarantined += 1
                        log.debug("backfill %s: %s", table, built)
                    continue
                event_row, collapse_key, attr_name, op = built
                batch.append(
                    RoutedEvent(
                        database=org.database, instance=org.instance, table=table,
                        op=op, mode=m.mode, row=event_row, collapse_key=collapse_key,
                        attr_name=attr_name, topic="<backfill>",
                    )
                )
                if len(batch) >= chunk:
                    written += self._flush(batch)
                    batch = []
        if batch:
            written += self._flush(batch)

        elapsed = time.monotonic() - started
        log.info(
            "backfill table=%s database=%s read=%d written=%d skipped=%d quarantined=%d "
            "elapsed=%.1fs last_pk=%s",
            table, org.database, read, written, skipped, quarantined, elapsed, last_pk,
        )
        return {
            "read": read, "written": written, "skipped": skipped, "quarantined": quarantined,
            "elapsed_seconds": round(elapsed, 2), "last_primary_key": last_pk,
        }

    def _flush(self, batch: list) -> int:
        for e in batch:
            self.writer.add(e)
        return self.writer.flush_all()

    @staticmethod
    def _primary_key_column(m, derived: set | None = None) -> str | None:
        """A real source column to page by. Never one the pipeline computes itself
        (a `derived:` column, e.g. cdp.order_items.inventory_product_id): Postgres
        does not have it."""
        derived = derived or set()
        ok = lambda c: c if c and c not in derived else None
        if m.synthesize_key:
            return None
        if m.mode == "node":
            return ok(m.key_column)
        if m.mode == "properties_on":
            return ok(m.target_key_column)
        if m.mode == "relationship_only":
            return ok(m.from_column) or ok(m.to_column)
        return None

    def _derived_columns(self, table: str) -> set:
        derived = (getattr(self.loader, "core_meta", {}) or {}).get("derived") or {}
        return {k[len(table) + 1:] for k in derived if k.startswith(table + ".")}
        if m.mode == "node":
            return m.key_column
        if m.mode == "properties_on":
            return m.target_key_column
        if m.mode == "relationship_only":
            return m.from_column
        return None