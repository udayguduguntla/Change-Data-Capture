"""``python main.py reconcile-deletes``: the safety net for deletes the CDC stream missed.

CDC removes a node when its row is deleted. A delete can still be missed: the pipeline was
down past Kafka retention, a topic was reset, or a row was removed before the connector
existed. This compares, label by label, the keys in Postgres with the keys in the graph and
deletes graph nodes whose row is gone (or matches the table's ``delete_when`` rule).

Only nodes written by their own table are candidates (``_source_version`` is set). Stub
nodes created ahead of their row by a relationship are left alone.

Dry run by default; ``--apply`` deletes.
"""
from __future__ import annotations

import logging
import string

from .embedding_worker import _q

log = logging.getLogger(__name__)


def _template_fields(template: str) -> list[str]:
    return [f for _, f, _, _ in string.Formatter().parse(template) if f]


def _evaluate_when(when: dict, row: dict) -> bool:
    from .router import _evaluate_when as ev
    return ev(when, row)


def _source_keys(conn, m, tenant_col: str, tenant_id: str) -> set[str] | None:
    """Every graph key this table should produce for the tenant, or None if unreadable."""
    cols = _template_fields(m.synthesize_key) if m.synthesize_key else [m.key_column]
    cols = list(dict.fromkeys(cols + list((m.delete_when or {}).keys())))
    schema, _, name = m.table.rpartition(".")
    import psycopg
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s "
                        "AND table_name=%s AND column_name=%s", (schema or "public", name, tenant_col))
            has_tenant = cur.fetchone() is not None
        sql = f"SELECT {', '.join(cols)} FROM {m.table}"
        params: tuple = ()
        if has_tenant:
            sql += f" WHERE {tenant_col}::text = %s"
            params = (str(tenant_id),)
        out: set[str] = set()
        with conn.cursor(name=f"reconcile_{m.table.replace('.', '_')}") as cur:
            cur.itersize = 10000
            cur.execute(sql, params)
            for rec in cur:
                row = dict(zip(cols, rec))
                if m.delete_when and _evaluate_when(m.delete_when, row):
                    continue
                if m.synthesize_key:
                    key = m.synthesize_key.format(**{k: ("" if v is None else v) for k, v in row.items()})
                else:
                    key = row[m.key_column]
                if key is not None:
                    out.add(str(key))
        return out
    except psycopg.Error as exc:
        conn.rollback()
        log.warning("reconcile: cannot read %s (%s); its label is skipped", m.table, exc)
        return None


def reconcile_org(pool, loader, org, conn, tenant_col: str, apply: bool = False,
                  labels: list[str] | None = None) -> dict:
    driver = pool.get(org.instance)
    with driver.session(database=org.database) as s:
        s.run("RETURN 1").consume()          # refuse to run against a fallback/shared database
    eff = loader.effective(org.database)
    owners: dict[str, list] = {}
    for m in eff.tables.values():
        if (m.mode != "node" or not m.label or m.raw_cypher or not m.emit_to_graph
                or m.on_delete == "remove_own_properties"):
            continue
        if m.product and org.products and m.product not in org.products:
            continue
        if labels and m.label not in labels:
            continue
        owners.setdefault(m.label, []).append(m)

    report = {}
    from .cypher_builder import build_node_delete
    for label, maps in sorted(owners.items()):
        key = maps[0].key
        # 1. Graph keys FIRST. A row created while we read the source then shows up in
        #    the source read and is never mistaken for an orphan.
        with driver.session(database=org.database) as s:
            graph = {str(r["k"]): r["k"] for r in s.run(
                f"MATCH (n:{_q(label)}) WHERE n._source_version IS NOT NULL "
                f"RETURN n.{_q(key)} AS k")}
        if not graph:
            continue
        # 2. Source keys.
        expected: set[str] = set()
        readable = True
        for m in maps:
            keys = _source_keys(conn, m, tenant_col, org.tenant_id)
            if keys is None:
                readable = False
                break
            expected |= keys
        if not readable:
            report[label] = "skipped (source unreadable)"
            continue
        if not expected:
            report[label] = f"skipped ({len(graph)} nodes but no source rows; check the source)"
            continue
        orphans = [graph[k] for k in graph if k not in expected]
        if orphans and apply:
            # The pipeline's own delete query, so collections left empty go too.
            q = build_node_delete(maps[0])
            with driver.session(database=org.database) as s:
                for i in range(0, len(orphans), 1000):
                    s.run(q, batch=[{"key": k} for k in orphans[i:i + 1000]]).consume()
        if orphans:
            report[label] = len(orphans)
            log.info("database=%s label=%s orphans=%d %s", org.database, label, len(orphans),
                     "deleted" if apply else "(dry run)")
    return report
