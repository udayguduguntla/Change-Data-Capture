"""``python main.py integrity``: prove a tenant graph follows the rules. All zeros = healthy.

Checks (each returns a count and a few example keys):
  product_labels       product nodes without exactly one product label, or setup/catalog
                       nodes (Tenant, User, Product, ...) carrying one
  hub_nodes            leftover hub nodes (removed in v4.4)
  collections_orphan   collection nodes with no owner
  collections_empty    collection / month-bucket nodes holding nothing
  collections_mixed    collection items whose product differs from the collection's
  collections_duplicate  an owner with two collections of the same kind
  single_value_edges   a node with 2+ edges for a single-valued foreign key
  hierarchy_cycles     cycles through self-referencing edges (e.g. Account part_of Account)
  stubs_old            nodes created ahead of their row that never got filled in

``--fix`` removes orphan and empty collections (always safe: they hold nothing).
``--remove-stubs-older-than DAYS`` deletes unfilled stubs older than DAYS.
"""
from __future__ import annotations

import time

SAMPLE = 5


def _q(name: str) -> str:
    return "`" + str(name).replace("`", "") + "`"


def _collections(eff) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for m in eff.tables.values():
        for c in getattr(m, "collections", None) or []:
            spec = out.setdefault(c["name"], {"owner_edge": c.get("owner_edge"), "items": set(),
                                              "product": m.product_label, "bucketed": False})
            spec["items"].add(c.get("item_edge"))
            spec["bucketed"] |= bool(c.get("bucket_column"))
    return out


def _entity_labels(eff) -> list[str]:
    return sorted({m.label for m in eff.tables.values()
                   if m.mode == "node" and m.label and not m.raw_cypher})


def run_checks(session, eff, scopes_cfg: dict, stub_days: int = 7) -> dict[str, dict]:
    products = list(eff.all_scopes)
    report: dict[str, dict] = {}

    def put(name, rows):
        rows = list(rows)
        report[name] = {"count": sum(r[0] for r in rows) if rows and isinstance(rows[0], tuple) else len(rows),
                        "examples": [r[1] if isinstance(r, tuple) else r for r in rows][:SAMPLE]}

    unscoped = list(getattr(eff, "unscoped_labels", []) or [])
    recs = session.run(
        "MATCH (n) WITH n, [l IN labels(n) WHERE l IN $p] AS pl, "
        "     any(l IN labels(n) WHERE l IN $u) AS free "
        "WHERE (free AND size(pl) > 0) OR (NOT free AND size(pl) <> 1) "
        "RETURN labels(n) AS labels LIMIT 1000", p=products, u=unscoped)
    put("product_labels", [r["labels"] for r in recs])

    recs = session.run("MATCH (n) WHERE any(l IN labels(n) WHERE l ENDS WITH 'Hub') "
                       "RETURN labels(n) AS labels LIMIT 100")
    put("hub_nodes", [r["labels"] for r in recs])

    cols = _collections(eff)
    orphan, empty, mixed, dup = [], [], [], []
    for name, spec in sorted(cols.items()):
        for r in session.run(f"MATCH (c:{_q(name)}) WHERE NOT (c)<--() RETURN c.uid AS k LIMIT 100"):
            orphan.append(r["k"])
        for r in session.run(f"MATCH (c:{_q(name)}) WHERE NOT (c)-->() RETURN c.uid AS k LIMIT 100"):
            empty.append(r["k"])
        items = "|".join(_q(i) for i in sorted(x for x in spec["items"] if x))
        path = (f"(c:{_q(name)})-[:HAS_PERIOD]->(:LogPeriod)-[:{items}]->(x)" if spec["bucketed"]
                else f"(c:{_q(name)})-[:{items}]->(x)")
        cond = ("any(l IN labels(x) WHERE l IN $p)" if spec["product"] is None
                else "NOT $prod IN labels(x)")
        for r in session.run(f"MATCH {path} WHERE {cond} RETURN c.uid AS k LIMIT 100",
                             prod=spec["product"], p=products):
            mixed.append(r["k"])
        if spec["owner_edge"]:
            for r in session.run(
                    f"MATCH (o)-[:{_q(spec['owner_edge'])}]->(c:{_q(name)}) WITH o, count(c) AS k "
                    f"WHERE k > 1 RETURN count(o) AS n"):
                if r["n"]:
                    dup.append((r["n"], name))
    for r in session.run("MATCH (p:LogPeriod) WHERE NOT (p)-->() RETURN p.uid AS k LIMIT 100"):
        empty.append(r["k"])
    put("collections_orphan", orphan)
    put("collections_empty", empty)
    put("collections_mixed", mixed)
    put("collections_duplicate", dup)

    multi = []
    for m in eff.tables.values():
        if m.mode != "node" or not m.label:
            continue
        for rel in m.relationships or []:
            if rel.is_list or not (m.stale_cleanup or {}).get(rel.payload_key):
                continue
            arrow = f"-[r:{_q(rel.type.upper())}]->" if rel.direction == "out" else f"<-[r:{_q(rel.type.upper())}]-"
            rec = session.run(
                f"MATCH (n:{_q(m.label)}){arrow}(:{_q(rel.target_label)}) "
                f"WITH n, count(r) AS k WHERE k > 1 RETURN count(n) AS n").single()
            if rec and rec["n"]:
                multi.append((rec["n"], f"{m.label}.{rel.type}"))
    put("single_value_edges", multi)

    cyc = []
    seen = set()
    for m in eff.tables.values():
        for rel in (m.relationships or []) if m.mode == "node" else []:
            if rel.target_label == m.label and (m.label, rel.type) not in seen:
                seen.add((m.label, rel.type))
                rec = session.run(
                    f"MATCH (n:{_q(m.label)}) WHERE (n)-[:{_q(rel.type.upper())}*1..25]->(n) "
                    f"RETURN count(n) AS n").single()
                if rec and rec["n"]:
                    cyc.append((rec["n"], f"{m.label}.{rel.type}"))
    put("hierarchy_cycles", cyc)

    cutoff = int(time.time() * 1000) - stub_days * 86_400_000
    stubs = []
    for label in _entity_labels(eff):
        rec = session.run(
            f"MATCH (n:{_q(label)}) WHERE n._stub_since IS NOT NULL AND n._stub_since < $cut "
            f"RETURN count(n) AS n", cut=cutoff).single()
        if rec and rec["n"]:
            stubs.append((rec["n"], label))
    put("stubs_old", stubs)
    return report


def fix_collections(session, eff) -> int:
    """Delete orphan and empty collections / month buckets (they hold nothing)."""
    removed = 0
    names = list(_collections(eff)) + ["LogPeriod"]
    for _ in range(2):                       # buckets first, then collections they emptied
        for name in names:
            rec = session.run(f"MATCH (c:{_q(name)}) WHERE NOT (c)-->() OR NOT (c)<--() "
                              f"DETACH DELETE c RETURN count(*) AS n").single()
            removed += int(rec["n"]) if rec else 0
    return removed


def remove_old_stubs(session, eff, days: int) -> int:
    cutoff = int(time.time() * 1000) - days * 86_400_000
    removed = 0
    for label in _entity_labels(eff):
        rec = session.run(f"MATCH (n:{_q(label)}) WHERE n._stub_since IS NOT NULL AND n._stub_since < $cut "
                          f"DETACH DELETE n RETURN count(*) AS n", cut=cutoff).single()
        removed += int(rec["n"]) if rec else 0
    return removed
