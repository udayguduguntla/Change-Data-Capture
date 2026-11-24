"""Cypher generation.  Spec section 7."""

from __future__ import annotations

import re

from .config_loader import TableMapping
from .errors import MappingError
from .naming import safe_property_name

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# "legacy"  -> CALL { WITH n, e ...
SUBQUERY_SYNTAXES = ("legacy", "scoped")


def _ident(value: str | None, what: str) -> str:
    """Reject anything that is not a bare identifier."""
    if value is None or not _IDENT.match(str(value)):
        raise MappingError(
            f"{what} must be a bare identifier matching [A-Za-z_][A-Za-z0-9_]*, "
            f"got {value!r}"
        )
    return str(value)


def _labels_clause(labels: list[str]) -> str:
    return "".join(f":{_ident(l, 'extra label')}" for l in labels)


def _product_of(m: TableMapping, label: str) -> str | None:
    ps = (getattr(m, "target_scopes", None) or {}).get(label) or []
    return ps[0] if ps else None


def _scope_set(var: str, m: TableMapping, label: str, indent: str = "") -> list[str]:
    """Give a node this mapping creates as a side effect (stub target, category node)
    a product label, but only if it has none yet: its owning table sets the real one."""
    p = _product_of(m, label)
    return [_product_if_missing(var, m, p, indent)] if p else []


def _product_if_missing(var: str, m: TableMapping, product, indent: str = "") -> str:
    if not product:
        return f"{indent}WITH *"  # unscoped node: never given a product label
    allp = "[" + ", ".join(f"'{x}'" for x in (m.all_products or [product])) + "]"
    return (f"{indent}FOREACH (_ IN CASE WHEN size([l IN labels({var}) WHERE l IN {allp}]) = 0 "
            f"THEN [1] ELSE [] END | SET {var}:`{product}`)")


def _product_enforce(var: str, m: TableMapping, product, indent: str = "") -> list[str]:
    """Exactly one product label (remove any other, set this one); none when unscoped."""
    others = [x for x in (m.all_products or []) if x != product]
    out = [f"{indent}REMOVE {var}" + "".join(f":`{x}`" for x in others)] if others else []
    return out + ([f"{indent}SET {var}:`{product}`"] if product else [])


def _subquery(body: list[str], tag: str, syntax: str) -> list[str]:
    """Wrap ``body`` in a CALL subquery importing n and e. It returns one row whatever
    happens inside, so the outer row always continues. Grouping inside happens by node
    only (never by the event map), which every Cypher engine supports."""
    head = ["WITH n, e", "CALL {", "  WITH n, e"] if syntax == "legacy" else ["WITH n, e", "CALL (n, e) {"]
    return head + ["  " + l for l in body] + [f"  RETURN count(*) AS _{tag}", "}"]


def _detach_and_cleanup(match: list[str]) -> list[str]:
    """``match`` binds (p)-[r]->(n): edges into the item n that must go. Delete them, then
    delete parents (collections or month buckets) left with nothing, then collections
    left without buckets. Emptiness is counted excluding what is being deleted, so the
    result is the same whether the engine applies deletes at once or at the end."""
    return match + [
        "WITH n, collect(DISTINCT r) AS _r, collect(DISTINCT p) AS _p",
        "FOREACH (x IN _r | DELETE x)",
        "WITH n, _r, _p",
        "UNWIND (CASE WHEN size(_p) = 0 THEN [null] ELSE _p END) AS x",
        "OPTIONAL MATCH (x)-[q]->() WHERE NOT q IN _r",
        "OPTIONAL MATCH (gp)-[:HAS_PERIOD]->(x)",
        "WITH n, x, gp, count(q) AS _left",
        "WITH n, collect(CASE WHEN x IS NOT NULL AND _left = 0 THEN x END) AS _empty, "
        "collect(DISTINCT gp) AS _gps",
        "FOREACH (x IN _empty | DETACH DELETE x)",
        "WITH _empty, _gps",
        "UNWIND (CASE WHEN size(_gps) = 0 THEN [null] ELSE _gps END) AS c",
        "OPTIONAL MATCH (c)-[q2]->(y) WHERE NOT y IN _empty",
        "WITH c, count(q2) AS _gleft",
        "WITH collect(CASE WHEN c IS NOT NULL AND _gleft = 0 THEN c END) AS _egp",
        "FOREACH (x IN _egp | DETACH DELETE x)",
    ]


def _collection_parent_uid(c: dict, i: int) -> str:
    base = f"'{c['name']}:' + toString(e.`_c{i}`)"
    return base + (f" + ':' + e.`_cb{i}`" if c.get("bucket_column") else "")


def resolve_target(label: str, table: str, cfg: dict) -> str:
    """create | match | defer | skip"""
    if not cfg:
        return "create"
    owner = cfg.get("node_ownership", {}).get(label, {}).get("owner")
    owners = owner if isinstance(owner, list) else [owner]
    if table in owners:
        return "create"                      # the owning table
    if label in cfg.get("create_target_allowed", []):
        return "create"                      # bounded dimension stub
    return cfg.get("edge_policy", {}).get(table, {}).get("on_missing", cfg.get("default_on_missing", "skip"))

def build_node_upsert(m: TableMapping, subquery_syntax: str = "legacy") -> str:
    if m.mode != "node":
        raise MappingError(f"{m.table}: build_node_upsert called on mode {m.mode!r}")
    if subquery_syntax not in SUBQUERY_SYNTAXES:
        raise MappingError(f"unknown subquery_syntax {subquery_syntax!r}")

    label = _ident(m.label, "label")
    key = _ident(m.key, "key property")

    lines = [
        "UNWIND $batch AS e",
        f"MERGE (n:{label} {{{key}: e.key}})",
        "  ON CREATE SET n._source_version = coalesce(e.ts, 0)",
        "WITH n, e WHERE coalesce(e.ts, 0) >= coalesce(n._source_version, 0)",
        "SET n += e.props, n._source_version = coalesce(e.ts, 0)",
        "REMOVE n._stub_since",
    ]
    if m.extra_labels:
        lines.append(f"SET n{_labels_clause(m.extra_labels)}")
    lines.extend(_product_enforce("n", m, getattr(m, "product_label", None)))

    lines.append("WITH n, e")

    gi = getattr(m, 'graph_integrity', {})
    if gi and "exclusive_label_groups" in gi:
        exclusive = gi["exclusive_label_groups"].get(m.table)
        if exclusive:
            group = exclusive.get("group", [])
            if group:
                lines.append(f"REMOVE n:{', n:'.join(group)}")

    state_labels = []
    for cl in getattr(m, 'conditional_labels', None) or []:
        state_labels.append(cl['label'])
    sub = getattr(m, 'sublabels', None) or {}
    if isinstance(sub, dict) and sub.get('column'):
        state_labels.extend((sub.get('mapping') or {}).values())
    keep = {m.label, *(m.extra_labels or [])}
    state_labels = sorted({_ident(l, 'state label') for l in state_labels} - keep)
    if state_labels:
        lines.append("REMOVE n" + "".join(f":`{l}`" for l in state_labels))

    # E1: Dynamic Labels (Native Cypher)
    if getattr(m, 'conditional_labels', None):
        for cl in m.conditional_labels:
            lbl = cl['label']
            lines.append(f"FOREACH (_ IN CASE WHEN e.dynamic_labels IS NOT NULL AND '{lbl}' IN e.dynamic_labels THEN [1] ELSE [] END | SET n:`{lbl}`)")

    # E2: Promoted Properties (Native Cypher)
    if getattr(m, 'promote_property_to_node', None):
        for p_col, p_spec in m.promote_property_to_node.items():
            plbl = p_spec['label']
            pkey = p_spec.get('key', 'id')
            pname = p_spec.get('name_prop', 'name')
            pedge = p_spec['edge']
            lines.append(f"FOREACH (promo IN [x IN coalesce(e.promotions, []) WHERE x.label = '{plbl}' AND x.edge = '{pedge}'] |")
            lines.append(f"  MERGE (target:`{plbl}` {{`{pkey}`: promo.key_val}})")
            lines.append(f"  SET target.`{pname}` = promo.name_val")
            lines.extend(_scope_set("target", m, plbl, "  "))
            lines.append(f"  MERGE (n)-[:`{pedge}`]->(target)")
            lines.append(")")

    promo_specs = {v['label']: v for v in (getattr(m, 'promote_property_to_node', None) or {}).values()}
    for h in getattr(m, 'derive_hierarchy', None) or []:
        child, parent = promo_specs.get(h.get('from')), promo_specs.get(h.get('to'))
        if not child or not parent:
            continue
        cl, pl, edge = _ident(child['label'], 'label'), _ident(parent['label'], 'label'), _ident(h['edge'], 'edge')
        ck, pk = _ident(child.get('key', 'id'), 'key'), _ident(parent.get('key', 'id'), 'key')
        lines.append(f"FOREACH (c_ IN [x IN coalesce(e.promotions, []) WHERE x.label = '{cl}'] |")
        lines.append(f"  FOREACH (p_ IN [y IN coalesce(e.promotions, []) WHERE y.label = '{pl}'] |")
        lines.append(f"    MERGE (hc:`{cl}` {{`{ck}`: c_.key_val}})")
        lines.append(f"    MERGE (hp:`{pl}` {{`{pk}`: p_.key_val}})")
        lines.append(f"    MERGE (hc)-[:`{edge}`]->(hp)))")

    # E3: Conditional Relationships (Native Cypher)
    if getattr(m, 'conditional_relationships', None):
        for cr in m.conditional_relationships:
            tlbl = cr.get('target_label') or cr.get('to')
            tkey = cr.get('target_key') or cr.get('to_key')
            ttype = cr['type']
            tdir = "out" if "to" in cr else cr.get('direction', 'out')
            arrow_l, arrow_r = ("-", "->") if tdir == "out" else ("<-", "-")
            lines.append(f"FOREACH (dyn IN [x IN coalesce(e.dynamic_relationships, []) WHERE x.target_label = '{tlbl}' AND x.type = '{ttype}'] |")
            lines.append(f"  MERGE (target:`{tlbl}` {{`{tkey}`: dyn.target_val}})")
            lines.extend(_scope_set("target", m, tlbl, "  "))
            lines.append(f"  MERGE (n){arrow_l}[:`{ttype}`]{arrow_r}(target)")
            lines.append(")")


    if getattr(m, 'sublabels', None):
        col = m.sublabels.get('column')
        mapping = m.sublabels.get('mapping', {})
        if col and mapping:
            for val, lbl in mapping.items():
                v = str(val).lower().replace("'", "")
                lines.append(f"FOREACH (_ IN CASE WHEN toLower(toString(e.props.`{col}`)) = '{v}' THEN [1] ELSE [] END | SET n:`{lbl}`)")


    for i, c in enumerate(getattr(m, "collections", None) or []):
        name = _ident(c["name"], "collection name")
        olabel = _ident(c["owner_label"], "collection owner label")
        okey = _ident(c["owner_key"], "collection owner key")
        oedge = _ident(c["owner_edge"], "collection owner edge")
        iedge = _ident(c["item_edge"], "collection item edge")
        product = m.product_label
        oprod = _product_of(m, c["owner_label"]) or product
        lines.append("WITH n, e")
        if subquery_syntax == "legacy":
            lines += ["CALL {", "  WITH n, e", f"  WITH n, e WHERE e.`_c{i}` IS NOT NULL"]
        else:
            lines += ["CALL (n, e) {", f"  WITH n, e WHERE e.`_c{i}` IS NOT NULL"]
        # The owner is created as a stub if it has not arrived yet, so the collection is
        # never left unattached; the owner's own row fills it in later.
        if c.get("create_owner") is False:
            # the owner must already exist (found by a secondary key): never create it
            lines.append(f"  MATCH (o:`{olabel}` {{`{okey}`: e.`_c{i}`}})")
        else:
            lines.append(f"  MERGE (o:`{olabel}` {{`{okey}`: e.`_c{i}`}})")
            lines.append("    ON CREATE SET o._stub_since = timestamp()")
            lines.append(_product_if_missing("o", m, oprod, "  "))
        lines.append(f"  MERGE (col:`{name}` {{uid: '{name}:' + toString(e.`_c{i}`)}})")
        lines.extend(_product_enforce("col", m, product, "  "))
        lines.append(f"  MERGE (o)-[:`{oedge}`]->(col)")
        if c.get("bucket_column"):
            lines.append(f"  MERGE (lp:LogPeriod {{uid: {_collection_parent_uid(c, i)}}})")
            lines.append(f"    ON CREATE SET lp.period = e.`_cb{i}`")
            lines.extend(_product_enforce("lp", m, product, "  "))
            lines.append("  MERGE (col)-[:HAS_PERIOD]->(lp)")
            lines.append(f"  MERGE (lp)-[:`{iedge}`]->(n)")
        else:
            lines.append(f"  MERGE (col)-[:`{iedge}`]->(n)")
        lines.append(f"  RETURN count(*) AS _col{i}")
        lines.append("}")
        # Re-parent: the item moved to another owner (or lost it): detach it from the old
        # collection / bucket and remove whatever became empty.
        lines.extend(_subquery(_detach_and_cleanup([
            f"OPTIONAL MATCH (p)-[r:`{iedge}`]->(n)",
            f"  WHERE (p:`{name}` OR (p:LogPeriod AND p.uid STARTS WITH '{name}:'))",
            f"    AND (e.`_c{i}` IS NULL OR p.uid <> {_collection_parent_uid(c, i)})",
        ]), f"reparent{i}", subquery_syntax))

    if m.relationships:
        lines.append("WITH n, e")
        for i, rel in enumerate(m.relationships):
            tlabel = _ident(rel.target_label, "target_label")
            tkey = _ident(rel.target_key, "target_key") if not getattr(rel, 'match_fields', None) else ""
            rtype = _ident(rel.type, "relationship type")
            var = f"t{i}"
            pkey = rel.payload_key
            arrow_l, arrow_r = ("-", "->") if rel.direction == "out" else ("<-", "-")

            if getattr(rel, 'is_list', False):
                item_var = "list_item"
            else:
                item_var = f"e.{pkey}"

            if getattr(rel, 'match_fields', None):
                null_checks = " AND ".join(f"e.props.`{v}` IS NOT NULL" for v in rel.match_fields.values())
                if subquery_syntax == "legacy":
                    head = ["WITH n, e", "CALL {", "  WITH n, e", f"  WITH n, e WHERE {null_checks}"]
                else:
                    head = ["WITH n, e", "CALL (n, e) {", f"  WITH n, e WHERE {null_checks}"]

                match_props = ", ".join(f"`{k}`: e.props.`{v}`" for k, v in rel.match_fields.items())
                mprod = (getattr(m, "match_products", None) or {}).get(rel.target_label)
                tpat = f"{tlabel}:`{mprod}`" if mprod else tlabel
                lines.extend(head)
                if getattr(rel, 'create_target', False):
                    lines.append(f"  MERGE ({var}:{tpat} {{{match_props}}})")
                    lines.append(f"    ON CREATE SET {var}._stub_since = timestamp()")
                    lines.extend(_scope_set(var, m, tlabel, "  "))
                else:
                    lines.append(f"  MATCH ({var}:{tpat} {{{match_props}}})")
                lines.append(f"  MERGE (n){arrow_l}[:{rtype}]{arrow_r}({var})")
                lines.append("  RETURN count(*) AS _r" + str(i))
                lines.append("}")
                if (getattr(m, "stale_cleanup", None) or {}).get(rel.payload_key):
                    same = " AND ".join(f"x.`{k}` = e.props.`{v}`" for k, v in rel.match_fields.items())
                    lines += _subquery([
                        f"OPTIONAL MATCH (n){arrow_l}[s:{rtype}]{arrow_r}(x:{tpat})",
                        f"  WHERE NOT coalesce({same}, false)",
                        "WITH collect(s) AS _s",
                        "FOREACH (x IN _s | DELETE x)",
                    ], f"stale{i}", subquery_syntax)
                continue

            if subquery_syntax == "legacy":
                head = ["WITH n, e", "CALL {", "  WITH n, e", f"  WITH n, e WHERE e.{pkey} IS NOT NULL AND size(e.{pkey}) > 0" if getattr(rel, 'is_list', False) else f"  WITH n, e WHERE e.{pkey} IS NOT NULL"]
            else:
                head = ["WITH n, e", "CALL (n, e) {", f"  WITH n, e WHERE e.{pkey} IS NOT NULL AND size(e.{pkey}) > 0" if getattr(rel, 'is_list', False) else f"  WITH n, e WHERE e.{pkey} IS NOT NULL"]

            if getattr(rel, 'is_list', False):
                head.append(f"  UNWIND e.{pkey} AS list_item")

            if getattr(rel, 'coerce_to_int', False):
                # toInteger() is null for anything that is not a whole number, so a bad value
                # simply links nothing (no regex needed).
                if not getattr(rel, 'is_list', False):
                    head[-1] += f" AND toInteger(toString({item_var})) IS NOT NULL"
                else:
                    head.append(f"  WITH n, e, {item_var} WHERE toInteger(toString({item_var})) IS NOT NULL")
                val_expr = f"toInteger(toString({item_var}))"
            else:
                val_expr = item_var

            lines.extend(head)
            if getattr(rel, 'create_target', False) and getattr(rel, 'acts_as_user', False):
                # A product role of a user (SalesRep, MarketingAgent): derived from the user
                # column, so it is complete as created (no stub marker) and linked to its User.
                lines.append(f"  MERGE ({var}:{tlabel} {{{tkey}: {val_expr}}})")
                lines.extend(_scope_set(var, m, tlabel, "  "))
                lines.append(f"  MERGE (u{i}:User {{id: {val_expr}}})")
                lines.append(f"    ON CREATE SET u{i}._stub_since = timestamp()")
                lines.append(f"  MERGE (u{i})-[:ACTS_AS]->({var})")
            elif getattr(rel, 'create_target', False):
                lines.append(f"  MERGE ({var}:{tlabel} {{{tkey}: {val_expr}}})")
                lines.append(f"    ON CREATE SET {var}._stub_since = timestamp()")
                lines.extend(_scope_set(var, m, tlabel, "  "))
            else:
                lines.append(f"  MATCH ({var}:{tlabel} {{{tkey}: {val_expr}}})")
            lines.append(f"  MERGE (n){arrow_l}[:{rtype}]{arrow_r}({var})")
            lines.append("  RETURN count(*) AS _r" + str(i))
            lines.append("}")
            if (getattr(m, "stale_cleanup", None) or {}).get(rel.payload_key):
                # The edge follows its column: when the value changes or becomes null, the
                # old edge goes. Without this a reassigned ticket keeps both owners.
                if getattr(rel, 'is_list', False):
                    conv = "toInteger(v)" if getattr(rel, 'coerce_to_int', False) else "v"
                    keep = f"x.{tkey} IN [v IN coalesce(e.{pkey}, []) | {conv}]"
                else:
                    cur = f"toInteger(e.{pkey})" if getattr(rel, 'coerce_to_int', False) else f"e.{pkey}"
                    keep = f"x.{tkey} = {cur}"
                lines += _subquery([
                    f"OPTIONAL MATCH (n){arrow_l}[s:{rtype}]{arrow_r}(x:{tlabel})",
                    f"  WHERE NOT coalesce({keep}, false)",
                    "WITH collect(s) AS _s",
                    "FOREACH (x IN _s | DELETE x)",
                ], f"stale{i}", subquery_syntax)

    lines.append('RETURN count(*) AS _processed')
    return "\n".join(lines)


def build_node_delete(m: TableMapping) -> str:
    if m.mode != "node":
        raise MappingError(f"{m.table}: build_node_delete called on mode {m.mode!r}")
    label = _ident(m.label, "label")
    key = _ident(m.key, "key property")
    if m.on_delete == "remove_own_properties":
        # This table only adds properties to a node another table owns: a delete here
        # removes what it added and leaves the node for its owner.
        props = [f"n.`{safe_property_name(p)}`" for p in (m.own_properties or [])]
        return (
            "UNWIND $batch AS e\n"
            f"MATCH (n:{label} {{{key}: e.key}})\n"
            + (f"REMOVE {', '.join(props)}" if props else "RETURN count(n) AS _noop")
        )
    lines = ["UNWIND $batch AS e", f"MATCH (n:{label} {{{key}: e.key}})"]
    owned = getattr(m, "owned_collections", None) or []
    if owned:
        # An owner's collections (and their month buckets) go with it; the items stay,
        # because their own tables decide about them.
        types = "|".join(sorted({f"`{_ident(o['owner_edge'], 'owner edge')}`" for o in owned}))
        is_col = " OR ".join(f"oc:`{_ident(o['name'], 'collection')}`" for o in owned)
        lines += [
            f"OPTIONAL MATCH (n)-[:{types}]->(oc) WHERE {is_col}",
            "OPTIONAL MATCH (oc)-[:HAS_PERIOD]->(lp)",
            "WITH n, collect(DISTINCT oc) AS _oc, collect(DISTINCT lp) AS _olp",
            "FOREACH (x IN _olp | DETACH DELETE x)",
            "FOREACH (x IN _oc | DETACH DELETE x)",
            "WITH n, {} AS e",
        ]
    cols = getattr(m, "collections", None) or []
    if cols:
        conds = " OR ".join(
            f"(type(r) = '{_ident(c['item_edge'], 'item edge')}' AND (p:`{_ident(c['name'], 'collection')}` "
            f"OR (p:LogPeriod AND p.uid STARTS WITH '{c['name']}:')))" for c in cols)
        lines += [
        ] + _subquery(_detach_and_cleanup([
            "OPTIONAL MATCH (p)-[r]->(n)",
            f"  WHERE {conds}",
        ]), "itemclean", "legacy") + [
            "WITH n",
            "DETACH DELETE n",
            "RETURN count(*) AS _deleted",
        ]
    else:
        lines.append("DETACH DELETE n")
    return "\n".join(lines)


def build_relationship_upsert(m: TableMapping) -> str:
    if m.mode != "relationship_only":
        raise MappingError(f"{m.table}: build_relationship_upsert called on mode {m.mode!r}")
    fl, fk = _ident(m.from_label, "from_label"), _ident(m.from_key, "from_key")
    tl, tk = _ident(m.to_label, "to_label"), _ident(m.to_key, "to_key")
    rtype = _ident(m.type, "relationship type")
    if getattr(m, "on_delete", "detach") == "ignore":
        return "RETURN 1"  # No-op query

    gi = getattr(m, 'graph_integrity', {})
    a_action = "match" if getattr(m, "from_create", None) is False else resolve_target(fl, m.table, gi)
    b_action = "match" if getattr(m, "to_create", None) is False else resolve_target(tl, m.table, gi)

    a_cypher = (f"MERGE (a:{fl} {{{fk}: e.from_key}}) ON CREATE SET a._stub_since = timestamp()"
                if a_action == "create" else f"MATCH (a:{fl} {{{fk}: e.from_key}})")
    b_cypher = (f"MERGE (b:{tl} {{{tk}: e.to_key}}) ON CREATE SET b._stub_since = timestamp()"
                if b_action == "create" else f"MATCH (b:{tl} {{{tk}: e.to_key}})")
    if a_action == "create" and _scope_set("a", m, fl):
        a_cypher += "\n        " + _scope_set("a", m, fl)[0]
    if b_action == "create" and _scope_set("b", m, tl):
        b_cypher += "\n        " + _scope_set("b", m, tl)[0]

    props_clause = "\n        SET r += e.props" if m.properties else ""
    cols = getattr(m, "collections", None) or []
    if cols:
        # The "from" side keeps its memberships in one collection node:
        # (Contact)-[:HAS_SEGMENT_MEMBERSHIP]->(SegmentMembership)-[:IN_SEGMENT]->(Segment)
        c = cols[0]
        name = _ident(c["name"], "collection name")
        oedge = _ident(c["owner_edge"], "owner edge")
        lines = ["UNWIND $batch AS e", f"        {a_cypher}", f"        {b_cypher}",
                 f"        MERGE (col:`{name}` {{uid: '{name}:' + toString(e.from_key)}})"]
        lines += _product_enforce("col", m, m.product_label, "        ")
        lines += [f"        MERGE (a)-[:`{oedge}`]->(col)",
                  f"        MERGE (col)-[r:{rtype}]->(b){props_clause}"]
        return "\n".join(lines)
    return (
        "UNWIND $batch AS e\n"
        f"        {a_cypher}\n"
        f"        {b_cypher}\n"
        f"        MERGE (a)-[r:{rtype}]->(b){props_clause}"
    )


def build_relationship_delete(m: TableMapping) -> str:
    if m.mode != "relationship_only":
        raise MappingError(f"{m.table}: build_relationship_delete called on mode {m.mode!r}")
    fl, fk = _ident(m.from_label, "from_label"), _ident(m.from_key, "from_key")
    tl, tk = _ident(m.to_label, "to_label"), _ident(m.to_key, "to_key")
    rtype = _ident(m.type, "relationship type")
    if getattr(m, "on_delete", "detach") == "ignore":
        return "RETURN 1"  # No-op query
    cols = getattr(m, "collections", None) or []
    if cols:
        name = _ident(cols[0]["name"], "collection name")
        return (
            "UNWIND $batch AS e\n"
            f"MATCH (col:`{name}` {{uid: '{name}:' + toString(e.from_key)}})-[r:{rtype}]->(b:{tl} {{{tk}: e.to_key}})\n"
            "WITH col, r, size([(col)-[q]->() WHERE startNode(q) = col AND q <> r | 1]) AS _rest\n"
            "DELETE r\n"
            "WITH col, _rest WHERE _rest = 0\n"
            "DETACH DELETE col"
        )
    return (
        "UNWIND $batch AS e\n"
        f"MATCH (a:{fl} {{{fk}: e.from_key}})-[r:{rtype}]->(b:{tl} {{{tk}: e.to_key}})\n"
        "DELETE r"
    )


def build_properties_on_upsert(m: TableMapping) -> str:
    if m.mode != "properties_on":
        raise MappingError(f"{m.table}: build_properties_on_upsert called on mode {m.mode!r}")
    label = _ident(m.target_label, "target_label")
    key = _ident(m.target_key, "target_key")
    # SET n += merges the map and leaves every unmentioned property untouched.
    return (
        "UNWIND $batch AS e\n"
        f"MERGE (n:{label} {{{key}: e.target_key}})\n"
        "SET n += e.props"
        + ("\n" + _scope_set("n", m, m.target_label)[0] if _scope_set("n", m, m.target_label) else "")
    )


def build_properties_on_remove(m: TableMapping, attr_name: str) -> str:
    """
    Cypher cannot remove a dynamically named property, so the name is built in Python from the sanitised attribute and embedded as a backticked literal.
    """
    if m.mode != "properties_on":
        raise MappingError(f"{m.table}: build_properties_on_remove called on mode {m.mode!r}")
    label = _ident(m.target_label, "target_label")
    key = _ident(m.target_key, "target_key")
    safe = safe_property_name(f"{m.prefix or ''}{attr_name}")
    return (
        "UNWIND $batch AS e\n"
        f"MATCH (n:{label} {{{key}: e.target_key}})\n"
        f"REMOVE n.`{safe}`"
    )


def build_all(m: TableMapping, subquery_syntax: str = "legacy") -> dict[str, str]:
    """Every query a mapping can produce, keyed by operation.  Used by dry-run."""
    if m.raw_cypher:
        return {"raw": m.raw_cypher}
    if m.mode == "node":
        return {
            "upsert": build_node_upsert(m, subquery_syntax),
            "delete": build_node_delete(m),
        }
    if m.mode == "relationship_only":
        return {
            "upsert": build_relationship_upsert(m),
            "delete": build_relationship_delete(m),
        }
    if m.mode == "properties_on":
        return {
            "upsert": build_properties_on_upsert(m),
            "delete": build_properties_on_remove(m, "DYNAMIC"),
        }
    if m.mode in ("lookup_source", "view"):
        return {}
    raise MappingError(f"{m.table}: unknown mode {m.mode!r}")


def build_constraint(label: str, key: str, name: str) -> str:
    """Uniqueness constraint DDL.  Created BEFORE any data is written."""
    return (
        f"CREATE CONSTRAINT {_ident(name, 'constraint name')} IF NOT EXISTS "
        f"FOR (n:{_ident(label, 'label')}) REQUIRE n.{_ident(key, 'key')} IS UNIQUE"
    )
