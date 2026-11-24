"""Config-time graph rules. `python main.py validate` fails when any is broken.

1. Every product table writes exactly one product label; setup/catalog tables none
   (enforced while building the mapping, see src/scope.py).
2. A label is either a product's or unscoped, never both.
3. A collection holds items of one product only (or only unscoped items).
4. A label written by several products uses product-prefixed keys, so their nodes never collide.
5. At most one HAS between the same two labels: HAS means containment, and a second meaning
   between the same pair needs its own verb (a ticket is ASSIGNED_TO and RESOLVED_BY an agent).
6. Every collection has exactly one owner (enforced while loading core.yaml).
"""
from __future__ import annotations

import re


def _has_pairs(eff) -> dict[tuple[str, str], list[str]]:
    pairs: dict[tuple[str, str], list[str]] = {}

    def add(a, b, where):
        pairs.setdefault((a, b), []).append(where)

    for m in eff.tables.values():
        if m.mode == "node" and m.label:
            for rel in m.relationships or []:
                if rel.type.upper() != "HAS":
                    continue
                a, b = (m.label, rel.target_label) if rel.direction == "out" else (rel.target_label, m.label)
                add(a, b, m.table)
        elif m.mode == "relationship_only" and (m.type or "").upper() == "HAS":
            add(m.from_label, m.to_label, m.table)
        for c in getattr(m, "collections", None) or []:
            # one collection = one meaning, however many tables fill it
            add(c["owner_label"], c["name"], f"collection {c['name']}")
            if m.mode == "node" and not c.get("bucket_column"):
                add(c["name"], m.label, f"collection {c['name']}")
    return pairs


def check(eff, scopes_cfg: dict) -> list[str]:
    problems = []

    writers: dict[str, list] = {}
    for m in eff.tables.values():
        if m.mode == "node" and m.label and not m.raw_cypher and not (
                m.merges_into and m.on_delete == "remove_own_properties"):
            writers.setdefault(m.label, []).append(m)

    for label, ms in sorted(writers.items()):
        ps = {m.product_label for m in ms}
        if None in ps and len(ps) > 1:
            problems.append(f"label {label} is written by unscoped and product tables "
                            f"({', '.join(sorted(m.table for m in ms))}); give it one home")

    col_products: dict[str, set] = {}
    for m in eff.tables.values():
        for c in getattr(m, "collections", None) or []:
            col_products.setdefault(c["name"], set()).add(m.product_label)
    for name, ps in sorted(col_products.items()):
        if len(ps) > 1:
            problems.append(f"collection {name} holds items of several products: "
                            f"{sorted(str(p) for p in ps)}")

    for label, ms in sorted(writers.items()):
        if len({m.product_label for m in ms}) < 2:
            continue
        prefixes = {}
        for m in ms:
            pre = re.match(r"^([A-Za-z_]+):", m.synthesize_key or "")
            if not pre:
                problems.append(f"{m.table}: label {label} is written by several products, so its "
                                f"key needs a product prefix (synthesize_key like 'crm:{{id}}')")
                continue
            prefixes.setdefault(pre.group(1), set()).add(m.product_label)
        for pre, ps in prefixes.items():
            if len(ps) > 1:
                problems.append(f"label {label}: key prefix '{pre}:' is used by several products")

    for (a, b), where in sorted(_has_pairs(eff).items()):
        if len(set(where)) > 1 and a != b:
            problems.append(f"{a}-HAS->{b} is defined by {len(set(where))} tables "
                            f"({', '.join(sorted(set(where)))}); use a verb for the second meaning")
    return problems
