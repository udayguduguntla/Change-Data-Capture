"""Product labels: exactly one per product node, none on unscoped (setup/catalog) nodes.

    Support  Sales  Marketing  ContactCentre  CDP  WebTracking      (+ unscoped: no label)

The label of a node is the product of the table that writes it (access.domain, with the
overrides in config/scopes.yaml). A merged secondary table (on_delete:
remove_own_properties) adds properties to its primary's node, so it uses the primary's
product. Nodes a table creates besides its own (stub targets, category nodes,
collections, month buckets) take the product of their label when exactly one product
writes that label, otherwise the product of the table creating them.

Who may READ which product is not stored on nodes; see agent access in the registry.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from .errors import ConfigError

DEFAULT_PRODUCTS = {
    "ticketing": "Support", "crm": "Sales", "campaign": "Marketing",
    "ccaas": "ContactCentre", "cdp": "CDP", "webtracking": "WebTracking",
}
NONE = None          # product of an unscoped table / label


def load_scopes_config(config_dir: Path) -> dict:
    import yaml
    path = Path(config_dir) / "scopes.yaml"
    doc = (yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else None) or {}
    products = dict(doc.get("products") or DEFAULT_PRODUCTS)
    unscoped = set(doc.get("unscoped") or ["common"])
    names = set(products.values())
    overrides = {}
    for where in ("label_overrides", "table_overrides"):
        overrides[where] = {}
        for k, v in (doc.get(where) or {}).items():
            if str(v).lower() == "none":
                v = NONE
            elif v not in names:
                raise ConfigError(f"scopes.yaml: {where}.{k} -> unknown product {v!r}")
            overrides[where][k] = v
    return {"products": products, "unscoped": unscoped, **overrides}


def product_names(cfg: dict) -> list[str]:
    return sorted(set(cfg["products"].values()))


def product_of_domain(cfg: dict, domain: str | None, where: str):
    if domain in cfg["unscoped"]:
        return NONE
    if not domain:
        raise ConfigError(f"{where}: access.domain is required to choose its product label")
    if domain not in cfg["products"]:
        raise ConfigError(f"{where}: access.domain {domain!r} is neither a product in scopes.yaml "
                          f"nor listed under unscoped")
    return cfg["products"][domain]


def _collections(m) -> list[dict]:
    col = getattr(m, "collections", None)
    return [c for c in (col or []) if isinstance(c, dict) and c.get("name")]


def _created_labels(m) -> list[str]:
    """Labels a table may MERGE besides its own node."""
    out = []
    for rel in m.relationships or []:
        if rel.create_target:
            out.append(rel.target_label)
    for spec in (m.promote_property_to_node or {}).values():
        out.append(spec["label"])
    for cr in m.conditional_relationships or []:
        lbl = cr.get("target_label") or cr.get("to")
        if lbl:
            out.append(lbl)
    for c in _collections(m):
        out.append(c["name"])
        if c.get("owner_label"):
            out.append(c["owner_label"])
        if c.get("bucket_column"):
            out.append("LogPeriod")
    return out


def apply_scopes(eff, cfg: dict) -> dict[str, list[str]]:
    """Set m.product_label / m.scope_labels / m.target_scopes on every mapping."""
    products = cfg["products"]
    names = set(products.values())
    lab_ov, tab_ov = cfg["label_overrides"], cfg["table_overrides"]

    def own_product(m):
        if m.table in tab_ov:
            return tab_ov[m.table]
        if m.label in lab_ov:
            return lab_ov[m.label]
        return product_of_domain(cfg, (m.access or {}).get("domain"), m.table)

    node_tables = [m for m in eff.tables.values() if m.mode == "node" and m.label and not m.raw_cypher]
    for m in node_tables:
        if m.label in names:
            raise ConfigError(f"{m.table}: label {m.label!r} collides with a product label")
    primary = {}
    for m in node_tables:
        if not m.merges_into:
            primary.setdefault(m.label, m)

    label_products: dict[str, set] = {}
    for m in eff.tables.values():
        if m.mode == "node" and m.label and not m.raw_cypher:
            if m.merges_into and m.on_delete == "remove_own_properties" and m.label in primary:
                p = own_product(primary[m.label])      # same node as the primary table
            else:
                p = own_product(m)
            m.product_label = p
            label_products.setdefault(m.label, set()).add(p)
        else:
            m.product_label = own_product(m) if (m.access or {}).get("domain") else NONE

    def target_product(label: str, m):
        owners = label_products.get(label)
        if label in lab_ov:
            return lab_ov[label]
        if owners and len(owners) == 1:
            return next(iter(owners))
        if label in eff.stub_labels:           # derived nodes (e.g. MarketingAgent): creator's product
            return m.product_label
        return m.product_label

    for m in eff.tables.values():
        m.all_products = sorted(names)
        # Field-matched edges (no key) to a label several products write must stay
        # inside the writer's product, e.g. a ticket's SLA is a Support SLA.
        m.match_products = {rel.target_label: m.product_label
                            for rel in (m.relationships or [])
                            if rel.match_fields and len(label_products.get(rel.target_label, ())) > 1}
        m.scope_labels = [m.product_label] if (m.mode == "node" and m.product_label) else []
        targets: dict[str, list[str]] = {}
        if m.mode == "node":
            for lbl in _created_labels(m):
                targets[lbl] = [target_product(lbl, m)]
            for c in _collections(m):
                targets[c["name"]] = [m.product_label]          # a collection holds one product
        elif m.mode == "relationship_only":
            for lbl in (m.from_label, m.to_label):
                if lbl:
                    targets[lbl] = [target_product(lbl, m)]
            for c in _collections(m):
                targets[c["name"]] = [m.product_label]
                if c.get("owner_label"):
                    targets[c["owner_label"]] = [target_product(c["owner_label"], m)]
        elif m.mode == "properties_on" and m.target_label:
            targets[m.target_label] = [target_product(m.target_label, m)]
        m.target_scopes = targets

    all_labels: dict[str, set] = {k: set(v) for k, v in label_products.items()}
    for m in eff.tables.values():
        for lbl, ps in (m.target_scopes or {}).items():
            if lbl not in label_products:
                all_labels.setdefault(lbl, set()).update(ps)
    eff.label_scopes = {k: sorted(x for x in v if x) or [] for k, v in all_labels.items()}
    eff.unscoped_labels = sorted(k for k, v in all_labels.items() if v == {NONE})
    eff.home_scope = {k: (next(iter(v)) if len(v) == 1 else own_product(primary[k]) if k in primary
                          else sorted(x for x in v if x)[0])
                      for k, v in all_labels.items()}
    eff.all_scopes = sorted(names)
    return eff.label_scopes


# -- reports -----------------------------------------------------------------------

def scope_report(eff) -> list[dict]:
    return [{"label": lbl, "product": eff.home_scope.get(lbl) or "(none)",
             "products": eff.label_scopes[lbl] or ["(none)"]}
            for lbl in sorted(eff.label_scopes)]


def rbac_markdown(eff) -> str:
    by: dict[str, list[str]] = {s: [] for s in eff.all_scopes}
    for lbl, ps in sorted(eff.label_scopes.items()):
        for p in ps:
            by.setdefault(p, []).append(lbl)
    lines = [
        "# Product labels",
        "",
        "Generated by `python main.py scopes --write-doc`. A node written from a product's",
        "tables carries exactly one product label. Setup and catalog nodes carry none: every",
        "agent may read them. An agent reads a node when it has no product label or one of",
        "the agent's products.",
        "",
        "| Label | Product |",
        "|---|---|",
    ]
    lines += [f"| `{r['label']}` | {', '.join(r['products'])} |" for r in scope_report(eff)]
    lines += ["", "## Labels per product", ""]
    for s in eff.all_scopes:
        lines += [f"### {s}", "", ", ".join(f"`{x}`" for x in by.get(s, [])) or "_none_", ""]
    lines += ["### No product label (readable by every product)", "",
              ", ".join(f"`{x}`" for x in eff.unscoped_labels) or "_none_", ""]
    return "\n".join(lines) + "\n"


# -- Community edition: enforce products in the agent's read path --------------------

def _nodes_in(value: Any):
    labels = getattr(value, "labels", None)
    if labels is not None and hasattr(value, "element_id"):
        yield value
        return
    if hasattr(value, "start_node") and hasattr(value, "end_node"):
        for n in (value.start_node, value.end_node):
            if n is not None:
                yield n
        return
    if hasattr(value, "nodes") and hasattr(value, "relationships"):
        yield from value.nodes
        return
    if isinstance(value, dict):
        for v in value.values():
            yield from _nodes_in(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            yield from _nodes_in(v)


def record_in_scope(record_values: Iterable[Any], allowed: set[str], products: set[str]) -> bool:
    """A node is readable when it has no product label, or one of ``allowed``."""
    for v in record_values:
        for n in _nodes_in(v):
            own = set(n.labels) & products
            if own and not (own & allowed):
                return False
    return True


def scoped_read(driver, database: str, products: Iterable[str], cypher: str,
                params: dict | None = None, all_products: Iterable[str] | None = None
                ) -> tuple[list[dict], int]:
    """Read-only query; drops records touching a node of a product not in ``products``.

    Nodes without a product label (setup and catalog) are always readable."""
    allowed = set(products)
    every = set(all_products or DEFAULT_PRODUCTS.values())

    def work(tx):
        return [r for r in tx.run(cypher, **(params or {}))]

    with driver.session(database=database) as s:
        records = s.execute_read(work)
    kept = [r.data() if hasattr(r, "data") else dict(r) for r in records
            if record_in_scope(r.values() if hasattr(r, "values") else r, allowed, every)]
    return kept, len(records) - len(kept)
