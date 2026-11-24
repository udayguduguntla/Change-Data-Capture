"""Configuration and mapping loading.  Spec sections 4 and 6."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)

from .errors import ConfigError

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

MODES = ("node", "relationship_only", "properties_on", "lookup_source")

# Types a property may declare.  ``decimal`` is ours (spec had no money type).
PROPERTY_TYPES = ("string", "integer", "float", "boolean", "datetime", "date", "decimal")

# Fields an overlay is forbidden to declare.  Spec section 6.3.
OVERLAY_FORBIDDEN = ("key", "label", "mode")
NODE_DELETE_MODES = ("detach", "remove_own_properties")


@dataclass(frozen=True)
class PropertySpec:
    name: str
    source_column: str | None = None # NEW: original DB column if renamed
    type: str | None = None          # None = pass through whatever coercion produced
    scale: int | None = None         # mandatory for type == decimal
    currency_column: str | None = None
    source: str = "column"           # "column" | "custom" (from the JSONB blob)

    @property
    def graph_name(self) -> str:
        """Property name as written into Neo4j (custom fields carry a prefix)."""
        return self.name


@dataclass(frozen=True)
class RelationshipSpec:
    type: str
    direction: str                   # "out" | "in"
    target_label: str
    target_key: str = ""
    from_column: str = ""
    create_target: bool = False
    coerce_to_int: bool = False
    match_fields: dict | None = None
    resolver: str | None = None
    is_list: bool = False
    to_key_template: str | None = None
    from_key_template: str | None = None
    acts_as_user: bool = False     # target is a product role of a User: also (User)-[:ACTS_AS]->(target)

    @property
    def payload_key(self) -> str:
        """Key under which the router places this relationship's FK value."""
        return f"{self.type}_{self.from_column or self.target_label}"


@dataclass
class TableMapping:
    table: str
    mode: str
    product: str | None = None
    # mode: node
    label: str | None = None
    key: str | None = None
    key_column: str | None = None
    synthesize_key: str | None = None
    properties: list[PropertySpec] = field(default_factory=list)
    custom_field: str | None = None
    tenant: dict | None = None
    custom_properties: list[PropertySpec] = field(default_factory=list)
    custom_prefix: str = "custom_"
    custom_dynamic: bool = False
    custom_fields_whitelist: list[str] = field(default_factory=list)
    custom_fields_blocklist: list[str] = field(default_factory=list)
    custom_fields_blocklist_action: str = "drop_silently"
    stop_dynamic_keys: list[str] = field(default_factory=list)
    extra_labels: list = field(default_factory=list)
    conditional_labels: list = field(default_factory=list)
    promote_property_to_node: dict = field(default_factory=dict)
    derive_hierarchy: list = field(default_factory=list)
    sublabels: dict = field(default_factory=dict)
    relationships: list = field(default_factory=list)
    conditional_relationships: list = field(default_factory=list)
    access: dict | None = None
    on_delete: str = "detach"        # node: "detach" | "remove_own_properties"
    delete_when: dict = field(default_factory=dict)   # app soft-delete -> graph delete
    merges_into: bool = False
    readable_by: list = field(default_factory=list)
    own_properties: list = field(default_factory=list)  # remove_own_properties only
    scope_labels: list = field(default_factory=list)    # set by src.scope.apply_scopes
    target_scopes: dict = field(default_factory=dict)
    edge_budget: int | None = None
    collections: list = field(default_factory=list)   # see _parse_collections
    owned_collections: list = field(default_factory=list)  # on an owner table: [{name, owner_edge}]
    from_create: bool | None = None   # relationship_only: False = match the endpoint, never create it
    query_indexes: list = field(default_factory=list)
    to_create: bool | None = None
    product_label: str = ""                            # set by src.scope.apply_scopes
    all_products: list = field(default_factory=list)   # every product label name
    match_products: dict = field(default_factory=dict) # multi-product target label -> product filter
    stale_cleanup: dict = field(default_factory=dict)  # rel payload_key -> bool
    # mode: relationship_only
    from_label: str | None = None
    from_key: str | None = None
    from_column: str | None = None
    to_label: str | None = None
    to_key: str | None = None
    to_column: str | None = None
    type: str | None = None
    # mode: properties_on
    target_label: str | None = None
    target_key: str | None = None
    target_key_column: str | None = None
    synthesize_key: str | None = None
    attr_key_column: str | None = None
    synthesize_key: str | None = None
    attr_value_column: str | None = None
    prefix: str | None = None
    # raw escape hatch
    raw_cypher: str | None = None
    graph_integrity: dict = field(default_factory=dict)
    raw_path: str | None = None
    embed: dict | None = None
    emit_to_graph: bool = True


@dataclass
class EffectiveMapping:
    database: str
    organization_id: str | None
    products: set[str]
    tables: dict[str, TableMapping]
    stub_labels: dict[str, str]      # label -> key property
    overlay_present: bool
    industry: str | None = None
    upper_ontology: dict[str, list[str]] = field(default_factory=dict)
    hubs: dict[str, dict] = field(default_factory=dict)
    collections: dict[str, dict] = field(default_factory=dict)
    merged_labels: dict[str, list[str]] = field(default_factory=dict)
    derived: dict[str, dict] = field(default_factory=dict)
    sensitivity_rubric: dict[str, str] = field(default_factory=dict)
    properties_not_nodes: list[dict] = field(default_factory=list)
    graph_integrity: dict = field(default_factory=dict)
    label_scopes: dict[str, list[str]] = field(default_factory=dict)
    home_scope: dict[str, str] = field(default_factory=dict)
    all_scopes: list[str] = field(default_factory=list)

    def secondary_keys(self) -> set[tuple[str, str]]:
        """(label, property) pairs used to FIND nodes that are not the label's primary key,
        e.g. Contact.campaign_contact_id or Product.sku. Provisioning indexes them."""
        primary = self.all_labels()
        out: set[tuple[str, str]] = set()

        def add(label, prop):
            if label and prop and primary.get(label) != prop:
                out.add((label, prop))
        for m in self.tables.values():
            for rel in getattr(m, "relationships", None) or []:
                add(rel.target_label, rel.target_key)
                for k in (rel.match_fields or {}):
                    add(rel.target_label, k)
            for c in getattr(m, "collections", None) or []:
                add(c.get("owner_label"), c.get("owner_key"))
            if m.mode == "relationship_only":
                add(m.from_label, m.from_key)
                add(m.to_label, m.to_key)
            for qi in m.query_indexes:
                add(m.label, qi)
        return out

    def get(self, table: str) -> TableMapping | None:
        return self.tables.get(table)

    def all_labels(self) -> dict[str, str]:
        """Every label the graph can contain, mapped to its key property."""
        out = dict(self.stub_labels)
        for m in self.tables.values():
            if m.mode == "node" and m.label and m.key:
                out[m.label] = m.key
            elif m.mode == "properties_on" and m.target_label and m.target_key:
                out.setdefault(m.target_label, m.target_key)
            elif m.mode == "relationship_only":
                if m.from_label and m.from_key:
                    out.setdefault(m.from_label, m.from_key)
                if m.to_label and m.to_key:
                    out.setdefault(m.to_label, m.to_key)

        for hub_label, spec in self.hubs.items():
            if spec.get("key"):
                out.setdefault(hub_label, spec["key"])

        for coll_name, spec in self.collections.items():
            if "owner" in spec and "key" in spec["owner"]:
                out.setdefault(coll_name, spec["owner"]["key"])

        for m in self.tables.values():
            for spec in (getattr(m, "promote_property_to_node", None) or {}).values():
                if isinstance(spec, dict) and spec.get("label"):
                    out.setdefault(spec["label"], spec.get("key", "id"))

        for m in self.tables.values():
            for col in getattr(m, "collections", None) or []:
                out.setdefault(col["name"], "uid")
                if col.get("bucket_column"):
                    out.setdefault("LogPeriod", "uid")
        return {k: v for k, v in out.items() if v}


def _substitute_env(node: Any, path: str = "") -> Any:
    if isinstance(node, dict):
        return {k: _substitute_env(v, f"{path}.{k}") for k, v in node.items()}
    if isinstance(node, list):
        return [_substitute_env(v, f"{path}[{i}]") for i, v in enumerate(node)]
    if isinstance(node, str):
        def repl(m: re.Match) -> str:
            var = m.group(1)
            if var not in os.environ:
                raise ConfigError(
                    f"environment variable {var} referenced at {path or '<root>'} is not set"
                )
            return os.environ[var]
        return _ENV_PATTERN.sub(repl, node)
    return node


def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"missing config file: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if data is None:
        raise ConfigError(f"config file is empty: {path}")
    if not isinstance(data, dict):
        raise ConfigError(f"config file must be a mapping at the top level: {path}")
    return data


def load_connections(path: Path) -> dict:
    """Load connections.yaml WITHOUT expanding ${VAR}."""
    return load_yaml(path)


def normalise_property(entry: Any, where: str, source: str = "column") -> PropertySpec:
    """Accept both ``first_name`` and ``{name: x, type: y, scale: n}`` or ``{from: x, as: y}``."""
    if isinstance(entry, str):
        return PropertySpec(name=entry, source=source)
    if not isinstance(entry, dict):
        raise ConfigError(f"{where}: property entry must be a string or a mapping, got {entry!r}")

    if "from" in entry:
        # {from: column, as: name} or {name: name, from: column}: the column is renamed.
        if not (entry.get("as") or entry.get("name")):
            raise ConfigError(f"{where}: renamed property needs 'as' (or 'name'): {entry!r}")
        name = str(entry.get("as") or entry["name"])
        source_column = str(entry["from"])
    else:
        if "name" not in entry:
            raise ConfigError(f"{where}: property entry is missing 'name' or 'as': {entry!r}")
        name = str(entry["name"])
        source_column = None

    unknown = set(entry) - {"name", "type", "scale", "currency_column", "from", "as"}
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) on property {name!r}: {sorted(unknown)}")
    return PropertySpec(
        name=name,
        source_column=source_column,
        type=entry.get("type"),
        scale=entry.get("scale"),
        currency_column=entry.get("currency_column"),
        source=source,
    )


def _normalise_relationship(entry: Any, where: str) -> RelationshipSpec:
    if not isinstance(entry, dict):
        raise ConfigError(f"{where}: relationship entry must be a mapping, got {entry!r}")

    if "to" in entry:
        entry["direction"] = "out"
        entry["target_label"] = entry["to"]
        if "to_key" in entry:
            entry["target_key"] = entry["to_key"]
        elif "from_column" in entry:
            entry["target_key"] = entry["from_column"]
        if "to_key_template" in entry:
            entry["to_key_template"] = entry["to_key_template"]
    elif "from" in entry:
        entry["direction"] = "in"
        entry["target_label"] = entry["from"]
        if "from_key" in entry:
            entry["target_key"] = entry["from_key"]
        elif "from_column" in entry:
            entry["target_key"] = entry["from_column"]

    required = {"type", "target_label"}
    if not entry.get("match_fields"):
        required.add("target_key")
        if not entry.get("resolver"):
            required.add("from_column")

    missing = required - set(entry)
    if missing:
        raise ConfigError(f"{where}: relationship is missing {sorted(missing)}")
    direction = entry.get("direction", "out")
    if direction not in ("out", "in"):
        raise ConfigError(f"{where}: relationship direction must be 'out' or 'in', got {direction!r}")
    return RelationshipSpec(
        type=str(entry["type"]).upper(),        # UPPER_SNAKE_CASE, always
        direction=direction,
        target_label=str(entry["target_label"]),
        target_key=str(entry.get("target_key", "")),
        from_column=str(entry.get("from_column", "")),
        # None = decide later: stub the endpoint when to_key IS that label's key.
        create_target=(None if "create_target" not in entry else bool(entry["create_target"])),
        coerce_to_int=bool(entry.get("coerce_to_int", False)),
        match_fields=entry.get("match_fields"),
        resolver=entry.get("resolver"),
        is_list=bool(entry.get("is_list", False)),
        to_key_template=entry.get("to_key_template"),
        from_key_template=entry.get("from_key_template"),
        acts_as_user=bool(entry.get("acts_as_user", False)),
    )


def _parse_core_table(table: str, spec: dict) -> TableMapping:
    where = f"core.yaml:{table}"
    if not isinstance(spec, dict):
        raise ConfigError(f"{where}: mapping must be a YAML mapping")
    mode = spec.get("mode", "node")
    if mode not in MODES:
        raise ConfigError(f"{where}: mode must be one of {MODES}, got {mode!r}")

    actual_table = spec.get("table", table)
    m = TableMapping(table=actual_table, mode=mode, product=spec.get("product"))
    m.emit_to_graph = bool(spec.get("emit_to_graph", mode != "lookup_source"))
    m.access = spec.get("access") if isinstance(spec.get("access"), dict) else None
    m.readable_by = [str(x) for x in (spec.get("readable_by") or [])]

    if "tenant" in spec:
        m.tenant = spec["tenant"]

    if mode == "lookup_source":
        m.key = spec.get("key", "id")
        m.properties = [
            normalise_property(p, where) for p in spec.get("properties", []) or []
        ]
        return m

    if mode == "node":
        if "merges_into" in spec:
            spec["label"] = spec["merges_into"]
            m.merges_into = True
        for req in ("label", "key"):
            if not spec.get(req):
                raise ConfigError(f"{where}: mode 'node' requires '{req}'")
        m.label = spec["label"]
        m.key = spec["key"]
        m.key_column = spec.get("key_column", spec["key"])
        m.synthesize_key = spec.get("synthesize_key")
        m.properties = [
            normalise_property(p, where) for p in spec.get("properties", []) or []
        ]
        m.custom_field = spec.get("custom_field")
        m.custom_prefix = spec.get("custom_prefix", "custom_")
        m.custom_dynamic = spec.get("custom_dynamic", False)
        for lbl in spec.get("extra_labels", []):
            if isinstance(lbl, dict):
                m.conditional_labels.append(lbl)
            else:
                m.extra_labels.append(str(lbl))
        m.sublabels = spec.get("sublabels", {}) or {}
        # `sublabels: {static: X}` = every row of this table also carries label X (e.g.
        static = m.sublabels.get("static") if isinstance(m.sublabels, dict) else None
        for lbl in ([static] if isinstance(static, str) else (static or [])):
            if lbl not in m.extra_labels:
                m.extra_labels.append(str(lbl))
        m.relationships = [
            _normalise_relationship(r, where) for r in spec.get("relationships", []) or []
        ]
        m.promote_property_to_node = dict(spec.get("promote_property_to_node") or {})
        m.derive_hierarchy = list(spec.get("derive_hierarchy") or [])
        m.on_delete = spec.get("on_delete", "detach")
        if m.on_delete not in NODE_DELETE_MODES:
            raise ConfigError(f"{where}: on_delete for mode 'node' must be one of "
                              f"{NODE_DELETE_MODES}, got {m.on_delete!r}")
        if m.on_delete == "remove_own_properties" and not m.merges_into:
            raise ConfigError(f"{where}: on_delete 'remove_own_properties' is only for "
                              "tables that merge into another table's node (merges_into)")
        m.delete_when = dict(spec.get("delete_when") or {})
        if not isinstance(spec.get("delete_when") or {}, dict):
            raise ConfigError(f"{where}: delete_when must be a mapping of column -> condition")
        m.query_indexes = list(spec.get("query_indexes") or [])
        m.edge_budget = spec.get("edge_budget")
        m.collections = _parse_collections(spec, where)
        m.embed = spec.get("embed")

        if m.key == "uid" and not m.synthesize_key:
            raise ConfigError(f"{where}: key 'uid' requires synthesize_key")

    elif mode == "relationship_only":
        m.on_delete = spec.get("on_delete", "detach")

        if "from" in spec and isinstance(spec["from"], dict):
            spec["from_label"] = spec["from"].get("label")
            spec["from_key"] = spec["from"].get("key")
            spec["from_column"] = spec["from"].get("column")
            spec["from_key_template"] = spec["from"].get("key_template")
            spec["from_create"] = spec["from"].get("create")
        if "to" in spec and isinstance(spec["to"], dict):
            spec["to_label"] = spec["to"].get("label")
            spec["to_key"] = spec["to"].get("key")
            spec["to_column"] = spec["to"].get("column")
            spec["to_key_template"] = spec["to"].get("key_template")
            spec["to_create"] = spec["to"].get("create")

        for req in ("from_label", "from_key", "from_column", "to_label", "to_key", "to_column", "type"):
            if not spec.get(req):
                raise ConfigError(f"{where}: mode 'relationship_only' requires '{req}'")
        m.from_label, m.from_key, m.from_column = spec["from_label"], spec["from_key"], spec["from_column"]
        m.from_create, m.to_create = spec.get("from_create"), spec.get("to_create")
        # properties of the edge itself, e.g. an order line's quantity and price
        m.properties = [normalise_property(p, where) for p in spec.get("properties", []) or []]
        m.collections = _parse_collections(spec, where)
        for c in m.collections:
            c.setdefault("owner_column", m.from_column)   # the collection hangs off the "from" node
        m.to_label, m.to_key, m.to_column = spec["to_label"], spec["to_key"], spec["to_column"]
        m.type = str(spec["type"]).upper()

    elif mode == "properties_on":
        for req in ("target_label", "target_key", "target_key_column", "key_column", "value_column"):
            if not spec.get(req):
                raise ConfigError(f"{where}: mode 'properties_on' requires '{req}'")
        m.target_label = spec["target_label"]
        m.target_key = spec["target_key"]
        m.target_key_column = spec["target_key_column"]
        m.attr_key_column = spec["key_column"]
        m.attr_value_column = spec["value_column"]
        m.prefix = spec.get("prefix")
        m.on_delete = spec.get("on_delete", "remove_property")

    return m


def parse_core(core_doc: dict) -> tuple[dict[str, TableMapping], dict[str, str], dict]:
    tables_doc = core_doc.get("tables") or {}
    if not isinstance(tables_doc, dict):
        raise ConfigError("core.yaml: 'tables' must be a mapping")
    tables = {t: _parse_core_table(t, s) for t, s in tables_doc.items()}

    stubs_doc = core_doc.get("stub_labels") or {}
    if not isinstance(stubs_doc, dict):
        raise ConfigError("core.yaml: 'stub_labels' must be a mapping of Label -> {key: ...}")
    stub_labels: dict[str, str] = {}
    for label, spec in stubs_doc.items():
        if not isinstance(spec, dict) or not spec.get("key"):
            raise ConfigError(f"core.yaml: stub_labels.{label} must declare a 'key'")
        stub_labels[str(label)] = str(spec["key"])

    hubs = dict(core_doc.get("hubs", {}) or {})
    for _t, spec in tables_doc.items():
        for hub in (spec or {}).get("creates_hubs", []) if isinstance(spec, dict) else []:
            # Tenant -[:HAS_HUB]-> <Hub {organization_id}>.
            hubs.setdefault(str(hub), {"key": "organization_id", "owner_edge": "HAS_HUB"})
            stub_labels.setdefault(str(hub), "organization_id")
    meta = {
        "upper_ontology": core_doc.get("upper_ontology", {}),
        "hubs": hubs,
        "collections": core_doc.get("collections", {}),
        "merged_labels": core_doc.get("merged_labels", {}),
        "derived": core_doc.get("derived", {}),
        "sensitivity_rubric": core_doc.get("sensitivity_rubric", {}),
        "properties_not_nodes": core_doc.get("properties_not_nodes", []),
        "graph_integrity": core_doc.get("graph_integrity", {}),
    }

    owners = {}
    for t, spec in tables_doc.items():
        m = tables.get(t)
        if isinstance(spec, dict) and m is not None and m.label:
            for name in spec.get("owns_collections") or []:
                if name in owners and owners[name][2] != t:
                    raise ConfigError(f"collection {name!r} is owned by both {owners[name][2]} and {t}")
                owners[name] = (m.label, m.key, t)
    for m in tables.values():
        for col in getattr(m, "collections", None) or []:
            if col["name"] not in owners:
                raise ConfigError(f"{m.table}: collection {col['name']!r} has no owner; add it to "
                                  "owns_collections on the owning table")
            label, key, owner_table = owners[col["name"]]
            owned = tables[owner_table].owned_collections
            if col["name"] not in [o["name"] for o in owned]:
                owned.append({"name": col["name"], "owner_edge": "HAS"})
            col.setdefault("owner_label", label)
            col.setdefault("owner_key", key)
            # Containment is always HAS: (Contact)-[:HAS]->(TicketHistory)-[:HAS]->(Ticket).
            col.setdefault("owner_edge", "HAS")
            col.setdefault("item_edge", "HAS" if m.mode == "node" else (m.type or "HAS").upper())

    import dataclasses
    creatable = set(stub_labels) | {"LogPeriod"}
    for m in tables.values():
        for spec in (getattr(m, "promote_property_to_node", None) or {}).values():
            if isinstance(spec, dict) and spec.get("label"):
                creatable.add(spec["label"])
    for m in tables.values():
        rels = []
        for rel in getattr(m, "relationships", None) or []:
            if rel.create_target is None:
                # Key-based edges create their target as a stub when it has not arrived
                # yet (its own row fills it in later), so the result never depends on the
                # order events arrive in. Field-matched edges have no key to stub.
                rel = dataclasses.replace(rel, create_target=not rel.match_fields)
            rels.append(rel)
        if hasattr(m, "relationships"):
            m.relationships = rels
        if m.mode == "relationship_only" and getattr(m, "type", None):
            m.type = m.type.upper()
    if not meta.get("graph_integrity"):
        meta["graph_integrity"] = {"default_on_missing": "create",
                                   "create_target_allowed": sorted(creatable)}

    gi = meta["graph_integrity"]
    for m in tables.values():
        m.graph_integrity = gi

    return tables, stub_labels, meta


def merge_overlay(
    core_tables: dict[str, TableMapping],
    stub_labels: dict[str, str],
    core_meta: dict,
    overlay: dict | None,
    database: str,
    mappings_dir: Path,
) -> EffectiveMapping:
    """Merge core with a tenant overlay.  The overlay may only ADD.  Spec 6.3."""
    import copy

    tables = {t: copy.deepcopy(m) for t, m in core_tables.items()}

    if overlay is None:
        return EffectiveMapping(
            database=database,
            organization_id=None,
            products=set(),
            tables=tables,
            stub_labels=dict(stub_labels),
            overlay_present=False,
            **core_meta
        )

    products = set(overlay.get("products") or [])
    org_id = overlay.get("organization_id")

    for table, ext in (overlay.get("extends") or {}).items():
        where = f"{database}.yaml:extends.{table}"
        if table not in tables:
            if table in archived_tables():
                # Deliberately removed from the graph (config/archive): an overlay that still
                # extends it must not stop the tenant from loading.
                log.warning("%s: table was removed from the graph; extension ignored", where)
                continue
            raise ConfigError(f"{where}: table is not declared in core.yaml")
        if not isinstance(ext, dict):
            raise ConfigError(f"{where}: must be a mapping")
        for forbidden in OVERLAY_FORBIDDEN:
            if forbidden in ext:
                raise ConfigError(
                    f"{where}: an overlay may not declare {forbidden!r}; "
                    "identity belongs to core.yaml"
                )
        m = tables[table]
        for v in ext.get("readable_by") or []:
            if str(v) not in m.readable_by:
                m.readable_by.append(str(v))      # an overlay may widen visibility, never narrow it
        if ext.get("embed_fields"):
            if m.mode != "node":
                raise ConfigError(f"{where}: embed_fields is only valid on node tables")
            m.embed = dict(m.embed or {"property": "_embedding"})
            fields = list(m.embed.get("fields") or [])
            for f in ext["embed_fields"]:
                if str(f) not in fields:
                    fields.append(str(f))
            m.embed["fields"] = fields
        for lbl in ext.get("extra_labels") or []:
            if isinstance(lbl, dict):
                m.conditional_labels.append(lbl)
            elif lbl not in m.extra_labels:
                m.extra_labels.append(str(lbl))

        # Parse E2: promotions and hierarchy
        if "promote_property_to_node" in ext:
            m.promote_property_to_node.update(ext["promote_property_to_node"])
        if "derive_hierarchy" in ext:
            m.derive_hierarchy.extend(ext["derive_hierarchy"])
        if "conditional_relationships" in ext:
            m.conditional_relationships.extend(ext["conditional_relationships"])

        for p in ext.get("custom_properties") or []:
            m.custom_properties.append(normalise_property(p, where, source="custom"))
        if "custom_dynamic" in ext:
            m.custom_dynamic = ext["custom_dynamic"]
        if "custom_fields" in ext and isinstance(ext["custom_fields"], dict):
            cf = ext["custom_fields"]
            if cf.get("whitelist"):
                for key in cf["whitelist"]:
                    sk = str(key)
                    if sk not in m.custom_fields_whitelist:
                        m.custom_fields_whitelist.append(sk)
            if cf.get("identity"):
                for key in cf["identity"]:
                    sk = str(key)
                    if sk not in m.custom_fields_whitelist:
                        m.custom_fields_whitelist.append(sk)
            if cf.get("promote_to_node"):
                m.promote_property_to_node.update(cf["promote_to_node"])
            if cf.get("blocklist"):
                for key in cf["blocklist"]:
                    if key not in m.custom_fields_blocklist:
                        m.custom_fields_blocklist.append(str(key))
            if cf.get("blocklist_action"):
                m.custom_fields_blocklist_action = str(cf["blocklist_action"])
        if "stop_dynamic_keys" in ext:
            for key in ext["stop_dynamic_keys"] or []:
                if key not in m.stop_dynamic_keys:
                    m.stop_dynamic_keys.append(str(key))
        for r in ext.get("relationships") or []:
            rel = _normalise_relationship(r, where)
            if any(existing.type == rel.type for existing in m.relationships):
                raise ConfigError(
                    f"{where}: relationship type {rel.type!r} is already defined in core; "
                    "an overlay may add relationships but not redefine one"
                )
            m.relationships.append(rel)

    for table, rel_path in (overlay.get("raw") or {}).items():
        where = f"{database}.yaml:raw.{table}"
        path = (mappings_dir.parent / rel_path) if not str(rel_path).startswith("/") else Path(rel_path)
        if table not in tables:
            tables[table] = TableMapping(table=table, mode="node")
        tables[table].raw_path = str(path)
        try:
            tables[table].raw_cypher = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigError(f"{where}: cannot read raw Cypher file {path}: {exc}") from exc

    return EffectiveMapping(
        database=database,
        organization_id=str(org_id) if org_id is not None else None,
        products=products,
        tables=tables,
        stub_labels=dict(stub_labels),
        overlay_present=True,
        industry=overlay.get("industry"),
        **core_meta
    )


_ARCHIVED: set[str] | None = None


def archived_tables() -> set[str]:
    """Tables removed from the graph on purpose (config/archive/*.yaml)."""
    global _ARCHIVED
    if _ARCHIVED is None:
        import yaml as _yaml
        out: set[str] = set()
        for f in sorted((Path(__file__).resolve().parent.parent / "config" / "archive").glob("dropped_from_graph*.yaml")):
            try:
                out |= set(((_yaml.safe_load(f.read_text(encoding="utf-8")) or {}).get("tables") or {}))
            except Exception:                  # noqa: BLE001 - an unreadable archive only loses the hint
                pass
        _ARCHIVED = out
    return _ARCHIVED


def _snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", str(name)).upper()


def _parse_collections(spec: dict, where: str) -> list[dict]:
    """``collection:`` (one) or ``collections:`` (several) -> list of dicts.

    name            collection label, e.g. DealHistory (one node per owner)
    owner_column    column holding the owner's key (e.g. the contact's cdp_id)
    owner_template  optional key template when the owner key is prefixed, e.g. "service:{queue_id}"
    bucket_column   optional timestamp column: month buckets, for high-volume streams only
    """
    raw = spec.get("collections", spec.get("collection"))
    if raw is None:
        return []
    items = raw if isinstance(raw, list) else [raw]
    out = []
    for c in items:
        if not isinstance(c, dict) or not c.get("name"):
            raise ConfigError(f"{where}: each collection needs a name")
        if spec.get("mode", "node") == "node" and not c.get("owner_column"):
            raise ConfigError(f"{where}: collection {c['name']} needs owner_column")
        out.append(dict(c))
    names = [c["name"] for c in out]
    if len(names) != len(set(names)):
        raise ConfigError(f"{where}: the same collection is listed twice")
    return out


def _mark_stale_cleanup(eff: EffectiveMapping) -> None:
    """Decide per relationship whether the writer may remove edges that no longer match.

    A single-valued FK edge must follow its column: when the column changes or becomes
    null, the old edge goes. This is only safe when nothing else writes the same edge
    type between the same labels, so duplicates are excluded (and reported by validate).
    """
    rel_only = {(m.type.upper() if m.type else "", m.from_label, m.to_label)
                for m in eff.tables.values() if m.mode == "relationship_only"}
    for m in eff.tables.values():
        if m.mode != "node":
            continue
        sigs: dict[tuple, int] = {}
        for rel in m.relationships or []:
            sig = (rel.type.upper(), rel.direction, rel.target_label)
            sigs[sig] = sigs.get(sig, 0) + 1
        flags = {}
        for rel in m.relationships or []:
            sig = (rel.type.upper(), rel.direction, rel.target_label)
            pair = ((rel.type.upper(), m.label, rel.target_label) if rel.direction == "out"
                    else (rel.type.upper(), rel.target_label, m.label))
            flags[rel.payload_key] = sigs[sig] == 1 and pair not in rel_only and not rel.resolver
        m.stale_cleanup = flags


def _finalise(eff: EffectiveMapping, scopes_cfg: dict) -> None:
    """Per-database rules that need every table at once."""
    from .scope import apply_scopes
    primary: dict[str, TableMapping] = {}
    for m in eff.tables.values():
        if m.mode == "node" and m.label and not m.merges_into:
            primary.setdefault(m.label, m)
    for m in eff.tables.values():
        if m.mode == "node" and m.on_delete == "remove_own_properties":
            base = primary.get(m.label)
            base_props = {p.name for p in base.properties} if base else set()
            m.own_properties = sorted(p.name for p in m.properties
                                      if p.name not in base_props and p.name not in (m.key, m.key_column))
    # A table that only adds properties to another table's node (remove_own_properties)
    # must re-queue that node's embedding when it changes one of the node's embed fields:
    # e.g. the conversation memory writes Conversation.summary.
    for m in eff.tables.values():
        if m.mode == "node" and m.on_delete == "remove_own_properties" and not m.embed:
            base = primary.get(m.label)
            if base is not None and base.embed:
                mine = [f for f in (base.embed.get("fields") or []) if f in {p.name for p in m.properties}]
                if mine:
                    m.embed = {"property": base.embed.get("property", "_embedding"), "fields": mine}
    _mark_stale_cleanup(eff)
    apply_scopes(eff, scopes_cfg)


def _norm_industry(value: str) -> str:
    return str(value).lower().strip().replace(" ", "_").replace("-", "_")


def resolve_domain_overlay(mappings_dir: Path, industry_hint: str | None) -> Path | None:
    """Pick a domain pack YAML from industry hint or applies_when rules."""
    if not industry_hint:
        return None
    hint = _norm_industry(industry_hint)
    domains_dir = mappings_dir / "domains"
    if not domains_dir.exists():
        return None
    best: tuple[int, Path] | None = None
    for path in sorted(domains_dir.glob("*.yaml")):
        doc = load_yaml(path)
        score = 0
        pack_ind = _norm_industry(doc.get("industry") or "")
        hint_tokens = set(hint.split("_"))
        if pack_ind and (pack_ind == hint or pack_ind in hint_tokens):
            score += 3
        applies = doc.get("applies_when") or {}
        for _field, values in applies.items():
            if not isinstance(values, list):
                values = [values]
            for raw in values:
                norm = _norm_industry(str(raw))
                # Exact value, or a whole-word match.
                if norm == hint or (("_" not in norm) and norm in hint_tokens):
                    score += 2 if norm == hint else 1
        if score and (best is None or score > best[0]):
            best = (score, path)
    return best[1] if best else None


class ConfigLoader:
    def __init__(self, config_dir: str | Path = "config", overrides: dict = None, domain_overlay: str = None):
        self.config_dir = Path(config_dir)
        self.overrides = overrides or {}
        self.domain_overlay = Path(domain_overlay) if domain_overlay else None
        self.mappings_dir = self.config_dir / "mappings"
        self.connections = load_connections(self.config_dir / "connections.yaml")
        self.core_doc = load_yaml(self.mappings_dir / "core.yaml")
        self.core_tables, self.stub_labels, self.core_meta = parse_core(self.core_doc)
        from .scope import load_scopes_config
        self.scopes_cfg = load_scopes_config(self.config_dir)
        self._cache: dict[str, EffectiveMapping] = {}
        self._section_cache: dict[str, dict] = {}
        self._industry: dict[str, str] = {}

    def set_industry(self, database: str, industry_type: str | None) -> None:
        """Record accounts.organizations.industry_type for a tenant database."""
        if industry_type and self._industry.get(database) != str(industry_type):
            self._industry[database] = str(industry_type)
            self._cache.pop(database, None)

    # -- connection helpers -------------------------------------------------

    def section(self, name: str) -> dict:
        """Return one config section with ${VAR} expanded, cached."""
        if name in self._section_cache:
            return self._section_cache[name]
        try:
            raw = self.connections[name]
        except KeyError as exc:
            raise ConfigError(f"connections.yaml is missing the '{name}' section") from exc
        expanded = _substitute_env(raw, f"connections.yaml.{name}")
        if name in self.overrides:
            if name == "neo4j":
                default = expanded.get("default_instance")
                if default and "instances" in expanded and default in expanded["instances"]:
                    expanded["instances"][default].update(self.overrides[name])
                else:
                    expanded.update(self.overrides[name])
            else:
                expanded.update(self.overrides[name])
        self._section_cache[name] = expanded
        return expanded

    @property
    def prefix(self) -> str:
        return self.section("database_naming")["prefix"]

    def overlay_path(self, database: str) -> Path:
        return self.mappings_dir / f"{database}.yaml"

    def overlay_files(self) -> list[Path]:
        return sorted(
            p for p in self.mappings_dir.glob("*.yaml")
            if p.name not in ("core.yaml", "_TEMPLATE.yaml")
        )

    # -- effective mapping --------------------------------------------------

    _PACK_TO_DOMAIN = {
        "saas_b2b": "saas",
        "saas": "saas",
        "ecommerce": "ecommerce",
        "healthcare": "healthcare",
        "health_care": "healthcare",
        "automotive": "automotive",
        "bfsi": "bfsi",
        "legal": "legal",
        "education": "education",
        "real_estate": "real-estate",
    }

    def _resolve_industry_hint(self, database: str, tenant_doc: dict | None) -> str | None:
        if tenant_doc and tenant_doc.get("industry"):
            return str(tenant_doc["industry"])
        if database in self._industry:
            return self._industry[database]
        org_id = (tenant_doc or {}).get("organization_id") or database.replace("org", "")
        sem_path = self.config_dir.parent.parent / "tenants" / f"org{org_id}" / "semantic.yaml"
        if sem_path.exists():
            sem = load_yaml(sem_path)
            pack = sem.get("industryPack") or sem.get("industry_pack")
            if pack:
                return self._PACK_TO_DOMAIN.get(str(pack).lower().replace("-", "_"), str(pack))
        return None

    def clear_cache(self, database: str | None = None) -> None:
        if database:
            self._cache.pop(database, None)
        else:
            self._cache.clear()

    def effective(self, database: str) -> EffectiveMapping:
        """Merged core+overlay mapping for one database.  Computed once, cached."""
        if database in self._cache:
            return self._cache[database]

        import copy

        tables = {t: copy.deepcopy(m) for t, m in self.core_tables.items()}
        domain_industry: str | None = None

        tenant_doc: dict | None = None
        tenant_path = self.overlay_path(database)
        if tenant_path.exists():
            tenant_doc = load_yaml(tenant_path)

        industry_hint = self._resolve_industry_hint(database, tenant_doc)
        domain_path = self.domain_overlay
        if (not domain_path or not domain_path.exists()) and industry_hint:
            domain_path = resolve_domain_overlay(self.mappings_dir, industry_hint)

        eff: EffectiveMapping | None = None
        if domain_path and domain_path.exists():
            domain_doc = load_yaml(domain_path)
            domain_industry = domain_doc.get("industry")
            eff = merge_overlay(tables, self.stub_labels, self.core_meta, domain_doc, database, self.mappings_dir)
            tables = eff.tables

        core_prods = {m.product for m in tables.values() if m.product}

        if tenant_doc:
            eff = merge_overlay(tables, self.stub_labels, self.core_meta, tenant_doc, database, self.mappings_dir)
            if not eff.products and "products" not in tenant_doc:
                eff.products = set(core_prods)
            if eff.industry is None and domain_industry:
                eff.industry = domain_industry
        elif eff is not None:
            if not eff.products:
                eff.products = set(core_prods)
        else:
            eff = EffectiveMapping(
                database=database,
                organization_id=database.replace("org", "") if database.startswith("org") else None,
                products=set(core_prods),
                tables=tables,
                stub_labels=dict(self.stub_labels),
                overlay_present=False,
                **self.core_meta,
            )

        self._apply_steward_stop_keys(eff, database)
        _finalise(eff, self.scopes_cfg)
        self._cache[database] = eff
        return eff

    def _apply_steward_stop_keys(self, eff: EffectiveMapping, database: str) -> None:
        """Merge steward registry stop keys for this org's tables."""
        try:
            from ontology_steward.tools.registry_io import load_stop_keys, REGISTRY_DIR
            if not REGISTRY_DIR.exists():
                return
            org = eff.organization_id or database.replace("org", "")
            global_keys = load_stop_keys()
            if not global_keys and not load_stop_keys(org):
                return
            doc_path = REGISTRY_DIR / "stop_keys.yml"
            import yaml
            doc = yaml.safe_load(doc_path.read_text(encoding="utf-8")) or {}
            by_org = doc.get("by_org") or {}
            for entry in by_org.get(str(org), []) or []:
                if not isinstance(entry, dict):
                    continue
                table = entry.get("table")
                key = entry.get("key")
                if table and key and table in eff.tables:
                    if key not in eff.tables[table].stop_dynamic_keys:
                        eff.tables[table].stop_dynamic_keys.append(key)
            for key in global_keys:
                for m in eff.tables.values():
                    if m.custom_dynamic and key not in m.stop_dynamic_keys:
                        m.stop_dynamic_keys.append(key)
        except ImportError:
            pass

    def get_mapping(self, database: str, table: str) -> TableMapping | None:
        return self.effective(database).get(table)
