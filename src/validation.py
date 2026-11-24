"""Startup validation.  Spec section 6.4 -- fail loudly, never silently."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from .config_loader import load_yaml  # noqa: E402
from .config_loader import (
    MODES,
    PROPERTY_TYPES,
    ConfigLoader,
    normalise_property,
)
from .errors import ConfigError
from .naming import MAX_LEN, MIN_LEN

INT64_MAX = 2**63 - 1


class ValidationReport:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    @property
    def ok(self) -> bool:
        return not self.errors

    def raise_if_failed(self) -> None:
        if self.errors:
            raise ConfigError(
                "configuration validation failed with "
                f"{len(self.errors)} error(s):\n  - " + "\n  - ".join(self.errors)
            )


def _check_property_types(props, where: str, report: ValidationReport) -> None:
    for p in props:
        # check 3 -- declared type must be known
        if p.type is not None and p.type not in PROPERTY_TYPES:
            report.error(
                f"{where}: property {p.name!r} has type {p.type!r}; "
                f"allowed types are {sorted(PROPERTY_TYPES)}"
            )
        # check 11 -- money must declare its scale
        if p.type == "decimal":
            if p.scale is None:
                report.error(
                    f"{where}: property {p.name!r} is decimal but declares no 'scale'. "
                    "NUMERIC without a scale modifier has arbitrary scale; an explicit "
                    "scale is required so the value can be stored as integer minor units."
                )
            elif not isinstance(p.scale, int) or isinstance(p.scale, bool) or p.scale < 0:
                report.error(
                    f"{where}: property {p.name!r} has scale {p.scale!r}; "
                    "scale must be a non-negative integer"
                )
            else:
                # check 12 -- would a plausible value overflow int64 at this scale?
                if p.scale > 18:
                    report.error(
                        f"{where}: property {p.name!r} declares scale {p.scale}; "
                        f"minor units at that scale exceed the signed 64-bit range "
                        f"({INT64_MAX}) for any value >= 10. Store as string instead."
                    )
                elif p.scale >= 8:
                    report.warn(
                        f"{where}: property {p.name!r} has scale {p.scale}; the largest "
                        f"representable value is {INT64_MAX // (10 ** p.scale):,}. "
                        "Confirm this exceeds any real value in this column."
                    )
        elif p.scale is not None:
            report.error(
                f"{where}: property {p.name!r} declares 'scale' but its type is "
                f"{p.type!r}; scale is only meaningful for decimal"
            )


def validate_all(loader: ConfigLoader) -> ValidationReport:
    report = ValidationReport()
    core_tables = loader.core_tables
    stub_labels = loader.stub_labels

    core_labels: dict[str, str] = {}
    for table, m in core_tables.items():
        where = f"core.yaml:{table}"

        # check 6 -- mode is one of the three
        if m.mode not in MODES:
            report.error(f"{where}: unknown mode {m.mode!r}")
            continue

        if m.mode == "node":
            if m.label in core_labels and core_labels[m.label] != m.key:
                if m.label not in loader.core_meta.get("merged_labels", {}):
                    report.error(
                        f"{where}: label {m.label!r} is already keyed on "
                        f"{core_labels[m.label]!r} elsewhere but here on {m.key!r}; "
                        "one label must have exactly one key property unless it is a merged_label"
                    )
            core_labels[m.label] = m.key
            _check_property_types(m.properties, where, report)

        # check 7 -- properties_on must carry a collision-guard prefix
        if m.mode == "properties_on" and not (m.prefix or "").strip():
            report.error(
                f"{where}: mode 'properties_on' requires a non-empty 'prefix'. "
                "Without it an attribute named 'status' overwrites the real column."
            )

    # check 13 / 14 -- stub labels
    for label, key in stub_labels.items():
        if not key:
            report.error(f"core.yaml:stub_labels.{label}: missing 'key'")
        if label in core_labels and core_labels[label] != key:
            report.error(
                f"core.yaml:stub_labels.{label}: key {key!r} disagrees with the node "
                f"mapping's key {core_labels[label]!r} for the same label"
            )

    known_labels = {**stub_labels, **core_labels}
    for hub in loader.core_meta.get("hubs", {}): known_labels[hub] = loader.core_meta["hubs"][hub].get("key")
    for coll in loader.core_meta.get("collections", {}):
        owner = loader.core_meta["collections"][coll].get("owner", {})
        known_labels[coll] = owner.get("key")

    # check 5 -- relationship targets resolve, from core labels OR stub labels
    for table, m in core_tables.items():
        for rel in m.relationships:
            if rel.target_label not in known_labels:
                report.error(
                    f"core.yaml:{table}: relationship {rel.type!r} targets label "
                    f"{rel.target_label!r}, which is neither a core node label nor a "
                    "declared stub_label. Add it to stub_labels so a uniqueness "
                    "constraint is created for it."
                )
            elif rel.match_fields:
                pass  # target_key isn't used for match_fields lookups
            elif rel.create_target is False:
                pass  # match-only by an indexed secondary key (e.g. Contact.campaign_contact_id)
            elif known_labels[rel.target_label] != rel.target_key:
                if rel.target_label not in loader.core_meta.get("merged_labels", {}):
                    report.error(
                        f"core.yaml:{table}: relationship {rel.type!r} uses target_key "
                        f"{rel.target_key!r} but label {rel.target_label!r} is keyed on "
                        f"{known_labels[rel.target_label]!r}"
                    )
        if m.mode == "relationship_only":
            for lbl, k, create in ((m.from_label, m.from_key, getattr(m, "from_create", None)),
                                   (m.to_label, m.to_key, getattr(m, "to_create", None))):
                if create is False and lbl in known_labels:
                    continue  # match-only by an indexed secondary key: never creates the node
                if lbl not in known_labels:
                    report.error(
                        f"core.yaml:{table}: label {lbl!r} is not a core node label or "
                        "a declared stub_label"
                    )
                elif known_labels[lbl] is not None and known_labels[lbl] != k:
                    if lbl not in loader.core_meta.get("merged_labels", {}):
                        report.error(
                            f"core.yaml:{table}: label {lbl!r} keyed {k!r} here but "
                            f"{known_labels[lbl]!r} elsewhere"
                        )
        if m.mode == "properties_on" and m.target_label not in known_labels:
            report.error(
                f"core.yaml:{table}: target_label {m.target_label!r} is not defined"
            )

    declared_products: set[str] = set()

    for path in loader.overlay_files():
        db_from_name = path.stem
        where = path.name

        # check 16 -- filename must itself be a legal database name
        if not re.fullmatch(r"[a-z][a-z0-9.\-]*", db_from_name) or not (
            MIN_LEN <= len(db_from_name) <= MAX_LEN
        ):
            report.error(
                f"{where}: filename {db_from_name!r} is not a legal Neo4j database name "
                "(lowercase, starts with a letter, 3-63 chars, no underscores)"
            )

        try:
            with path.open("r", encoding="utf-8") as fh:
                doc = yaml.safe_load(fh) or {}
        except yaml.YAMLError as exc:
            report.error(f"{where}: YAML parse error: {exc}")
            continue
        if not isinstance(doc, dict):
            report.error(f"{where}: top level must be a mapping")
            continue

        # check 9 -- filename matches the database field inside
        if doc.get("database") != db_from_name:
            report.error(
                f"{where}: 'database' field is {doc.get('database')!r} but the filename "
                f"implies {db_from_name!r}; these must match"
            )

        # check 10 -- organisation id must be a quoted string
        org_id = doc.get("organization_id")
        if org_id is None:
            report.error(f"{where}: missing 'organization_id'")
        elif not isinstance(org_id, str):
            report.error(
                f"{where}: organization_id parsed as {type(org_id).__name__} "
                f"({org_id!r}). Quote it. Unquoted YAML coerces 12345 to an integer and "
                "on/no/yes to booleans, which silently breaks string lookups."
            )

        products = doc.get("products") or []
        if not isinstance(products, list):
            report.error(f"{where}: 'products' must be a list")
        else:
            for p in products:
                if not isinstance(p, str):
                    report.error(f"{where}: product {p!r} must be a quoted string")
            declared_products.update(str(p) for p in products)

        for table, ext in (doc.get("extends") or {}).items():
            ewhere = f"{where}:extends.{table}"

            # check 1 -- extends key exists in core
            if table not in core_tables:
                report.error(f"{ewhere}: table is not declared in core.yaml")
                continue
            if not isinstance(ext, dict):
                report.error(f"{ewhere}: must be a mapping")
                continue

            # check 2 -- no identity fields in an overlay
            for forbidden in ("key", "label", "mode"):
                if forbidden in ext:
                    report.error(
                        f"{ewhere}: declares {forbidden!r}. An overlay may only add; "
                        "identity belongs to core.yaml."
                    )

            m = core_tables[table]
            core_prop_names = {p.name for p in m.properties}
            prefix = m.custom_prefix

            custom = []
            for entry in ext.get("custom_properties") or []:
                try:
                    spec = normalise_property(entry, ewhere, source="custom")
                except ConfigError as exc:
                    report.error(str(exc))
                    continue
                custom.append(spec)
                # check 3 -- name and type present
                if spec.type is None:
                    report.error(
                        f"{ewhere}: custom property {spec.name!r} must declare a 'type'"
                    )
                # check 4 -- no collision with a core property after prefixing
                if f"{prefix}{spec.name}" in core_prop_names:
                    report.error(
                        f"{ewhere}: custom property {spec.name!r} collides with core "
                        f"property {prefix}{spec.name!r} after prefixing"
                    )
                if spec.name in core_prop_names and not prefix:
                    report.error(
                        f"{ewhere}: custom property {spec.name!r} collides with a core "
                        "property and no prefix is configured"
                    )
            _check_property_types(custom, ewhere, report)

            seen_custom: set[str] = set()
            for spec in custom:
                if spec.name in seen_custom:
                    report.error(f"{ewhere}: custom property {spec.name!r} declared twice")
                seen_custom.add(spec.name)

            for rel in ext.get("relationships") or []:
                if not isinstance(rel, dict):
                    report.error(f"{ewhere}: relationship entry must be a mapping")
                    continue
                tl = rel.get("target_label")
                # check 5 again, for overlay relationships
                if tl not in known_labels:
                    report.error(
                        f"{ewhere}: relationship targets label {tl!r}, which is neither "
                        "a core node label nor a declared stub_label"
                    )
                # overlay may not redefine a core relationship's type
                if any(r.type == rel.get("type") for r in m.relationships):
                    report.error(
                        f"{ewhere}: relationship type {rel.get('type')!r} is already "
                        "defined in core.yaml"
                    )

        # check 8 -- raw files exist and contain $batch
        for table, rel_path in (doc.get("raw") or {}).items():
            rwhere = f"{where}:raw.{table}"
            p = Path(rel_path)
            if not p.is_absolute():
                p = loader.config_dir / rel_path
            if not p.exists():
                report.error(f"{rwhere}: raw Cypher file not found: {p}")
                continue
            text = p.read_text(encoding="utf-8")
            if "$batch" not in text:
                report.error(
                    f"{rwhere}: raw Cypher must parameterise the batch as $batch "
                    "and use 'UNWIND $batch AS e'"
                )

    # check 15 -- a product gate nobody has purchased is probably a typo
    core_products = {m.product for m in core_tables.values() if m.product}
    for prod in sorted(core_products - declared_products):
        report.warn(
            f"core.yaml: product {prod!r} gates one or more mappings but is not listed "
            "in any overlay's 'products'. Those mappings will be skipped for every "
            "organisation."
        )

    _check_ontology_rules(loader, report)
    return report


_PASCAL = re.compile(r"^[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]*)*$|^[A-Z]{2,4}$")
_SYNONYM_KEYS = ("naming", "role_naming", "naming_overrides", "extend_synonyms", "synonyms", "aliases")


def _find_keys(node, keys, path=""):
    if isinstance(node, dict):
        for k, v in node.items():
            here = f"{path}.{k}" if path else str(k)
            if k in keys:
                yield here
            yield from _find_keys(v, keys, here)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from _find_keys(v, keys, f"{path}[{i}]")


def _check_ontology_rules(loader, report) -> None:
    core_labels = {m.label for m in loader.core_tables.values() if m.mode == "node" and m.label}
    core_state = {}
    for t, m in loader.core_tables.items():
        for cl in getattr(m, "conditional_labels", None) or []:
            core_state.setdefault(cl["label"], t)
        for lbl in list(getattr(m, "extra_labels", None) or []) + list(core_labels):
            if not _PASCAL.match(lbl):
                report.warn(f"core.yaml:{t}: label {lbl!r} is not simple PascalCase English")

    domains = loader.mappings_dir / "domains"
    for path in sorted(domains.glob("*.yaml")) if domains.exists() else []:
        where = f"domains/{path.name}"
        doc = load_yaml(path) or {}
        for hit in _find_keys(doc, _SYNONYM_KEYS):
            report.error(f"{where}: {hit} -- industry packs may not rename core concepts "
                         "(no synonyms); one label per concept in every industry")
        for table, ext in (doc.get("extends") or {}).items():
            loader.core_tables.get(table)
            for lbl in ext.get("extra_labels") or []:
                name = lbl.get("label") if isinstance(lbl, dict) else lbl
                if not isinstance(lbl, dict) or not lbl.get("when"):
                    report.error(f"{where}:extends.{table}: label {name!r} has no `when:` -- an "
                                 "unconditional industry label is a synonym of the core label")
                    continue
                if name in core_labels:
                    report.error(f"{where}:extends.{table}: {name!r} is a core base label; "
                                 "reusing it on another table merges two different concepts")
                owner = core_state.get(name)
                if owner and owner != table:
                    report.error(f"{where}:extends.{table}: {name!r} is a core state label of {owner}")
                if not _PASCAL.match(str(name)):
                    report.warn(f"{where}:extends.{table}: label {name!r} is not simple PascalCase English")
