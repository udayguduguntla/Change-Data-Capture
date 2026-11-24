#!/usr/bin/env python3
"""Build config/catalog/column_descriptions.json from database_schemas/."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SOURCES = {
    "accounts_table_descriptions.md": "accounts",
    "CAMPAIGN_DATABASE_TABLES_REFERENCE.md": "campaign",
    "ccass-schema.md": "ccass",
    "CCASS_SCHEMAS.md": "ccass",
    "CDP_CUSTOMER360_SCHEMA_DESCRIPTION.txt": "cdp",
    "crm-tables-reference.md": "crm",
    "ticketing-table-catalog.md": "ticketing",
}
PURPOSE = re.compile(r"^\*\*(?:Purpose|What this table is for):\*\*\s*(.+)$|^Purpose:\s*(.+)$")
MD_HEAD = re.compile(r"^#{2,4}\s+(.*)$")
TXT_HEAD = re.compile(r"^\d+\.\s+([a-z_]+\.[a-z_0-9]+)\s*$")
MD_ROW = re.compile(r"^\|\s*`([a-z_][a-z_0-9]*)`\s*\|(.*)\|\s*$")
TXT_ROW = re.compile(r"^-\s+([a-z_][a-z_0-9]*)\s+[A-Z][A-Z0-9()_, ]*[^:]*:\s*(.+)$")


def clean(text: str) -> str:
    text = re.sub(r"\*\*|`", "", text)
    return re.sub(r"\s+", " ", text).strip()


def table_from_heading(head: str, schema: str) -> str | None:
    m = re.search(r"`([a-z_]+\.[a-z_0-9]+)`", head)
    if m:
        return m.group(1)
    m = re.search(r"`([a-z_][a-z_0-9]+)`", head) or re.fullmatch(r"(?:\d+\.\s*)?([a-z_][a-z_0-9]+)\s*", head)
    return f"{schema}.{m.group(1)}" if m else None


def parse(path: Path, schema: str, out: dict) -> None:
    table, pending_intro = None, False
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        head = MD_HEAD.match(line) or TXT_HEAD.match(line)
        if head:
            t = head.group(1) if TXT_HEAD.match(line) else table_from_heading(head.group(1), schema)
            if t:
                table, pending_intro = t, True
                out.setdefault(table, {"description": "", "columns": {}})
            elif MD_HEAD.match(line) and line.startswith("## "):
                table = None
            continue
        if not table or not line:
            continue
        entry = out[table]
        p = PURPOSE.match(line)
        if p:
            entry["description"] = clean(p.group(1) or p.group(2))[:400]
            pending_intro = False
            continue
        row = MD_ROW.match(line) or TXT_ROW.match(line)
        if row:
            col = row.group(1)
            if MD_ROW.match(line):
                cells = [clean(c) for c in row.group(2).split("|")]
                desc = next((c for c in reversed(cells) if c and c not in ("—", "-")), "")
            else:
                desc = clean(row.group(2))
            if desc and col not in entry["columns"]:
                entry["columns"][col] = desc[:300]
            pending_intro = False
            continue
        if pending_intro and not line.startswith(("|", "-", ">", "```")) and not entry["description"]:
            entry["description"] = clean(line)[:400]
            pending_intro = False


def main() -> int:
    out: dict = {}
    for name, schema in SOURCES.items():
        path = ROOT / "database_schemas" / name
        if path.exists():
            parse(path, schema, out)
    out = {t: v for t, v in out.items() if v["columns"] or v["description"]}

    from src.config_loader import ConfigLoader
    loader = ConfigLoader(ROOT / "config")
    covered = total = 0
    missing_tables = []
    for table, m in loader.core_tables.items():
        doc = out.get(table)
        if not doc:
            missing_tables.append(table)
            continue
        for p in m.properties:
            total += 1
            covered += bool(doc["columns"].get(p.source_column or p.name))
    dest = ROOT / "config" / "catalog" / "column_descriptions.json"
    dest.write_text(json.dumps(dict(sorted(out.items())), indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"{len(out)} tables described -> {dest.relative_to(ROOT)}")
    print(f"core coverage: {len(loader.core_tables) - len(missing_tables)}/{len(loader.core_tables)} tables, "
          f"{covered}/{total} properties")
    if missing_tables:
        print("tables with no description:", ", ".join(sorted(missing_tables)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
