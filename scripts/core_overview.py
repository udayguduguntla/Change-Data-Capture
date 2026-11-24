"""Write docs/CORE_OVERVIEW.md: every mapped table, the node it writes, its key, edges and
collections, grouped by product; then collection owners and the tables left out.

    python scripts/core_overview.py
"""
import collections
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from src.config_loader import ConfigLoader, archived_tables  # noqa: E402

PURPOSE = ("The knowledge graph is for reports, analytics and investigation across products "
           "(the hub looking over every product's data). It keeps the business entities and "
           "how they connect; configuration, internal logs, copies of another product's data "
           "and high-volume metrics stay out.")


def main():
    loader = ConfigLoader(ROOT / "config")
    e = loader.effective("org1000")
    by = collections.defaultdict(list)
    for t, m in sorted(e.tables.items()):
        by[m.product_label or "(no product label)"].append((t, m))
    out = ["# Core graph overview", "", PURPOSE, "",
           f"**{len(e.tables)} tables mapped · {len(e.label_scopes)} node labels · "
           f"{len(archived_tables())} tables left out** (`config/archive/dropped_from_graph_v45.yaml`)", "",
           "Reading rule: a node is readable by an agent when it has no product label, or one of the "
           "agent's products. `HAS` = containment; verbs = business meaning.", ""]
    for p in ["CDP", "ContactCentre", "Support", "Sales", "Marketing", "WebTracking", "(no product label)"]:
        items = by.get(p, [])
        if not items:
            continue
        out += [f"## {p}", "", "| Table | Node | Key | Edges | Collection |", "|---|---|---|---|---|"]
        for t, m in items:
            if m.mode == "relationship_only":
                node, key = "*edge only*", "—"
                edges = f"({m.from_label})-[:{m.type}]->({m.to_label})"
            else:
                node = f"`{m.label}`" + (" *(adds properties)*"
                                         if m.merges_into and m.on_delete == "remove_own_properties" else "")
                key = f"`{m.key}`" + (f" = `{m.synthesize_key}`" if m.synthesize_key else "")
                es = []
                for r in m.relationships or []:
                    role = " *(role of a User)*" if r.acts_as_user else ""
                    es.append(f"-[:{r.type.upper()}]->{r.target_label}{role}" if r.direction == "out"
                              else f"<-[:{r.type.upper()}]-{r.target_label}")
                edges = "<br>".join(es) or "—"
            renamed = [f"{p.source_column}→{p.name}" for p in m.properties if p.source_column]
            if renamed:
                node += f"<br>*({', '.join(renamed)})*"
            cols = "<br>".join(f"{c['owner_label']} → **{c['name']}**" + (" (by month)" if c.get("bucket_column") else "")
                               for c in (m.collections or [])) or "—"
            out.append(f"| `{t}` | {node} | {key} | {edges} | {cols} |")
        out.append("")
    owned = collections.defaultdict(list)
    for m in e.tables.values():
        for o in m.owned_collections or []:
            owned[m.label].append(o["name"])
    out += ["## Collection owners", ""] + [f"- **{k}**: {', '.join(v)}" for k, v in sorted(owned.items())] + [""]
    out += ["## Embedded (searchable by meaning)", ""]
    emb = collections.defaultdict(set)
    for m in e.tables.values():
        if m.embed and m.label:
            emb[m.label] |= set(m.embed.get("fields") or [])
    out += [f"- `{k}`: {', '.join(sorted(v))}" for k, v in sorted(emb.items())] + [""]
    derived = (getattr(loader, "core_meta", {}) or {}).get("derived") or {}
    if derived:
        out += ["## Computed and looked-up columns", ""]
        for k, v in derived.items():
            if v.get("function"):
                out.append(f"- `{k}` = `{v['function']}({v.get('from')})`")
            elif v.get("lookup"):
                lk = v["lookup"]
                scope = f", same `{lk['scope']}`" if lk.get("scope") else ""
                out.append(f"- `{k}` = `{lk['table']}.{lk['select']}` where `{lk['match_remote']}` = row `{lk['match_local']}`{scope}")
        out.append("")
    arch = collections.defaultdict(list)
    for t in sorted(archived_tables()):
        arch[t.split(".")[0]].append(t.split(".", 1)[1])
    out += ["## Left out of the graph", ""] + [f"- **{s}** ({len(v)}): {', '.join(v)}" for s, v in sorted(arch.items())]
    (ROOT / "docs" / "CORE_OVERVIEW.md").write_text("\n".join(out) + "\n", encoding="utf-8")
    print("wrote docs/CORE_OVERVIEW.md")


if __name__ == "__main__":
    main()
