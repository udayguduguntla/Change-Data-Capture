import json
import datetime
from pathlib import Path
from src import graph_state

def build_manifest(loader, pool, database: str) -> dict:
    eff = loader.effective(database)
    state = graph_state.get_state(pool, database)
    
    catalog_path = Path(loader.config_dir) / "catalog" / "column_descriptions.json"
    catalog = {}
    if catalog_path.exists():
        with open(catalog_path, "r", encoding="utf-8") as f:
            catalog = json.load(f)

    labels = {}
    edges = []
    collections = []
    money = {}
    notes = {}

    for table_name, m in eff.tables.items():
        if m.mode == "node" and m.label:
            table_cat = catalog.get(table_name, {})
            
            # State labels
            state_labels = [str(l) for l in getattr(m, "extra_labels", [])]
            for cl in getattr(m, "conditional_labels", []):
                state_labels.append(cl["label"])
            
            # Properties
            props = {}
            for p in m.properties:
                desc = table_cat.get("columns", {}).get(p.source_column or p.name, "")
                props[p.name] = {"type": p.type, "meaning": desc}
                if p.type == "decimal" and getattr(p, "scale", None):
                    money[f"{m.label}.{p.name}"] = {"scale": p.scale}
            
            labels[m.label] = {
                "product": getattr(m, "product_label", None) or getattr(m, "product", ""),
                "key": m.key,
                "description": table_cat.get("description", ""),
                "state_labels": {"dynamic": state_labels} if state_labels else {},
                "properties": props,
                "embedded": bool(getattr(m, "embed", None))
            }
            if getattr(m, "note", None):
                notes[m.label] = m.note
                
            for c in getattr(m, "collections", []):
                collections.append({
                    "owner": c.get("owner_label"),
                    "collection": c.get("name"),
                    "bucketed": bool(c.get("bucket_column")),
                    "item": m.label
                })

        # Relationships
        for r in getattr(m, "relationships", []):
            edges.append({
                "from": m.label,
                "type": r.type.upper(),
                "to": r.target_label,
                "fan_in": "high" if getattr(m, "edge_budget", None) else "low"
            })
            if getattr(r, "note", None):
                notes[r.type.upper()] = r.note

        if m.mode == "relationship_only":
            edges.append({
                "from": m.from_label,
                "type": getattr(m, "type", "").upper(),
                "to": m.to_label,
                "fan_in": "low"
            })
            if getattr(m, "note", None):
                notes[getattr(m, "type", "").upper()] = m.note

    return {
        "org": database,
        "schema_fingerprint": state.get("schema_fingerprint", ""),
        "data_watermark": state.get("data_watermark", ""),
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "labels": labels,
        "edges": edges,
        "collections": collections,
        "money": money,
        "notes": notes
    }
