"""``python main.py verify --database org1012``: check a live tenant graph (read-only).

1. Label counts per product, and nodes with a wrong product label.
2. The product questions (scripts/reasoning_check.py): for each path, does the real data have
   at least one? "no data yet" is not an error (the tenant may not use that feature).
3. Data checks that expose id-space mismatches the design cannot see.

Writes logs/verify_<database>.md and prints a summary.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DATA_CHECKS = [
    ("Conversations not attached to a contact (contact_id id-space check)",
     "MATCH (c:Conversation) WHERE NOT (c)<-[:HAS]-(:ConversationHistory) RETURN count(c) AS n",
     "MATCH (c:Conversation) RETURN count(c) AS n"),
    ("Campaign deliveries that found no contact (campaign_contact_id check)",
     "MATCH (d:Delivery) WHERE NOT (d)<-[:HAS]-(:LogPeriod) RETURN count(d) AS n",
     "MATCH (d:Delivery) RETURN count(d) AS n"),
    ("Chat/email conversations without a channel",
     "MATCH (c:Conversation) WHERE (c:ChatConversation OR c:EmailConversation) "
     "AND NOT (c)-[:VIA_CHANNEL]->() RETURN count(c) AS n",
     "MATCH (c:Conversation) WHERE c:ChatConversation OR c:EmailConversation RETURN count(c) AS n"),
    ("Products created as stubs (order-line sku or line item not found in inventory)",
     "MATCH (p:Product) WHERE p._stub_since IS NOT NULL RETURN count(p) AS n",
     "MATCH (p:Product) RETURN count(p) AS n"),
    ("Tickets without a company link (B2C tenants: expected)",
     "MATCH (t:Ticket) WHERE NOT (t)-[:FOR_COMPANY]->() RETURN count(t) AS n",
     "MATCH (t:Ticket) RETURN count(t) AS n"),
]


def _reasoning():
    spec = importlib.util.spec_from_file_location("reasoning_check", ROOT / "scripts" / "reasoning_check.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _path_cypher(path) -> str:
    q = "MATCH "
    for i, (a, t, b) in enumerate(path):
        if i == 0:
            q += f"(:`{a}`)"
        q += f"-[:`{t}`]->(:`{b}`)"
    return q + " WITH 1 AS x LIMIT 1 RETURN count(x) AS n"


def verify(session, eff) -> tuple[list[str], dict]:
    products = list(eff.all_scopes)
    unscoped = list(eff.unscoped_labels)
    out, summary = [], {"wrong_product_labels": 0, "questions_with_data": 0, "questions": 0}

    out += ["## Nodes per product", "", "| Product | Nodes |", "|---|---|"]
    for p in products:
        n = session.run(f"MATCH (n:`{p}`) RETURN count(n) AS n").single()["n"]
        out.append(f"| {p} | {n:,} |")
    n_un = session.run("MATCH (n) WHERE any(l IN labels(n) WHERE l IN $u) RETURN count(n) AS n",
                       u=unscoped).single()["n"]
    out.append(f"| (setup/catalog, no product label) | {n_un:,} |")
    bad = session.run(
        "MATCH (n) WITH n, [l IN labels(n) WHERE l IN $p] AS pl, any(l IN labels(n) WHERE l IN $u) AS free "
        "WHERE (free AND size(pl) > 0) OR (NOT free AND size(pl) <> 1) "
        "RETURN labels(n) AS labels, count(*) AS n ORDER BY n DESC LIMIT 10", p=products, u=unscoped)
    bad = list(bad)
    summary["wrong_product_labels"] = sum(r["n"] for r in bad)
    out += ["", f"Nodes with a wrong product label: **{summary['wrong_product_labels']}**"]
    out += [f"- {r['labels']}: {r['n']}" for r in bad]

    rc = _reasoning()
    out += ["", "## Product questions on real data", "",
            "| Agent | Question | Paths with data | Design |", "|---|---|---|---|"]
    for agent, prods, question, paths, expect in rc.QUESTIONS:
        design, _ = rc.check(eff, prods, paths)
        have = sum(1 for p in paths if session.run(_path_cypher(p)).single()["n"])
        summary["questions"] += 1
        summary["questions_with_data"] += 1 if have == len(paths) else 0
        out.append(f"| {agent} | {question} | {have}/{len(paths)}"
                   + ("" if have == len(paths) else " (no data yet)") + f" | {design} |")

    out += ["", "## Data checks", "", "| Check | Count | Of |", "|---|---|---|"]
    for name, q, total in DATA_CHECKS:
        n = session.run(q).single()["n"]
        t = session.run(total).single()["n"]
        out.append(f"| {name} | {n:,} | {t:,} |")
        summary[name] = (n, t)
    return out, summary
