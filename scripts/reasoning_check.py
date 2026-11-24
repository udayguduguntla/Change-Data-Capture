"""Product questions as graph paths, checked against core.yaml.

Each question names the agent asking (its products) and the hops it walks. For every hop the
check confirms the edge exists in that direction, and that the agent may read every node on
the path (no product label, or one of its products). Questions marked `blocked` must be
refused by scope: that is the product boundary working.

    python scripts/reasoning_check.py        # writes docs/ONTOLOGY_REASONING.md
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SUPPORT, CC, SALES, MKT, CDP, WEB = "Support", "ContactCentre", "Sales", "Marketing", "CDP", "WebTracking"
HUB = [SUPPORT, CC, SALES, MKT, CDP, WEB]

# (agent, products, question, [path, path...], expectation)   path = [(from, TYPE, to), ...]
QUESTIONS = [
    # -- Support ---------------------------------------------------------------------------
    ("Support", [SUPPORT, CDP], "Which tickets has this customer raised?",
     [[("Contact", "HAS", "TicketHistory"), ("TicketHistory", "HAS", "Ticket")]], "ok"),
    ("Support", [SUPPORT, CDP], "Who is working on this ticket, and who resolved it?",
     [[("Ticket", "ASSIGNED_TO", "SupportAgent")], [("Ticket", "RESOLVED_BY", "SupportAgent")]], "ok"),
    ("Support", [SUPPORT, CDP], "Which company is this ticket for?",
     [[("Ticket", "FOR_COMPANY", "SupportCompany")]], "ok"),
    ("Support", [SUPPORT, CDP], "Which SLA applies to this ticket?",
     [[("Ticket", "GOVERNED_BY", "SupportSLA")]], "ok"),
    ("Support", [SUPPORT, CDP], "What happened on this ticket, and who wrote the notes?",
     [[("Ticket", "HAS", "TicketAuditTrail"), ("TicketAuditTrail", "HAS", "AuditEvent")],
      [("Ticket", "HAS", "Note"), ("Note", "WRITTEN_BY", "SupportAgent")]], "ok"),
    ("Support", [SUPPORT, CDP], "What did the AI suggest on this ticket, and to whom?",
     [[("Ticket", "HAS", "ProactiveHistory"), ("ProactiveHistory", "HAS", "Recommendation"),
       ("Recommendation", "SHOWN_TO", "SupportAgent")]], "ok"),
    ("Support", [SUPPORT, CDP], "Which macros does the team have, and who wrote them?",
     [[("Tenant", "HAS", "MacroLibrary"), ("MacroLibrary", "HAS", "Macro"), ("Macro", "CREATED_BY", "SupportAgent")]], "ok"),
    ("Support", [SUPPORT, CDP], "(boundary) Can a support agent open the customer's deals?",
     [[("Contact", "HAS", "DealHistory"), ("DealHistory", "HAS", "Deal")]], "blocked"),
    # -- Contact centre ----------------------------------------------------------------------
    ("Contact centre", [CC, CDP], "What conversations has this customer had, and what were they about?",
     [[("Contact", "HAS", "ConversationHistory"), ("ConversationHistory", "HAS", "Conversation")]], "ok"),
    ("Contact centre", [CC, CDP], "Which channel did this conversation come in on?",
     [[("Conversation", "VIA_CHANNEL", "Channel")]], "ok"),
    ("Contact centre", [CC, CDP], "Which queue and agent handled it?",
     [[("Conversation", "ROUTED_TO", "ContactCentreQueue")], [("Conversation", "HANDLED_BY", "ContactCentreAgent")]], "ok"),
    ("Contact centre", [CC, CDP], "Where does this web-chat widget send its chats?",
     [[("Channel", "ROUTES_TO", "ContactCentreQueue")]], "ok"),
    ("Contact centre", [CC, CDP], "Which callbacks are open for this agent?",
     [[("Task", "ASSIGNED_TO", "ContactCentreAgent")], [("Conversation", "HAS", "Task")]], "ok"),
    ("Contact centre", [CC, CDP], "Was this conversation transferred, and to whom?",
     [[("Conversation", "HAS", "Transfer"), ("Transfer", "OFFERED_TO", "ContactCentreAgent")]], "ok"),
    ("Contact centre", [CC, CDP], "What skills and queues does this agent have?",
     [[("ContactCentreAgent", "SKILLED_IN", "Skill")], [("ContactCentreAgent", "MEMBER_OF", "ContactCentreQueue")]], "ok"),
    ("Contact centre", [CC, CDP], "Which person is this agent?",
     [[("User", "ACTS_AS", "ContactCentreAgent")]], "ok"),
    ("Contact centre", [CC, CDP], "(boundary) Through the user, can it see their marketing work?",
     [[("User", "ACTS_AS", "ContactCentreAgent")], [("User", "ACTS_AS", "MarketingAgent")]], "blocked"),
    # -- Sales -----------------------------------------------------------------------------
    ("Sales", [SALES, CDP], "What is each rep's pipeline?",
     [[("Deal", "OWNED_BY", "SalesRep")], [("User", "ACTS_AS", "SalesRep")]], "ok"),
    ("Sales", [SALES, CDP], "What deals and invoices does this account have?",
     [[("Account", "HAS", "AccountDealHistory"), ("AccountDealHistory", "HAS", "Deal")],
      [("Account", "HAS", "AccountInvoiceHistory"), ("AccountInvoiceHistory", "HAS", "Invoice")]], "ok"),
    ("Sales", [SALES, CDP], "Which products sell, in quotes and orders?",
     [[("Quote", "HAS", "LineItem"), ("LineItem", "FOR_PRODUCT", "Product")],
      [("SalesOrder", "HAS", "LineItem")]], "ok"),
    ("Sales", [SALES, CDP], "How did this quote become an order and an invoice?",
     [[("SalesOrder", "FROM_QUOTE", "Quote")], [("Invoice", "FOR_ORDER", "SalesOrder")]], "ok"),
    ("Sales", [SALES, CDP], "Which company does this contact work at?",
     [[("Contact", "HAS", "SalesProfile"), ("SalesProfile", "WORKS_AT", "Account")]], "ok"),
    ("Sales", [SALES, CDP], "What came out of the meeting about this deal?",
     [[("ContactActivity", "HAS", "MeetingOutcome"), ("MeetingOutcome", "FOR_DEAL", "Deal")]], "ok"),
    # -- Marketing -------------------------------------------------------------------------
    ("Marketing", [MKT, CDP], "Which campaigns reached this contact?",
     [[("Contact", "HAS", "CampaignHistory"), ("CampaignHistory", "TARGETED_BY", "Campaign")]], "ok"),
    ("Marketing", [MKT, CDP], "How did this campaign's messages perform?",
     [[("Delivery", "FOR_CAMPAIGN", "Campaign")]], "ok"),
    ("Marketing", [MKT, CDP], "Who created this campaign, and from which assistant session and templates?",
     [[("Campaign", "CREATED_BY", "MarketingAgent")], [("Campaign", "FROM_SESSION", "Session")],
      [("Session", "HAS", "MessageTemplate")]], "ok"),
    ("Marketing", [MKT, CDP], "How engaged is this contact with email?",
     [[("Contact", "HAS", "EmailEngagement")]], "ok"),
    # -- CDP -------------------------------------------------------------------------------
    ("CDP", [CDP], "Who is in this segment or persona?",
     [[("Contact", "IN_SEGMENT", "Segment")], [("Contact", "MATCHES_PERSONA", "Persona")]], "ok"),
    ("CDP", [CDP], "Who bought this product, and how many?",
     [[("Product", "HAS", "ProductPurchaseHistory"), ("ProductPurchaseHistory", "HAS", "Purchase")]], "ok"),
    ("CDP", [CDP], "What has this customer bought?",
     [[("Contact", "HAS", "PurchaseHistory"), ("PurchaseHistory", "HAS_PERIOD", "LogPeriod"),
       ("LogPeriod", "HAS", "Purchase")]], "ok"),
    ("CDP", [CDP], "How do we recognise this customer (emails, phones)?",
     [[("Contact", "HAS", "ContactProfile"), ("ContactProfile", "HAS", "ContactIdentity")]], "ok"),
    ("CDP", [CDP], "What do we know and recommend for this customer?",
     [[("Contact", "HAS", "InsightHistory"), ("InsightHistory", "HAS", "ContactInsight")],
      [("Contact", "HAS", "NextBestActionHistory"), ("NextBestActionHistory", "HAS", "NextBestAction")]], "ok"),
    # -- Web tracking ----------------------------------------------------------------------
    ("Web tracking", [WEB, CDP], "What did this customer do on our website?",
     [[("Contact", "HAS", "SessionHistory"), ("SessionHistory", "HAS_PERIOD", "LogPeriod"), ("LogPeriod", "HAS", "Session")],
      [("Session", "HAS", "Event")], [("Session", "ON_WEBSITE", "Website")]], "ok"),
    ("Web tracking", [WEB, CDP], "(boundary) Can web tracking open the campaign that drove a visit?",
     [[("Session", "FROM_CAMPAIGN", "Campaign")]], "blocked"),
    # -- Hub (every product) ---------------------------------------------------------------
    ("Hub", HUB, "Which campaign drove this web visit?",
     [[("Session", "FROM_CAMPAIGN", "Campaign")]], "ok"),
    ("Hub", HUB, "Did this customer contact support after receiving a campaign?",
     [[("Contact", "HAS", "DeliveryHistory"), ("DeliveryHistory", "HAS_PERIOD", "LogPeriod"), ("LogPeriod", "HAS", "Delivery")],
      [("Contact", "HAS", "ConversationHistory"), ("ConversationHistory", "HAS", "Conversation")],
      [("Contact", "HAS", "TicketHistory"), ("TicketHistory", "HAS", "Ticket")]], "ok"),
    ("Hub", HUB, "Which people work in more than one product?",
     [[("User", "ACTS_AS", "SupportAgent")], [("User", "ACTS_AS", "ContactCentreAgent")],
      [("User", "ACTS_AS", "SalesRep")], [("User", "ACTS_AS", "MarketingAgent")]], "ok"),
    ("Hub", HUB, "Which channels bring the most conversations?",
     [[("Conversation", "VIA_CHANNEL", "Channel")], [("Tenant", "HAS", "ChannelDirectory"), ("ChannelDirectory", "HAS", "Channel")]], "ok"),
]


def schema_edges(eff) -> set[tuple[str, str, str]]:
    """Every (from_label, TYPE, to_label) the config can write, in its real direction."""
    out = set()
    for m in eff.tables.values():
        if m.mode == "node" and m.label:
            for r in m.relationships or []:
                t = r.type.upper()
                out.add((m.label, t, r.target_label) if r.direction == "out" else (r.target_label, t, m.label))
                if r.acts_as_user:
                    out.add(("User", "ACTS_AS", r.target_label))
        if m.mode == "relationship_only":
            if m.collections:
                c = m.collections[0]
                out.add((m.from_label, "HAS", c["name"]))
                out.add((c["name"], (m.type or "").upper(), m.to_label))
            else:
                out.add((m.from_label, (m.type or "").upper(), m.to_label))
        for c in (m.collections or []) if m.mode == "node" else []:
            out.add((c["owner_label"], "HAS", c["name"]))
            if c.get("bucket_column"):
                out.add((c["name"], "HAS_PERIOD", "LogPeriod"))
                out.add(("LogPeriod", "HAS", m.label))
            else:
                out.add((c["name"], "HAS", m.label))
    return out


def readable(eff, label: str, products: list[str]) -> bool:
    owners = eff.label_scopes.get(label)
    if owners is None:
        return False
    return not owners or bool(set(owners) & set(products))


def check(eff, products, paths) -> tuple[str, list[str]]:
    edges = schema_edges(eff)
    notes, blocked = [], False
    for path in paths:
        for a, t, b in path:
            if (a, t, b) not in edges:
                rev = (b, t, a) in edges
                notes.append(f"missing edge ({a})-[:{t}]->({b})" + (" (exists the other way round)" if rev else ""))
            for lbl in (a, b):
                if not readable(eff, lbl, products):
                    blocked = True
                    notes.append(f"{lbl} is outside the agent's products")
    if any(n.startswith("missing") for n in notes):
        return "missing", notes
    return ("blocked" if blocked else "ok"), notes


def run(eff):
    rows = []
    for agent, products, q, paths, expect in QUESTIONS:
        got, notes = check(eff, products, paths)
        rows.append((agent, products, q, paths, expect, got, notes))
    return rows


def write_doc(rows) -> Path:
    def fmt(path):
        s = f"({path[0][0]})"
        for a, t, b in path:
            s += f"-[:{t}]->({b})"
        return s
    ok = sum(1 for r in rows if r[4] == r[5])
    out = ["# Ontology reasoning check", "",
           "Generated by `python scripts/reasoning_check.py` (also run as a test). Each product question",
           "is written as the path an agent walks. Every hop must exist in that direction, and every node",
           "on the path must be readable by the agent (no product label, or one of its products).",
           "`blocked` questions are the product boundary working: the agent must be refused.", "",
           f"**{ok} of {len(rows)} questions behave as expected.**", "",
           "## Direction rules", "",
           "1. **Containment points from the whole to its part:** `(Contact)-[:HAS]->(TicketHistory)-[:HAS]->(Ticket)`,",
           "   `(Conversation)-[:HAS]->(Transfer)`, `(Product)-[:HAS]->(ProductPurchaseHistory)`.",
           "2. **Every other edge points from the record that holds the reference to the record it names,**",
           "   and reads as a sentence from its start: `(Ticket)-[:ASSIGNED_TO]->(SupportAgent)`,",
           "   `(Deal)-[:OWNED_BY]->(SalesRep)`, `(Session)-[:FROM_CAMPAIGN]->(Campaign)`.",
           "3. **A person acts in a product through a role:** `(User)-[:ACTS_AS]->(ContactCentreAgent)`.",
           "4. Traversal works both ways; the direction only fixes the meaning, so each question starts",
           "   wherever the agent starts and follows edges forwards or backwards.", "",
           "## How a conversation finds its channel", "",
           "`channel_config_id` is optional on a conversation, so the link is resolved in order:", "",
           "| Order | Conversation column | Matches on the `Channel` node | Works for |",
           "|---|---|---|---|",
           "| 1 | `channel_config_id` | `id` (the channel config) | any channel, when filled |",
           "| 2 | `webchat_id` | `chat_widget_id` (from `setup_webchat_configs.webchat_id`) | web chat |",
           "| 3 | `mailbox_connection_id` | `email_connection_id` (from `setup_email_configs.oauth_connection_id`) | email |",
           "", "WhatsApp, Instagram and Facebook conversations only link when `channel_config_id` is filled (the",
           "conversation carries no page or number id). Voice conversations have no configured channel on the",
           "row. Every conversation still carries its channel **type** as a label (`ChatConversation`,",
           "`EmailConversation`, `WhatsAppConversation`, `VoiceConversation`, ...), so reports by channel type",
           "always work; `VIA_CHANNEL` answers *which* widget, mailbox or account.", "",
           "## Data checks after the backfill", "",
           "Run on a tenant database; each should return 0 (or a small, explainable number):", "",
           "```cypher",
           "// conversations whose contact_id did not match a CDP contact (id-space check)",
           "MATCH (c:Conversation) WHERE NOT (c)<-[:HAS]-(:ConversationHistory) RETURN count(c);",
           "// campaign deliveries that found no contact through campaign_contact_id",
           "MATCH (d:Delivery) WHERE NOT (d)<-[:HAS]-(:LogPeriod) RETURN count(d);",
           "// chat/email conversations without a channel",
           "MATCH (c:Conversation) WHERE (c:ChatConversation OR c:EmailConversation) AND NOT (c)-[:VIA_CHANNEL]->() RETURN count(c);",
           "// order lines whose product sku was not found in inventory",
           "MATCH (p:Product) WHERE p._stub_since IS NOT NULL RETURN count(p);",
           "```", ""]
    agent = None
    for a, products, q, paths, expect, got, notes in rows:
        if a != agent:
            agent = a
            out += [f"## {a} agent (reads: {', '.join(products)} + unlabelled setup/catalog)", "",
                    "| Question | Path | Expected | Result |", "|---|---|---|---|"]
        mark = "✅" if expect == got else "❌"
        out.append(f"| {q} | {'<br>'.join('`' + fmt(p) + '`' for p in paths)} | {expect} | {mark} {got}"
                   + (f"<br>{'; '.join(sorted(set(notes)))}" if notes and expect != got else "") + " |")
        nxt = rows.index((a, products, q, paths, expect, got, notes)) + 1
        if nxt < len(rows) and rows[nxt][0] != a:
            out.append("")
    path = ROOT / "docs" / "ONTOLOGY_REASONING.md"
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    return path


if __name__ == "__main__":
    from src.config_loader import ConfigLoader
    rows = run(ConfigLoader(ROOT / "config").effective("org1000"))
    print("wrote", write_doc(rows))
    for r in rows:
        if r[4] != r[5]:
            print("UNEXPECTED:", r[0], "|", r[2], "|", r[5], r[6])
