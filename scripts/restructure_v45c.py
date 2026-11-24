"""One-off, third review round of the v4.5 model (kept for the record).

* kept "what happened" nodes get proper relations instead of floating:
  Conversation HAS Task / InternalNote / Transfer / MissedContact, each linked to the
  ContactCentreAgent involved (users are matched to their agent through setup_user_id);
  ContactActivity HAS the meeting's Session and MeetingOutcome; AI recommendations are
  SHOWN_TO and approvals DECIDED_BY a SupportAgent; a campaign Session HAS its MessageTemplates.
* ticketing macros come back: (Tenant)-[:HAS]->(MacroLibrary)-[:HAS]->(Macro)-[:CREATED_BY]->(SupportAgent)
* order items come back as the link between an order and a product:
  (Product)-[:HAS]->(ProductPurchaseHistory)-[:HAS {quantity, unit_price, subtotal}]->(Purchase)
  cdp.order_items points at cdp.products; its sku finds the real inventory product.
* Campaign stores contacts as a bigint projection of cdp_id: every Contact carries
  campaign_contact_id (src/id_mapping.py) and the three campaign references match on it,
  never creating contacts.
"""
from pathlib import Path
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

ROOT = Path(__file__).resolve().parent.parent
CORE = ROOT / "config/mappings/core.yaml"
ARCH = ROOT / "config/archive/dropped_from_graph_v45.yaml"
y = YAML(); y.preserve_quotes = True; y.width = 120
doc = y.load(CORE.read_text()); T = doc["tables"]
arch = y.load(ARCH.read_text()); A = arch["tables"]


def flow(d):
    m = CommentedMap(d); m.fa.set_flow_style(); return m


def rels(t, *items):
    T[t]["relationships"] = CommentedSeq([flow(i) for i in items])


def to(t, label, key, col, **kw): return {"type": t, "to": label, "to_key": key, "from_column": col, **kw}
def frm(t, label, key, col, **kw): return {"type": t, "from": label, "from_key": key, "from_column": col, **kw}
def match(t, label, fields): return {"type": t, "to": label, "match_fields": fields}


def props_have(t, *cols):
    have = {p if isinstance(p, str) else (p.get("as") or p.get("name")) for p in T[t].get("properties") or []}
    missing = [c for c in cols if c not in have]
    assert not missing, (t, missing)


def owns_add(t, *names):
    cur = list(T[t].get("owns_collections") or [])
    s = CommentedSeq(cur + [n for n in names if n not in cur]); s.fa.set_flow_style()
    T[t]["owns_collections"] = s


# -- contact centre: everything about a conversation hangs off it --------------------------
props_have("ccass.agents", "setup_user_id")
AGENT_BY_USER = lambda col: {"setup_user_id": col}
rels("ccass.agent_tasks", frm("has", "Conversation", "id", "conversation_id"),
     match("assigned_to", "ContactCentreAgent", AGENT_BY_USER("assigned_user_id")),
     to("created_by", "User", "id", "created_by_user_id"))
rels("ccass.conversation_notes", frm("has", "Conversation", "id", "conversation_id"),
     match("written_by", "ContactCentreAgent", AGENT_BY_USER("author_user_id")))
props_have("ccass.conversation_transfers", "from_agent_id", "to_queue_id", "offered_agent_id")
rels("ccass.conversation_transfers", frm("has", "Conversation", "id", "conversation_id"),
     to("from_queue", "ContactCentreQueue", "id", "from_queue_id"),
     to("to_queue", "ContactCentreQueue", "id", "to_queue_id"),
     to("from_agent", "ContactCentreAgent", "id", "from_agent_id"),
     to("offered_to", "ContactCentreAgent", "id", "offered_agent_id"))
rels("ccass.agent_missed_contacts", frm("has", "Conversation", "id", "conversation_id"),
     to("missed_by", "ContactCentreAgent", "id", "agent_id"),
     to("in_queue", "ContactCentreQueue", "id", "queue_id"))

# -- sales: meeting session and outcome belong to the meeting activity ----------------------
rels("crm.meeting_assist_sessions", frm("has", "ContactActivity", "id", "activity_id"),
     to("hosted_by", "SalesRep", "user_id", "host_user_id", acts_as_user=True))

# -- support: AI suggestions and approvals name the agent involved --------------------------
rels("ticketing.proactive_recommendations", to("shown_to", "SupportAgent", "id", "agent_id"))
rels("ticketing.proactive_approvals", to("decided_by", "SupportAgent", "id", "agent_id"))

# -- marketing: a campaign-assistant session holds the templates drafted in it ---------------
t = T["campaign.campaign_channel_templates"]
if "collection" in t:
    del t["collection"]
rels("campaign.campaign_channel_templates",
     frm("has", "Session", "uid", "session_id", to_key_template="campaign:{session_id}"))
if "owns_collections" in T["campaign.campaign_agent_sessions"]:
    del T["campaign.campaign_agent_sessions"]["owns_collections"]

# -- ticketing macros: the tenant's macro library --------------------------------------------
T["ticketing.macros"] = A.pop("ticketing.macros")
T["ticketing.macros"]["collection"] = flow({"name": "MacroLibrary", "owner_column": "organization_id"})
rels("ticketing.macros", to("created_by", "SupportAgent", "id", "created_by_id"))
owns_add("accounts.organizations", "MacroLibrary")

# -- order items: order <-> product with the line details on the edge -------------------------
oi = A.pop("cdp.order_items")
oi["from"] = flow({"label": "Product", "key": "id", "column": "inventory_product_id", "create": False})
oi["to"] = flow({"label": "Purchase", "key": "order_id", "column": "order_id", "create": False})
oi["type"] = "has"
oi["properties"] = CommentedSeq(["quantity", "unit_price", flow({"from": "subtotal_amount", "as": "subtotal"})])
oi["collection"] = flow({"name": "ProductPurchaseHistory"})
oi["note"] = ("cdp.order_items.product_id points at cdp.products (CDP's catalogue copy). Its sku finds "
              "the inventory product (see `derived`), so the order links to the real catalogue node.")
T["cdp.order_items"] = oi
owns_add("inventory.products", "ProductPurchaseHistory")
d = doc["derived"]
d["cdp.order_items.product_sku"] = flow({"lookup": flow({"table": "cdp.products", "match_local": "product_id",
                                                           "match_remote": "product_id", "select": "sku",
                                                           "scope": "organization_id"})})
d["cdp.order_items.inventory_product_id"] = flow({"lookup": flow({"table": "inventory.products",
                                                                   "match_local": "product_sku",
                                                                   "match_remote": "sku", "select": "id",
                                                                   "scope": "organization_id"})})

# -- campaign contact ids ------------------------------------------------------------------
d["cdp.customer_profile.campaign_contact_id"] = flow({"function": "campaign_contact_id", "from": "cdp_id"})
cp = T["cdp.customer_profile"]["properties"]
if "campaign_contact_id" not in cp:
    cp.append("campaign_contact_id")
T["campaign.campaign_send_recipients"]["collections"][0]["owner_key"] = "campaign_contact_id"
T["campaign.campaign_send_recipients"]["collections"][0]["create_owner"] = False
rels("campaign.campaign_contact_engagement",
     frm("has", "Contact", "campaign_contact_id", "contact_id", create_target=False))
aud = T["campaign.campaign_audience"]
aud["from"] = flow({"label": "Contact", "key": "campaign_contact_id", "column": "contact_id", "create": False})

with CORE.open("w") as f:
    y.dump(doc, f)
with ARCH.open("w") as f:
    f.write("# Tables removed from the knowledge graph in v4.5 (configuration plumbing, internal\n"
            "# logs, high-volume metrics, copies of another product's data, or duplicates).\n"
            "# To bring one back, move its block into core.yaml and run `main.py validate`.\n")
    y.dump(arch, f)
print(len(T), "tables in core;", len(A), "archived")
