"""One-off, second review round of the v4.5 model (kept for the record).

* anomaly alerts: used for another purpose -> out
* cdp.interactions: CDP copy of contact-centre conversations (one row per conversation) -> out
* cdp.customer_interactions: unified copy of conversations/cases/clicks -> out (one thing, one node)
* ccass.conversation_memory: the conversation summary -> properties `summary` / `facts` on Conversation
* cdp.customer_events / customer_sessions: collected by web tracking -> WebTracking product label
* cdp.customer_journey (derived transitions), cdp.semantic_memory (agent memory store) -> out
* cdp.order_items: points at cdp.products (CDP's catalogue copy), not inventory -> out
* campaign.website_contacts (visitor copy + push credentials), campaign.email_suppressions
  (second unsubscribe list; the contact's engagement row already carries unsubscribe) -> out
* ticketing.automation_rules, ticketing.macros (configuration/templates, never linked) -> out
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

OUT = ["cdp.anomaly_alerts", "cdp.interactions", "cdp.customer_interactions", "cdp.customer_journey",
       "cdp.semantic_memory", "cdp.order_items", "campaign.website_contacts", "campaign.email_suppressions",
       "ticketing.automation_rules", "ticketing.macros"]
for t in OUT:
    A[t] = T.pop(t)


def flow(d):
    m = CommentedMap(d); m.fa.set_flow_style(); return m


def owns(t, names):
    s = CommentedSeq(names); s.fa.set_flow_style(); T[t]["owns_collections"] = s


owns("accounts.organizations", ["UserDirectory", "TeamDirectory", "ChannelDirectory", "KnowledgeLibrary",
                                 "ProductCatalog"])
owns("cdp.customer_profile", ["ContactProfile", "ConversationHistory", "TicketHistory", "DealHistory",
                               "OrderHistory", "ActivityHistory", "PurchaseHistory", "CampaignHistory",
                               "DeliveryHistory", "EventHistory", "SessionHistory", "InsightHistory",
                               "NextBestActionHistory"])

# conversation memory -> the conversation's own summary
mem = A.pop("ccass.conversation_memory")
spec = CommentedMap()
spec["product"] = mem.get("product", "ccaas")
spec["merges_into"] = "Conversation"
spec["key"] = "id"
spec["key_column"] = "conversation_id"
spec["access"] = mem["access"]
spec["on_delete"] = "remove_own_properties"
spec["note"] = ("One row per conversation: the rolling summary and facts the bot keeps. Written onto the "
                "Conversation node as `summary` and `facts` (no separate summary node).")
spec["properties"] = CommentedSeq([flow({"name": "summary", "from": "rolling_summary"}),
                                   flow({"name": "facts", "from": "extracted_facts"}),
                                   flow({"name": "summary_updated_at", "from": "updated_at"})])
T.insert(list(T).index("ccass.conversations") + 1, "ccass.conversation_memory", spec)

# the conversation is searchable by meaning through its subject and summary
T["ccass.conversations"]["embed"] = flow({"property": "_embedding",
                                          "fields": ["subject", "summary", "resolution_summary", "channel"]})

# web-tracking data collected into cdp: product label WebTracking, session keys web:<id>
T["cdp.customer_sessions"]["synthesize_key"] = "web:{session_id}"
for t in ("cdp.customer_events",):
    for r in T[t].get("relationships") or []:
        if r.get("from") == "Session":
            r["to_key_template"] = "web:{session_id}"

with CORE.open("w") as f:
    y.dump(doc, f)
with ARCH.open("w") as f:
    f.write("# Tables removed from the knowledge graph in v4.5 (configuration plumbing, internal\n"
            "# logs, high-volume metrics, copies of another product's data, or duplicates).\n"
            "# To bring one back, move its block into core.yaml and run `main.py validate`.\n")
    y.dump(arch, f)
print(len(T), "tables in core;", len(A), "archived")
