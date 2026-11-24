"""One-off: reshape config/mappings/core.yaml to the v4.5 graph model (kept for the record).

Decisions (agreed in review):
* product labels: ticketing=Support, crm=Sales, campaign=Marketing, ccaas=ContactCentre,
  cdp=CDP, webtracking=WebTracking. accounts + inventory carry NO product label.
* users: (User)-[:ACTS_AS]->(ContactCentreAgent|SupportAgent|SalesRep|MarketingAgent)
* product-specific names: SupportQueue/ContactCentreQueue, SupportSLA/ContactCentreSLA
* generic Session/Conversation labels across products (product label says which)
* HAS for containment, verbs for business meaning; collections only for anchors
* ~80 plumbing/config/high-volume tables leave the graph (archived in config/archive)
"""
import sys
from pathlib import Path
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

sys.path.insert(0, "/tmp")
from decisions import DROP  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CORE = ROOT / "config/mappings/core.yaml"
ARCHIVE = ROOT / "config/archive/dropped_from_graph_v45.yaml"

yaml = YAML()
yaml.preserve_quotes = True
yaml.width = 120
doc = yaml.load(CORE.read_text())
T = doc["tables"]


def flow(d):
    m = CommentedMap(d)
    m.fa.set_flow_style()
    return m


def rels(*items):
    s = CommentedSeq([flow(i) for i in items])
    return s


def to(t, label, key, col, **kw):
    return {"type": t, "to": label, "to_key": key, "from_column": col, **kw}


def frm(t, label, key, col, **kw):
    return {"type": t, "from": label, "from_key": key, "from_column": col, **kw}


def match(t, label, fields):
    return {"type": t, "to": label, "match_fields": fields}


def coll(name, owner, bucket=None, **kw):
    c = {"name": name, "owner_column": owner}
    if bucket:
        c["bucket_column"] = bucket
    c.update(kw)
    return flow(c)


def set_label(t, label):
    if "merges_into" in T[t]:
        T[t]["merges_into"] = label
    else:
        T[t]["label"] = label


def set_key(t, key, key_column=None, synth=None):
    T[t]["key"] = key
    for k in ("key_column", "synthesize_key"):
        if k in T[t]:
            del T[t][k]
    if key_column:
        T[t]["key_column"] = key_column
    if synth:
        T[t]["synthesize_key"] = synth


def set_rels(t, *items):
    if items:
        T[t]["relationships"] = rels(*items)
    elif "relationships" in T[t]:
        del T[t]["relationships"]


def set_coll(t, *cols):
    for k in ("collection", "collections"):
        if k in T[t]:
            del T[t][k]
    if len(cols) == 1:
        T[t]["collection"] = cols[0]
    elif cols:
        T[t]["collections"] = CommentedSeq(list(cols))


def owns(t, *names):
    s = CommentedSeq(list(names))
    s.fa.set_flow_style()
    T[t]["owns_collections"] = s


def fold(t, into_label, key, key_column, drop_props=("id", "created_at", "updated_at")):
    """1:1 table -> extra properties on its owner's node (merged table)."""
    spec = T[t]
    if "label" in spec:
        del spec["label"]
    spec["merges_into"] = into_label
    set_key(t, key, key_column=key_column if key_column != key else None)
    spec["on_delete"] = "remove_own_properties"
    props = spec.get("properties") or []
    keep = [p for p in props if (p if isinstance(p, str) else p.get("name")) not in drop_props]
    spec["properties"] = CommentedSeq(keep)
    set_rels(t)
    set_coll(t)


# -- 1. drop -------------------------------------------------------------------------------
archive = CommentedMap()
for t in sorted(DROP):
    archive[t] = T.pop(t)
ARCHIVE.parent.mkdir(parents=True, exist_ok=True)
with ARCHIVE.open("w") as f:
    f.write("# Tables removed from the knowledge graph in v4.5 (configuration plumbing, internal\n"
            "# logs, high-volume metrics, or duplicates of another table). Kept here for reference;\n"
            "# to bring one back, move its block into core.yaml and run `main.py validate`.\n")
    yaml.dump(CommentedMap({"tables": archive}), f)

# -- 2. accounts (no product label) ------------------------------------------------------------
owns("accounts.organizations", "UserDirectory", "TeamDirectory", "ChannelDirectory", "KnowledgeLibrary",
     "ProductCatalog", "AlertHistory")
set_label("accounts.setup_users", "User")
if "owns_collections" in T["accounts.setup_users"]:
    del T["accounts.setup_users"]["owns_collections"]
set_rels("accounts.setup_users", to("member_of", "Team", "id", "team_id"),
         to("follows", "BusinessHours", "id", "business_hours_id"))
set_coll("accounts.setup_users", coll("UserDirectory", "organization_id"))
set_rels("accounts.setup_teams", to("follows", "BusinessHours", "id", "business_hours_id"))
set_coll("accounts.setup_teams", coll("TeamDirectory", "organization_id"))
set_rels("accounts.setup_roles")
set_rels("accounts.setup_business_hours", to("follows", "HolidayCalendar", "id", "holiday_calendar_id"))
set_rels("accounts.setup_business_hour_shifts", frm("has", "BusinessHours", "id", "business_hours_id"))
set_rels("accounts.setup_holidays", frm("has", "HolidayCalendar", "id", "calendar_id"))
for t in ("accounts.setup_channel_configs", "accounts.setup_email_configs",
          "accounts.setup_webchat_configs", "accounts.setup_social_configs"):
    set_label(t, "Channel")
set_coll("accounts.setup_channel_configs", coll("ChannelDirectory", "organization_id"))
set_rels("accounts.setup_webchat_configs",
         match("routes_to", "ContactCentreQueue", {"setup_queue_id": "routing_queue_id"}))
set_rels("accounts.setup_voice_dids",
         match("routes_to", "ContactCentreQueue", {"setup_queue_id": "routing_queue_id"}),
         to("routes_to", "User", "id", "routing_user_id"))
set_label("accounts.setup_knowledge_documents", "KnowledgeDocument")
set_rels("accounts.setup_knowledge_documents", to("uploaded_by", "User", "id", "uploaded_by"))
set_coll("accounts.setup_knowledge_documents", coll("KnowledgeLibrary", "organization_id"))
fold("accounts.setup_knowledge_org_profiles", "Tenant", "id", "organization_id")
set_coll("inventory.products", coll("ProductCatalog", "organization_id"))

# -- 3. cdp ---------------------------------------------------------------------------------
owns("cdp.customer_profile", "ContactProfile", "ConversationHistory", "TicketHistory", "DealHistory",
     "OrderHistory", "ActivityHistory", "PurchaseHistory", "CampaignHistory", "DeliveryHistory",
     "EventHistory", "SessionHistory", "TouchpointHistory", "JourneyHistory", "InsightHistory",
     "NextBestActionHistory", "FactHistory")
set_coll("cdp.identity_keys", coll("ContactProfile", "cdp_id"))
set_coll("cdp.contact_identities", coll("ContactProfile", "contact_id"))
SESSION = {"to_key_template": "cdp:{session_id}"}
set_rels("cdp.customer_events", frm("has", "Session", "uid", "session_id", **SESSION),
         to("on_website", "Website", "site_id", "site_id"))
set_key("cdp.customer_sessions", "uid", key_column="session_id", synth="cdp:{session_id}")
set_rels("cdp.customer_sessions", to("on_website", "Website", "site_id", "site_id"))
set_rels("cdp.customer_interactions", frm("has", "Session", "uid", "session_id", **SESSION))
set_rels("cdp.customer_journey", frm("has", "Session", "uid", "session_id", **SESSION))
set_coll("cdp.journey_states")
set_rels("cdp.journey_states", frm("has", "Contact", "cdp_id", "cdp_id"))
for t, name in (("cdp.ai_insights", "InsightHistory"), ("cdp.customer_recommendations", "NextBestActionHistory"),
                ("cdp.semantic_memory", "FactHistory"), ("cdp.orders", "PurchaseHistory")):
    c = T[t].get("collection")
    set_coll(t, coll(name, c["owner_column"], c.get("bucket_column")))
set_rels("cdp.anomaly_alerts")
set_coll("cdp.anomaly_alerts", coll("AlertHistory", "organization_id"))
set_coll("cdp.interactions")
set_rels("cdp.interactions", frm("has", "Conversation", "id", "conversation_id"))
for t in ("cdp.customer_segments", "cdp.customer_persona_assignments"):
    set_coll(t)
fold("cdp.customer_scores", "Contact", "cdp_id", "cdp_id")
fold("cdp.gold_customer_metrics", "Contact", "cdp_id", "cdp_id")

# -- 4. contact centre ----------------------------------------------------------------------
set_label("ccass.agents", "ContactCentreAgent")
set_key("ccass.agents", "id")
set_rels("ccass.agents", frm("acts_as", "User", "id", "setup_user_id"))
set_label("ccass.queues", "ContactCentreQueue")
set_key("ccass.queues", "id")
if "owns_collections" in T["ccass.queues"]:
    del T["ccass.queues"]["owns_collections"]
set_rels("ccass.queues", to("owned_by", "Team", "id", "team_id"),
         to("follows", "BusinessHours", "id", "business_hours_id"))
set_label("ccass.sla_policies", "ContactCentreSLA")
set_rels("ccass.conversations",
         to("routed_to", "ContactCentreQueue", "id", "queue_id"),
         to("handled_by", "ContactCentreAgent", "id", "assigned_agent_id"),
         to("via_channel", "Channel", "id", "channel_config_id"),
         to("continues", "Conversation", "id", "previous_conversation_id"))
set_coll("ccass.conversations", coll("ConversationHistory", "contact_id"))
set_rels("ccass.conversation_notes", frm("has", "Conversation", "id", "conversation_id"),
         to("written_by", "User", "id", "author_user_id"))
set_rels("ccass.conversation_transfers", frm("has", "Conversation", "id", "conversation_id"),
         to("from_queue", "ContactCentreQueue", "id", "from_queue_id"))
set_label("ccass.agent_missed_contacts", "MissedContact")
set_coll("ccass.agent_missed_contacts")
set_rels("ccass.agent_missed_contacts", to("about", "Conversation", "id", "conversation_id"),
         to("missed_by", "ContactCentreAgent", "id", "agent_id"))
set_coll("ccass.agent_tasks")
set_rels("ccass.agent_tasks", to("about", "Conversation", "id", "conversation_id"),
         to("created_by", "User", "id", "created_by_user_id"))
fold("ccass.agent_presence", "ContactCentreAgent", "id", "agent_id")
fold("ccass.queue_routing_profiles", "ContactCentreQueue", "id", "queue_id")

# -- 5. ticketing ---------------------------------------------------------------------------
set_label("ticketing.agents", "SupportAgent")
set_key("ticketing.agents", "id")
set_rels("ticketing.agents", frm("acts_as", "User", "id", "setup_user_id"))
set_label("ticketing.ticketing_accounts", "SupportCompany")
set_label("ticketing.agent_queues", "SupportQueue")
set_key("ticketing.agent_queues", "id")
set_label("ticketing.sla_policies", "SupportSLA")
set_rels("ticketing.tickets",
         to("routed_to", "SupportQueue", "id", "queue_id"),
         to("assigned_to", "SupportAgent", "id", "assignee_id"),
         to("resolved_by", "SupportAgent", "id", "resolver_id"),
         match("governed_by", "SupportSLA", {"priority": "priority", "organization_id": "organization_id"}))
set_rels("ticketing.ticket_notes", frm("has", "Ticket", "id", "ticket_id"),
         to("written_by", "SupportAgent", "id", "author_id"))
set_rels("ticketing.kb_articles", to("written_by", "SupportAgent", "id", "author_id"))
set_rels("ticketing.audit_events", to("performed_by", "SupportAgent", "id", "actor_id"))
set_rels("ticketing.email_message_refs", frm("has", "Ticket", "id", "ticket_id"))
set_label("ticketing.conversation_contexts", "Session")
set_key("ticketing.conversation_contexts", "uid", key_column="id", synth="ticketing:{id}")
set_rels("ticketing.conversation_contexts", to("used_by", "SupportAgent", "id", "user_id"))

# -- 6. crm ---------------------------------------------------------------------------------
REP = {"acts_as_user": True}
owns("crm.companies", "AccountDealHistory", "AccountOrderHistory", "AccountInvoiceHistory",
     "AccountContractHistory")
set_rels("crm.companies", to("owned_by", "SalesRep", "user_id", "owner_user_id", **REP),
         to("part_of", "Account", "id", "parent_company_id"))
set_coll("crm.contact_profiles")
set_rels("crm.contact_profiles", frm("has", "Contact", "cdp_id", "account_contact_id"),
         to("works_at", "Account", "id", "company_id"),
         to("owned_by", "SalesRep", "user_id", "owner_user_id", **REP),
         to("converted_to", "Deal", "id", "converted_deal_id"))
set_rels("crm.deals", to("owned_by", "SalesRep", "user_id", "owner_user_id", **REP))
set_coll("crm.deals", coll("DealHistory", "contact_id"), coll("AccountDealHistory", "company_id"))
set_rels("crm.deal_activities", frm("has", "Deal", "id", "deal_id"),
         to("created_by", "SalesRep", "user_id", "created_by_user_id", **REP))
set_coll("crm.contact_activities", coll("ActivityHistory", "contact_id"))
set_rels("crm.contact_activities", to("assigned_to", "SalesRep", "user_id", "assignee_user_id", **REP))
set_rels("crm.appointments", frm("has", "ContactActivity", "id", "activity_id"),
         to("hosted_by", "SalesRep", "user_id", "host_user_id", **REP))
set_rels("crm.proposals", frm("has", "Deal", "id", "deal_id"),
         to("approved_by", "SalesRep", "user_id", "approved_by_user_id", **REP))
set_rels("crm.quotes", frm("has", "Deal", "id", "deal_id"), to("from_proposal", "Proposal", "id", "proposal_id"),
         to("renews", "Contract", "id", "renewal_contract_id"),
         to("approved_by", "SalesRep", "user_id", "approved_by_user_id", **REP))
set_rels("crm.sales_orders", frm("has", "Deal", "id", "deal_id"), to("from_quote", "Quote", "id", "quote_id"))
set_coll("crm.sales_orders", coll("OrderHistory", "contact_id"), coll("AccountOrderHistory", "company_id"))
set_rels("crm.invoices", to("for_order", "SalesOrder", "id", "order_id"),
         to("under_contract", "Contract", "id", "contract_id"),
         to("approved_by", "SalesRep", "user_id", "approved_by_user_id", **REP))
set_coll("crm.invoices", coll("AccountInvoiceHistory", "company_id"))
for t, parent, col in (("crm.quote_lines", "Quote", "quote_id"), ("crm.order_lines", "SalesOrder", "order_id"),
                       ("crm.invoice_lines", "Invoice", "invoice_id")):
    set_rels(t, frm("has", parent, "id", col), to("for_product", "Product", "id", "product_id"))
set_rels("crm.contracts", to("from_deal", "Deal", "id", "deal_id"), to("renewed_by", "Deal", "id", "renew_deal_id"))
set_coll("crm.contracts", coll("AccountContractHistory", "company_id"))
set_label("crm.sales_user_assignment_profiles", "SalesRep")
set_key("crm.sales_user_assignment_profiles", "user_id")
set_rels("crm.sales_user_assignment_profiles", frm("acts_as", "User", "id", "user_id"))
set_coll("crm.lead_summaries")
set_rels("crm.lead_summaries", frm("has", "Contact", "cdp_id", "contact_id"))
set_label("crm.meeting_assist_sessions", "Session")
set_key("crm.meeting_assist_sessions", "uid", key_column="id", synth="crm:{id}")
set_rels("crm.meeting_assist_sessions", to("about", "ContactActivity", "id", "activity_id"),
         to("hosted_by", "SalesRep", "user_id", "host_user_id", **REP))
set_rels("crm.meeting_analyses", frm("has", "ContactActivity", "id", "activity_id"),
         to("for_deal", "Deal", "id", "deal_id"))
set_coll("crm.privacy_requests")
set_rels("crm.privacy_requests", frm("has", "Contact", "cdp_id", "contact_id"),
         to("raised_in", "ContactActivity", "id", "activity_id"),
         to("reviewed_by", "SalesRep", "user_id", "reviewed_by_user_id", **REP))

# -- 7. campaign ------------------------------------------------------------------------------
MREP = {"acts_as_user": True}
owns("campaign.campaigns", "CampaignDeliveryHistory")
set_rels("campaign.campaigns", to("created_by", "MarketingAgent", "user_id", "created_by_user_id", **MREP),
         to("from_session", "Session", "uid", "session_id", to_key_template="campaign:{session_id}"))
set_label("campaign.campaign_agent_sessions", "Session")
set_key("campaign.campaign_agent_sessions", "uid", key_column="id", synth="campaign:{id}")
set_rels("campaign.campaign_agent_sessions",
         to("created_by", "MarketingAgent", "user_id", "created_by_user_id", **MREP))
set_rels("campaign.campaign_channel_templates",
         to("drafted_in", "Session", "uid", "session_id", to_key_template="campaign:{session_id}"))
set_label("campaign.email_suppressions", "Unsubscribe")
set_rels("campaign.email_suppressions",
         to("for_identity", "ContactIdentity", "uid", "email",
            to_key_template="{organization_id}:email:{email}", create_target=False))
set_rels("campaign.campaign_send_recipients")
set_coll("campaign.campaign_send_recipients", coll("DeliveryHistory", "contact_id", "created_at"),
         coll("CampaignDeliveryHistory", "campaign_id", "created_at"))
set_coll("campaign.website_contacts")
set_rels("campaign.website_contacts", frm("has", "Contact", "cdp_id", "contact_id"))
set_coll("campaign.campaign_contact_engagement")
set_rels("campaign.campaign_contact_engagement", frm("has", "Contact", "cdp_id", "contact_id"))
c = T["campaign.campaign_audience"].get("collection")
if c:
    c["name"] = "CampaignHistory"

# -- 8. stub labels ---------------------------------------------------------------------------
sl = CommentedMap()
sl["MarketingAgent"] = flow({"key": "user_id"})
doc["stub_labels"] = sl

with CORE.open("w") as f:
    yaml.dump(doc, f)
print("core.yaml rewritten:", len(T), "tables;", len(archive), "archived")
