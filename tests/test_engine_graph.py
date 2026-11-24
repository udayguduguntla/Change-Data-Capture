"""What the generated Cypher actually does to a graph (see tests/engine.py).

Every test writes rows through the real router and builder and inspects the result.
"""
import random

import pytest

from tests.engine import Engine

ORG = 1000


def contact(E, cid, ts=1, **kw):
    E.upsert("cdp.customer_profile", ts=ts, cdp_id=cid, organization_id=ORG, **kw)


def ticket(E, tid, cid, ts=1, **kw):
    row = dict(id=tid, organization_id=ORG, customer_cdp_id=cid, status="open",
               subject="s", priority="high")
    row.update(kw)
    E.upsert("ticketing.tickets", ts=ts, **row)


def edges(E, rel_type=None):
    q = ("MATCH (a)-[r]->(b) RETURN labels(a), coalesce(a.uid, a.id, a.cdp_id), type(r), "
         "labels(b), coalesce(b.uid, b.id, b.cdp_id)")
    out = []
    for la, ka, t, lb, kb in E.rows(q):
        if rel_type is None or t == rel_type:
            out.append((t, str(ka), str(kb)))
    return sorted(out)


# -- product labels ------------------------------------------------------------------

def products_of(E, label, key, value):
    return E.labels(label, key, value) & set(E.eff.all_scopes)


def test_product_nodes_get_one_product_label_setup_nodes_none():
    E = Engine()
    contact(E, "C1")
    ticket(E, 10, "C1", assignee_id=5, queue_id=3)
    E.upsert("crm.deals", ts=1, id=7, organization_id=ORG, title="d", stage="open", contact_id="C1",
             owner_user_id=42)
    E.upsert("ccass.conversations", ts=1, id="v1", organization_id=ORG, channel="chat",
             contact_id="C1", status="active")
    E.upsert("accounts.setup_users", ts=1, id=42, organization_id=ORG, first_name="Ria")
    assert E.product_label_problems() == []
    assert products_of(E, "Contact", "cdp_id", "C1") == {"CDP"}
    assert products_of(E, "Ticket", "id", 10) == {"Support"}
    assert products_of(E, "Deal", "id", 7) == {"Sales"}
    assert products_of(E, "Conversation", "id", "v1") == {"ContactCentre"}
    assert products_of(E, "SupportAgent", "id", 5) == {"Support"}      # stub target, right product
    assert products_of(E, "User", "id", 42) == set()                    # setup: no product label


def test_unscoped_stub_never_gets_a_product_label():
    E = Engine()
    E.upsert("crm.quote_lines", ts=1, id=1, organization_id=ORG, quote_id=9, product_id=77)
    assert products_of(E, "Product", "id", 77) == set()
    assert products_of(E, "LineItem", "id", 1) == {"Sales"}


def test_owner_row_fixes_a_stub_created_with_a_wrong_product():
    E = Engine()
    E.g.query("CREATE (:Contact:Sales {cdp_id: 'C9'})")               # a wrong guess
    contact(E, "C9")
    assert products_of(E, "Contact", "cdp_id", "C9") == {"CDP"}


def test_support_and_contact_centre_queues_are_different_labels():
    E = Engine()
    E.upsert("ticketing.agent_queues", ts=1, id=5, organization_id=ORG, queue_name="T")
    E.upsert("ccass.queues", ts=1, id=5, organization_id=ORG, name="C", setup_queue_id=50)
    assert products_of(E, "SupportQueue", "id", 5) == {"Support"}
    assert products_of(E, "ContactCentreQueue", "id", 5) == {"ContactCentre"}


# -- users acting in products -----------------------------------------------------------

def test_a_user_acts_as_an_agent_in_each_product():
    E = Engine()
    E.upsert("accounts.setup_users", ts=1, id=42, organization_id=ORG, first_name="Ria")
    E.upsert("ccass.agents", ts=1, id=900, organization_id=ORG, setup_user_id=42, display_name="Ria")
    E.upsert("ticketing.agents", ts=1, id=500, organization_id=ORG, setup_user_id=42)
    E.upsert("crm.deals", ts=1, id=7, organization_id=ORG, title="d", owner_user_id=42)
    E.upsert("campaign.campaigns", ts=1, id=3, organization_id=ORG, name="Spring", created_by_user_id=42)
    roles = sorted(r[0] for r in E.rows(
        "MATCH (:User {id: 42})-[:ACTS_AS]->(r) RETURN [l IN labels(r) WHERE l ENDS WITH 'Agent' "
        "OR l = 'SalesRep'][0]"))
    assert roles == ["ContactCentreAgent", "MarketingAgent", "SalesRep", "SupportAgent"]
    assert E.count("(:Deal {id: 7})-[:OWNED_BY]->(:SalesRep {user_id: 42})") == 1
    assert E.count("(:Campaign {id: 3})-[:CREATED_BY]->(:MarketingAgent {user_id: 42})") == 1
    assert E.count("(:User)") == 1                                   # one person, four roles


# -- collections -----------------------------------------------------------------------

def test_contact_reaches_items_through_collections_with_has():
    E = Engine()
    contact(E, "C1")
    ticket(E, 10, "C1")
    ticket(E, 11, "C1")
    E.upsert("crm.deals", ts=1, id=7, organization_id=ORG, title="d", contact_id="C1")
    hop1 = sorted((r[0], r[1]) for r in E.rows(
        "MATCH (:Contact {cdp_id: 'C1'})-[r]->(c) RETURN type(r), c.uid"))
    assert hop1 == [("HAS", "DealHistory:C1"), ("HAS", "TicketHistory:C1")]
    assert E.count("(:TicketHistory {uid: 'TicketHistory:C1'})-[:HAS]->(:Ticket)") == 2
    assert E.count("(:Contact)-[]->(:Ticket)") == 0                   # never a direct edge


def test_both_identity_tables_fill_one_contact_profile():
    E = Engine()
    contact(E, "C1")
    E.upsert("cdp.identity_keys", ts=1, organization_id=ORG, key_type="email",
             key_value="a@x.io", cdp_id="C1")
    E.upsert("cdp.contact_identities", ts=1, id=1, organization_id=ORG, contact_id="C1",
             identity_type="phone", identity_value="+44")
    assert E.count("(:ContactProfile)") == 1
    assert E.count("(:Contact {cdp_id: 'C1'})-[:HAS]->(:ContactProfile)-[:HAS]->(:ContactIdentity)") == 2


def test_item_moving_to_another_owner_leaves_no_empty_collection():
    E = Engine()
    ticket(E, 10, "C1", ts=1)
    ticket(E, 10, "C2", ts=2)
    assert E.rows("MATCH (c:TicketHistory) RETURN c.uid") == [["TicketHistory:C2"]]
    assert edges(E, "HAS") == [("HAS", "C2", "TicketHistory:C2"), ("HAS", "TicketHistory:C2", "10")]


def test_item_losing_its_owner_is_detached():
    E = Engine()
    ticket(E, 10, "C1", ts=1)
    ticket(E, 10, None, ts=2)
    assert E.count("(:TicketHistory)") == 0
    assert E.count("(:Ticket {id: 10})") == 1


def test_deleting_an_item_keeps_a_collection_that_still_holds_others():
    E = Engine()
    ticket(E, 10, "C1")
    ticket(E, 11, "C1")
    E.delete("ticketing.tickets", id=10, organization_id=ORG)
    assert E.count("(:TicketHistory)-[:HAS]->(:Ticket)") == 1
    E.delete("ticketing.tickets", id=11, organization_id=ORG)
    assert E.count("(:TicketHistory)") == 0


def test_deleting_the_owner_removes_its_collections_but_not_the_items():
    E = Engine()
    contact(E, "C1")
    ticket(E, 10, "C1")
    E.delete("cdp.customer_profile", cdp_id="C1", organization_id=ORG)
    assert E.count("(:Contact)") == 0
    assert E.count("(:TicketHistory)") == 0
    assert E.count("(:Ticket {id: 10})") == 1


def test_high_volume_streams_use_month_buckets_and_clean_up():
    E = Engine()
    for i, day in enumerate(["2026-01-05", "2026-01-20", "2026-02-01"]):
        E.upsert("cdp.customer_events", ts=1, event_id=f"e{i}", cdp_id="C1", organization_id=ORG,
                 event_name="view", occurred_at=f"{day}T10:00:00")
    periods = sorted(r[0] for r in E.rows("MATCH (:EventHistory)-[:HAS_PERIOD]->(p) RETURN p.period"))
    assert periods == ["2026-01", "2026-02"]
    E.delete("cdp.customer_events", event_id="e2", organization_id=ORG)
    assert sorted(r[0] for r in E.rows("MATCH (p:LogPeriod) RETURN p.period")) == ["2026-01"]
    for i in (0, 1):
        E.delete("cdp.customer_events", event_id=f"e{i}", organization_id=ORG)
    assert E.count("(:LogPeriod)") == 0 and E.count("(:EventHistory)") == 0


def test_a_delivery_sits_in_its_contact_and_its_campaign_history():
    E = Engine()
    contact(E, "12345")                                   # campaign contact id 12345
    E.upsert("campaign.campaign_send_recipients", ts=1, id=1, organization_id=ORG, contact_id=12345,
             campaign_id=3, status="sent", created_at="2026-03-02T09:00:00")
    assert E.count("(:Contact {cdp_id: '12345'})-[:HAS]->(:DeliveryHistory)-[:HAS_PERIOD]->()-[:HAS]->(:Delivery)") == 1
    assert E.count("(:Delivery)-[:FOR_CAMPAIGN]->(:Campaign {id: 3})") == 1        # one hop to its campaign


def test_conversation_links_its_channel_and_agent_and_carries_its_summary():
    E = Engine()
    E.upsert("accounts.setup_channel_configs", ts=1, id=4, organization_id=ORG, channel_type="webchat")
    E.upsert("ccass.conversations", ts=1, id="v1", organization_id=ORG, channel="chat", contact_id="C1",
             channel_config_id=4, assigned_agent_id=900, subject="refund")
    E.upsert("ccass.conversation_memory", ts=2, id=1, organization_id=ORG, conversation_id="v1",
             rolling_summary="asked about refunds", extracted_facts="[]")
    assert E.count("(:ConversationHistory)-[:HAS]->(:Conversation {id: 'v1'})") == 1
    assert E.count("(:Conversation {id: 'v1'})-[:VIA_CHANNEL]->(:Channel {id: 4})") == 1
    assert E.count("(:Conversation {id: 'v1'})-[:HANDLED_BY]->(:ContactCentreAgent {id: 900})") == 1
    assert E.rows("MATCH (c:Conversation {id: 'v1'}) RETURN c.summary") == [["asked about refunds"]]
    assert E.count("(:ConversationSummary)") == 0                      # one conversation, one node
    E.delete("ccass.conversation_memory", id=1, organization_id=ORG, conversation_id="v1")
    assert E.rows("MATCH (c:Conversation {id: 'v1'}) RETURN c.summary, c.subject") == [[None, "refund"]]


def test_web_behaviour_collected_in_cdp_is_web_tracking():
    E = Engine()
    E.upsert("cdp.customer_sessions", ts=1, session_id="s1", cdp_id="C1", organization_id=ORG,
             started_at="2026-01-05T10:00:00", site_id=7)
    E.upsert("cdp.customer_events", ts=1, event_id="e1", cdp_id="C1", organization_id=ORG,
             session_id="s1", occurred_at="2026-01-05T10:01:00", site_id=7)
    assert products_of(E, "Session", "uid", "web:s1") == {"WebTracking"}
    assert products_of(E, "Event", "event_id", "e1") == {"WebTracking"}
    assert E.count("(:Session {uid: 'web:s1'})-[:HAS]->(:Event {event_id: 'e1'})") == 1
    assert E.count("(:Contact {cdp_id: 'C1'})-[:HAS]->(:EventHistory:WebTracking)") == 1


def test_small_memberships_are_direct_campaigns_use_a_history():
    E = Engine()
    E.upsert("cdp.segments", ts=1, segment_id="s1", organization_id=ORG, segment_name="VIP")
    E.upsert("cdp.customer_segments", ts=1, cdp_id="C1", segment_id="s1", organization_id=ORG)
    assert E.count("(:Contact {cdp_id: 'C1'})-[:IN_SEGMENT]->(:Segment {segment_id: 's1'})") == 1
    contact(E, "12345")
    E.upsert("campaign.campaign_audience", ts=1, contact_id=12345, campaign_id=3, organization_id=ORG)
    assert E.count("(:Contact {cdp_id: '12345'})-[:HAS]->(:CampaignHistory)-[:TARGETED_BY]->(:Campaign)") == 1
    E.delete("campaign.campaign_audience", contact_id=12345, campaign_id=3, organization_id=ORG)
    assert E.count("(:CampaignHistory)") == 0


# -- edges follow their columns --------------------------------------------------------

def test_reassigned_ticket_keeps_only_the_new_assignee():
    E = Engine()
    ticket(E, 10, "C1", ts=1, assignee_id=5)
    ticket(E, 10, "C1", ts=2, assignee_id=6)
    assert edges(E, "ASSIGNED_TO") == [("ASSIGNED_TO", "10", "6")]


def test_clearing_a_foreign_key_removes_its_edge():
    E = Engine()
    ticket(E, 10, "C1", ts=1, queue_id=3)
    ticket(E, 10, "C1", ts=2, queue_id=None)
    assert edges(E, "ROUTED_TO") == []


def test_older_event_never_overwrites_newer():
    E = Engine()
    ticket(E, 10, "C1", ts=5, assignee_id=6, status="resolved")
    ticket(E, 10, "C1", ts=3, assignee_id=5, status="open")
    assert edges(E, "ASSIGNED_TO") == [("ASSIGNED_TO", "10", "6")]
    assert "ResolvedTicket" in E.labels("Ticket", "id", 10)


# -- order and replay -----------------------------------------------------------------

def test_child_before_parent_gives_the_same_graph():
    a, b = Engine(), Engine()
    contact(a, "C1", first_name="Ana")
    ticket(a, 10, "C1", assignee_id=5)
    ticket(b, 10, "C1", assignee_id=5)
    contact(b, "C1", first_name="Ana")
    assert a.snapshot() == b.snapshot()


def _log():
    """A small event history for several keys (per-key order matters)."""
    return {
        ("contact", "C1"): [("cdp.customer_profile", "upsert", 1, dict(cdp_id="C1", first_name="A")),
                            ("cdp.customer_profile", "upsert", 4, dict(cdp_id="C1", first_name="B"))],
        ("contact", "C2"): [("cdp.customer_profile", "upsert", 2, dict(cdp_id="C2"))],
        ("ticket", 10): [("ticketing.tickets", "upsert", 1, dict(id=10, customer_cdp_id="C1", assignee_id=5, status="open")),
                         ("ticketing.tickets", "upsert", 3, dict(id=10, customer_cdp_id="C2", assignee_id=6, status="pending")),
                         ("ticketing.tickets", "upsert", 6, dict(id=10, customer_cdp_id="C2", assignee_id=None, status="resolved"))],
        ("ticket", 11): [("ticketing.tickets", "upsert", 2, dict(id=11, customer_cdp_id="C1", status="open")),
                         ("ticketing.tickets", "delete", 5, dict(id=11))],
        ("deal", 7): [("crm.deals", "upsert", 1, dict(id=7, contact_id="C1", title="d")),
                      ("crm.deals", "upsert", 2, dict(id=7, contact_id="C2", title="d"))],
        ("event", "e1"): [("cdp.customer_events", "upsert", 1, dict(event_id="e1", cdp_id="C1", occurred_at="2026-01-01T00:00:00"))],
    }


def _play(E, sequence):
    for table, op, ts, row in sequence:
        E.write(table, dict(row, organization_id=ORG), op, ts)


def _in_order():
    return [ev for evs in _log().values() for ev in sorted(evs, key=lambda x: x[2])]


@pytest.mark.parametrize("seed", range(6))
def test_shuffled_and_replayed_events_give_an_identical_graph(seed):
    """Kafka keeps order per key only. Interleave keys randomly and replay a suffix of
    each key's events (at-least-once delivery after a restart): the graph must match."""
    rnd = random.Random(seed)
    queues = {k: list(v) + list(v)[rnd.randint(0, len(v) - 1):] for k, v in _log().items()}
    mixed = []
    while any(queues.values()):
        k = rnd.choice([k for k, q in queues.items() if q])
        mixed.append(queues[k].pop(0))
    reference, candidate = Engine(), Engine()
    _play(reference, _in_order())
    _play(candidate, mixed)
    assert candidate.snapshot() == reference.snapshot()
    assert candidate.product_label_problems() == []


# -- review round 3: relations of kept nodes, macros, order lines, campaign contact ids ----

def test_contact_centre_work_hangs_off_the_conversation_and_names_the_agent():
    E = Engine()
    E.upsert("ccass.agents", ts=1, id=900, organization_id=ORG, setup_user_id=42)
    E.upsert("ccass.agents", ts=1, id=901, organization_id=ORG, setup_user_id=43)
    E.upsert("ccass.conversations", ts=1, id="v1", organization_id=ORG, channel="chat", contact_id="C1")
    E.upsert("ccass.agent_tasks", ts=1, id=1, organization_id=ORG, conversation_id="v1",
             assigned_user_id=42, task_type="callback")
    E.upsert("ccass.conversation_notes", ts=1, id=2, organization_id=ORG, conversation_id="v1",
             author_user_id=43, body="asked for refund")
    E.upsert("ccass.conversation_transfers", ts=1, id=3, organization_id=ORG, conversation_id="v1",
             from_agent_id=900, offered_agent_id=901, from_queue_id=5, to_queue_id=6)
    assert E.count("(:Conversation {id: 'v1'})-[:HAS]->(:Task)-[:ASSIGNED_TO]->(:ContactCentreAgent {id: 900})") == 1
    assert E.count("(:Conversation {id: 'v1'})-[:HAS]->(:InternalNote)-[:WRITTEN_BY]->(:ContactCentreAgent {id: 901})") == 1
    assert E.count("(:Transfer)-[:FROM_AGENT]->(:ContactCentreAgent {id: 900})") == 1
    assert E.count("(:Transfer)-[:OFFERED_TO]->(:ContactCentreAgent {id: 901})") == 1
    assert E.count("(:Transfer)-[:TO_QUEUE]->(:ContactCentreQueue {id: 6})") == 1


def test_macros_live_in_the_tenant_macro_library():
    E = Engine()
    E.upsert("accounts.organizations", ts=1, id=ORG, company_name="Acme")
    E.upsert("ticketing.macros", ts=1, id=1, organization_id=ORG, title="Refund steps", created_by_id=5)
    assert E.count(f"(:Tenant {{id: {ORG}}})-[:HAS]->(:MacroLibrary:Support)-[:HAS]->(:Macro)-[:CREATED_BY]->(:SupportAgent {{id: 5}})") == 1


def test_an_order_line_links_the_purchase_to_the_real_catalogue_product():
    E = Engine()
    catalog = {("cdp.products", "cdp-p1"): "SKU-1", ("inventory.products", "SKU-1"): 77}
    E.router.lookup_cache.fallback = lambda spec, value, scope: catalog.get((spec.table, value))
    E.upsert("inventory.products", ts=1, id=77, organization_id=ORG, name="Bike", sku="SKU-1")
    E.upsert("cdp.orders", ts=1, order_id="o1", cdp_id="C1", organization_id=ORG, created_at="2026-02-01T10:00:00")
    E.upsert("cdp.order_items", ts=1, order_id="o1", product_id="cdp-p1", organization_id=ORG,
             quantity=2, unit_price=50, subtotal_amount=100)
    rows = E.rows("MATCH (:Product {id: 77})-[:HAS]->(:ProductPurchaseHistory)-[r:HAS]->(:Purchase {order_id: 'o1'}) "
                  "RETURN r.quantity, r.unit_price, r.subtotal")
    assert rows == [[2, 50, 100]]
    assert E.count("(:Product)") == 1                                 # no catalogue copy, no fake product


def test_campaign_rows_find_their_contact_through_the_campaign_id_and_never_invent_one():
    E = Engine()
    contact(E, "CDP-campaign-org1012-1790413123456")
    assert E.rows("MATCH (c:Contact) RETURN c.campaign_contact_id") == [[1790413123456]]
    E.upsert("campaign.campaign_send_recipients", ts=1, id=1, organization_id=ORG, contact_id=1790413123456,
             campaign_id=3, status="sent", created_at="2026-03-02T09:00:00")
    E.upsert("campaign.campaign_contact_engagement", ts=1, id=1, organization_id=ORG,
             contact_id=1790413123456, email_open_count=4)
    E.upsert("campaign.campaign_audience", ts=1, contact_id=1790413123456, campaign_id=3, organization_id=ORG)
    c = "(:Contact {cdp_id: 'CDP-campaign-org1012-1790413123456'})"
    assert E.count(f"{c}-[:HAS]->(:DeliveryHistory)-[:HAS_PERIOD]->()-[:HAS]->(:Delivery)") == 1
    assert E.count(f"{c}-[:HAS]->(:EmailEngagement)") == 1
    assert E.count(f"{c}-[:HAS]->(:CampaignHistory)-[:TARGETED_BY]->(:Campaign)") == 1
    E.upsert("campaign.campaign_send_recipients", ts=1, id=2, organization_id=ORG, contact_id=555,
             campaign_id=3, status="sent", created_at="2026-03-02T09:00:00")
    assert E.count("(:Contact)") == 1                                 # unknown id: no fake contact


def test_campaign_contact_id_mapping_matches_campaigns_rules():
    from src.id_mapping import campaign_contact_id
    assert campaign_contact_id("12345") == 12345
    assert campaign_contact_id("CDP-campaign-org1012-1790413123456") == 1790413123456
    import hashlib
    uid = "3f0c1d2e-aaaa-bbbb-cccc-1234567890ab"
    assert campaign_contact_id(uid) == int(hashlib.sha256(f"campaign-id:{uid}".encode()).hexdigest()[:15], 16)
    assert campaign_contact_id(None) is None and campaign_contact_id("") is None


# -- review round 4: channel resolution and cross-product links ------------------------------

def test_a_conversation_finds_its_channel_even_without_channel_config_id():
    E = Engine()
    E.upsert("accounts.setup_channel_configs", ts=1, id=4, organization_id=ORG, channel_type="webchat")
    E.upsert("accounts.setup_webchat_configs", ts=1, organization_id=ORG, channel_config_id=4, webchat_id="w-abc")
    E.upsert("accounts.setup_channel_configs", ts=1, id=5, organization_id=ORG, channel_type="email")
    E.upsert("accounts.setup_email_configs", ts=1, id=1, organization_id=ORG, channel_config_id=5,
             oauth_connection_id=77)
    E.upsert("ccass.conversations", ts=1, id="chat1", organization_id=ORG, channel="webchat", contact_id="C1",
             webchat_id="w-abc")
    E.upsert("ccass.conversations", ts=1, id="mail1", organization_id=ORG, channel="email", contact_id="C1",
             mailbox_connection_id=77)
    E.upsert("ccass.conversations", ts=1, id="call1", organization_id=ORG, channel="voice", contact_id="C1")
    assert E.count("(:Conversation {id: 'chat1'})-[:VIA_CHANNEL]->(:Channel {id: 4})") == 1
    assert E.count("(:Conversation {id: 'mail1'})-[:VIA_CHANNEL]->(:Channel {id: 5})") == 1
    assert E.count("(:Conversation {id: 'call1'})-[:VIA_CHANNEL]->()") == 0      # voice: no configured channel
    assert "VoiceConversation" in E.labels("Conversation", "id", "call1")          # ...but its type is a label
    assert E.count("(:Channel)") == 2                                             # no duplicate channels


def test_a_ticket_reaches_the_customers_company():
    E = Engine()
    E.router.lookup_cache.fallback = lambda spec, value, scope: (
        {"account_id": 9, "cdp_id": "C1"}.get(spec.select) if spec.table == "ticketing.contact_profile" else None)
    E.upsert("ticketing.ticketing_accounts", ts=1, id=9, organization_id=ORG, name="Acme")
    ticket(E, 10, None, customer_id=300)
    assert E.count("(:Ticket {id: 10})-[:FOR_COMPANY]->(:SupportCompany {id: 9})") == 1


def test_a_web_visit_points_to_the_campaign_that_drove_it_without_inventing_one():
    E = Engine()
    E.upsert("campaign.campaigns", ts=1, id=3, organization_id=ORG, name="Spring")
    E.upsert("cdp.customer_sessions", ts=1, session_id="s1", cdp_id="C1", organization_id=ORG,
             started_at="2026-01-05T10:00:00", campaign_ref_id="3")
    E.upsert("cdp.customer_sessions", ts=1, session_id="s2", cdp_id="C1", organization_id=ORG,
             started_at="2026-01-05T11:00:00", campaign_ref_id="999")
    assert E.count("(:Session {uid: 'web:s1'})-[:FROM_CAMPAIGN]->(:Campaign {id: 3})") == 1
    assert E.count("(:Campaign)") == 1
