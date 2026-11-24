"""One-off, fourth review round (kept for the record): links the product questions need.

* Conversation -> Channel: channel_config_id is optional on a conversation, so it is resolved
  in order: channel_config_id, else the web-chat widget (conversation.webchat_id =
  Channel.chat_widget_id), else the mailbox (conversation.mailbox_connection_id =
  Channel.email_connection_id). Social channels only have channel_config_id; voice has no
  configured channel on the conversation (its channel type is still a label).
* Ticket -> SupportCompany: the ticketing customer record carries the company (account_id).
* Session -> Campaign: web visits carry campaign_ref_id (attribution); match only.
* Delivery -> Campaign: direct edge instead of a monthly collection under the campaign, so
  "which campaign was this" is one hop.
"""
from pathlib import Path
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

ROOT = Path(__file__).resolve().parent.parent
CORE = ROOT / "config/mappings/core.yaml"
y = YAML(); y.preserve_quotes = True; y.width = 120
doc = y.load(CORE.read_text()); T = doc["tables"]


def flow(d):
    m = CommentedMap(d); m.fa.set_flow_style(); return m


# email config: expose the mailbox connection on the Channel node
props = T["accounts.setup_email_configs"]["properties"]
if not any(isinstance(p, dict) and p.get("from") == "oauth_connection_id" for p in props):
    props.append(flow({"from": "oauth_connection_id", "as": "email_connection_id"}))

# conversation -> channel, three ways
rels = T["ccass.conversations"]["relationships"]
have = {(r.get("type"), str(r.get("match_fields"))) for r in rels}
for mf in ({"chat_widget_id": "webchat_id"}, {"email_connection_id": "mailbox_connection_id"}):
    if ("via_channel", str(mf)) not in have:
        rels.append(flow({"type": "via_channel", "to": "Channel", "match_fields": flow(mf)}))

# ticket -> the customer's company
doc["derived"]["ticketing.tickets.customer_company_id"] = flow({"lookup": flow({
    "table": "ticketing.contact_profile", "match_local": "customer_id", "match_remote": "id",
    "select": "account_id", "scope": "organization_id"})})
T["ticketing.tickets"]["relationships"].append(
    flow({"type": "for_company", "to": "SupportCompany", "to_key": "id", "from_column": "customer_company_id"}))

# web visit -> campaign (attribution), never creating a campaign
T["cdp.customer_sessions"]["relationships"].append(
    flow({"type": "from_campaign", "to": "Campaign", "to_key": "id", "from_column": "campaign_ref_id",
          "coerce_to_int": True, "create_target": False}))

# delivery -> campaign directly; the campaign keeps no monthly collection
rec = T["campaign.campaign_send_recipients"]
rec["collection"] = rec["collections"][0]
del rec["collections"]
rec["relationships"] = CommentedSeq([flow({"type": "for_campaign", "to": "Campaign", "to_key": "id",
                                           "from_column": "campaign_id"})])
if "owns_collections" in T["campaign.campaigns"]:
    del T["campaign.campaigns"]["owns_collections"]

with CORE.open("w") as f:
    y.dump(doc, f)
print("ok")
