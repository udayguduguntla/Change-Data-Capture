# Upgrading an existing installation to this version

## To v4.5 (CDP label, unlabelled setup/catalog, user roles, HAS, leaner graph)
Rebuild: `validate`, `reset --all`, `run_pipeline.sh --from-now`, `backfill --all`, `integrity`.

| v4.4 | v4.5 |
|---|---|
| `CustomerData` | `CDP` |
| `Shared` label on Contact, User, Team, Channel, Product, … | Contact is `CDP`; setup/catalog nodes carry **no** product label |
| `Employee` | `User` |
| `Staff` (`support:`/`service:` keys) | `SupportAgent` / `ContactCentreAgent` (plain ids), reached from `(User)-[:ACTS_AS]->` |
| CRM/Campaign edges to `Employee` | to `SalesRep` / `MarketingAgent` (keyed by user id), each linked `(User)-[:ACTS_AS]->` |
| `Queue`, `SLA` in two products | `SupportQueue`, `ContactCentreQueue`, `SupportSLA`, `ContactCentreSLA` |
| `ConnectedChannel`; `setup_channel_configs` not written | `Channel`, written; `(Conversation)-[:VIA_CHANNEL]->(Channel)`; `(Channel)-[:ROUTES_TO]->(ContactCentreQueue)` |
| `QueueSetup` + `Queue` (same queue twice) | one `ContactCentreQueue` (carries `setup_queue_id`) |
| `HAS_TICKET_HISTORY`, `HAS_TICKET`, `HAS_NOTE`, `SUMMARY_OF`, … | `HAS` for all containment |
| `IdentitySet` | `ContactProfile` |
| `CampaignDraft`, `AssistantSession`, `LiveMeeting` | `Session` with product label (`Marketing:Session`, `Support:Session`, `Sales:Session`) |
| `Message`, `Attachment` nodes | removed (the conversation summary carries the meaning) |
| `MissedConversation`, `BlockedEmail`, `KnowledgeFile` | `MissedContact`, `Unsubscribe`, `KnowledgeDocument` |
| 179 tables mapped | 90 (89 archived in `config/archive/dropped_from_graph_v45.yaml`) |
| `scopes --grants` | removed; scope is enforced in the read path |
| `ConversationSummary` from `cdp.interactions` | the conversation's own `summary` / `facts` (from `ccass.conversation_memory`); conversations are embedded |
| CDP `Event` / `Session` | `WebTracking:Event` / `WebTracking:Session` (keys `web:<id>`); `(Session)-[:FROM_CAMPAIGN]->(Campaign)` |
| `cdp.order_items` dropped | `(Product)-[:HAS]->(ProductPurchaseHistory)-[:HAS {quantity, unit_price, subtotal}]->(Purchase)`, product found by sku |
| campaign rows keyed by a bigint contact id | `Contact.campaign_contact_id` (Campaign's projection of `cdp_id`); campaign rows match it and never create contacts |
| `(Conversation)-[:VIA_CHANNEL]->` only by `channel_config_id` | also by web-chat widget id and email mailbox connection |
| deliveries under a campaign month collection | `(Delivery)-[:FOR_CAMPAIGN]->(Campaign)` |
| tickets had no company | `(Ticket)-[:FOR_COMPANY]->(SupportCompany)` via the ticketing customer record |
| macros dropped | `(Tenant)-[:HAS]->(MacroLibrary)-[:HAS]->(Macro)-[:CREATED_BY]->(SupportAgent)` |
| tasks / notes linked to `User` | `ASSIGNED_TO` / `WRITTEN_BY` the `ContactCentreAgent` (matched by `setup_user_id`) |
| alerts, CDP interaction copies, journey steps, semantic memory, visitor copies, suppressions, automation rules | out of the graph (90 tables mapped, 89 archived) |


## To v4.4 (one product label per node, collections, no hubs)
The graph shape changed, so rebuild instead of migrating in place:
```bash
python main.py validate
python main.py reset --all            # asks you to type DROP
./run_pipeline.sh --from-now          # terminal 1
python main.py backfill --all         # terminal 2
python main.py integrity              # every line 0
./run_embedder.sh --mark-missing
```

What changed for agents and saved queries:

| Before (v4.3) | Now (v4.4) |
|---|---|
| Several scope labels on a node (`:Contact:CustomerData:Support:Sales:…`) | Exactly one product label (`:Contact:Shared`, `:Ticket:Support`) |
| Hub nodes (`SupportHub`, `SalesHub`, …) and edges like `SetupHub-HAS_EMPLOYEE->` | Removed |
| `(:Ticket)-[:RAISED_BY]->(:Contact)`, `(:Deal)-[:PRIMARY_CONTACT]->(:Contact)` and other direct contact edges | `(:Contact)-[:HAS_TICKET_HISTORY]->(:TicketHistory)-[:HAS_TICKET]->(:Ticket)`, `…-[:HAS_DEAL_HISTORY]->(:DealHistory)-[:HAS_DEAL]->(:Deal)` |
| `(:Contact)-[:IDENTIFIED_BY]->(:ContactIdentity)` | `(:Contact)-[:HAS_IDENTITY_SET]->(:IdentitySet)-[:HAS_CONTACT_IDENTITY]->(:ContactIdentity)` |
| `ConversationHistory` with month buckets | Conversations directly under `ConversationHistory` |
| Collection item edge `CONTAINS` | `HAS_<ITEM>` (`HAS_TICKET`, `HAS_EVENT`, …) |
| Campaign deliveries labelled `Message`, collection `MessageHistory` | `Delivery` in `DeliveryHistory`; `Message` is now a conversation message |
| No conversation messages in the graph | `(:Conversation)-[:HAS_MESSAGE]->(:Message)` (metadata; body stays in Postgres) |
| Ticket audit collection `TicketHistory` | `TicketAuditTrail` (`TicketHistory` now holds a contact's tickets) |
| `Queue` keyed by plain `id` (ticketing and contact-centre queues collided) | `uid` = `support:<id>` / `service:<id>` |
| `ActivityHistory`, `InsightSet`, `Inbox`, `TouchpointHistory` mixed products | One per product: `SalesActivityHistory`/`CdpActivityHistory`, `SalesInsightSet`/`InsightSet`, `ContactCentreInbox`/`SalesInbox`, `ConversationSummaryHistory` |
| Old edge kept when a foreign key changed | Old edge removed |
| Edge skipped when its target had not arrived | Target created as a stub (`_stub_since`), filled in later |
| `main.py migrate` | Removed; use reset + backfill |


## To v4.3 (lean nodes, product scopes)
Stop the pipeline and the embedder, update the code, then run once:
```bash
python main.py migrate --dry-run
python main.py migrate
```
What changed for agents and saved queries:

| Before | Now |
|---|---|
| `(:EntityType)` catalog nodes and `DESCRIBES` / `LINKS_TO` edges | gone; the graph holds only records and hubs |
| hub `description`, `domain`, `entity_types` | hubs carry `name` and a scope label only |
| `_embed_model`, `_owner`, `_deleted_at`, `<field>_scale` on nodes | gone; the model is recorded once per database in `PipelineState` (metadata db); money scales are in `core.yaml` |
| `_schema_fingerprint` on `Tenant` | in `PipelineState` |
| `WHERE n._embed_model = ...` to filter search | not needed: a database never mixes models |
| `on_delete: soft` | removed; deletes always delete. Use `delete_when:` for application soft deletes |
| no product scope | every node has scope labels, e.g. `:Ticket:Support` |

Details: `docs/PLAN_V4_3.md`.

## Before you start
1. **Fix `.env`.** Set `EMBEDDING_MODEL=BAAI/bge-large-en-v1.5`, or delete the line. Environment variables override `connections.yaml`.
2. **Rotate the Neo4j, PostgreSQL and Meilisearch passwords.** They were in the shared zip and in the old scripts.
3. **Stop** `run_pipeline.sh` and any old embedding process.

## Breaking changes (update agents, saved queries and Meilisearch documents)

| Before | Now |
|---|---|
| `(:Customer)` for every person | `(:Contact)` plus exactly one of `:Lead` / `:Prospect` / `:Customer` |
| CustomerIdentity, CustomerInsight, CustomerScore, CustomerValue, CustomerFact, CustomerActivity | Contact... equivalents |
| lower-case relationship types (`raised_by`) | UPPER_SNAKE_CASE (`RAISED_BY`) |
| `PendingTicket` | `InProgressTicket` |
| `PageView`, `Interaction`, `Conversion` (events) | `PageViewEvent`, `InteractionEvent`, `ConversionEvent` |
| industry synonym and state labels (Patient, Learner, ActivePolicy, Discipline, ...) | removed; one core name each |
| `Branch`, `ConnectedChannel` from setup_channel_configs, `IN_BRANCH`, channel_config joins | removed |
| collections not linked to their owner | `HAS_EVENT_HISTORY`, `HAS_TICKET_HISTORY`, ... from the owner |
| `LogPeriod.uid = owner:month` | `LogPeriod.uid = Collection:owner:month` |
| no graph self-description | `EntityType` catalog under every hub, plus the new `SalesHub` |

## Rebuild
```bash
python main.py validate
python scripts/diagnose_pipeline.py     # if the oldest Kafka offsets are gone, use backfill
python scripts/reset_graph.py           # wipe the tenant graphs, reset the consumer group
python main.py provision --all
./run_pipeline.sh                       # terminal 1
./run_embedder.sh --mark-missing        # terminal 2
python main.py backfill --org <id>      # only for tables whose history expired from Kafka
```

## Removed from the repository
- **Root debug scripts** (`check_*`, `query_*`, `count_*` and similar), `scripts/sandbox_tools/`, `offset_4981.json` and the dummy producer. These were one-off debugging tools with hard-coded hosts and passwords.
- **Superseded scripts:** `migrate_embeddings.py`, `migrate_all.py`, `drop_vector_indexes.py` and `wait_and_compile.py` are replaced by the embedding worker and provisioning. `migrate_labels_v2.py` and `patch_overlays.py` were one-off v2 migrations. `src/async_embedder.py` is also gone.
- **One-off DDL:** `create_table.py` and `update_schema.py`. The DLQ consumer creates its own table.
- **98 tests** written for tables and labels that no longer exist. They are replaced by `tests/test_ontology.py`.
- **Moved:** the old SETUP, TROUBLESHOOTING and TRACEABILITY docs are now in `docs/history/`, and the old core backup is in `config/archive/`.
