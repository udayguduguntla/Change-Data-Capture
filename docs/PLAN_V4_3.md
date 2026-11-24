# v4.3 plan: lean nodes, product scopes, reliable deletes, model switching

> **v4.4:** hub nodes were removed and every node now carries exactly one product label; contacts reach their data through collection nodes. Where this document mentions hubs or several scope labels per node, README section 5 is authoritative.


## Goals

1. A node holds its business properties and nothing the pipeline could keep elsewhere.
2. The tenant graph holds only records and hubs. No schema catalog inside it.
3. Every node says which products may see it, so an agent can stay inside its product.
4. A row deleted in Postgres is never visible in the graph.
5. The embedding model can be changed without breaking search, and old and new vectors are never mixed.

## 1. Node shape

| Kept on a node | Why |
|---|---|
| business properties | the row's columns, as mapped in `core.yaml` |
| `organization_id` | tenant separation; the only one on a shared Community database |
| `_source_version` | an older change event can never overwrite a newer one; also guards the embedder |
| `_embedding` | the vector, on embedded labels only |
| `_embed_pending` | only while waiting; removed in the same write that stores the vector |

| Removed | Where it went |
|---|---|
| `_embed_model` | once per database, `PipelineState.embed_model` |
| `_owner` | nowhere: never read, and wrong for merged nodes |
| `_deleted_at` | nowhere: soft-delete mode is gone |
| `<field>_scale` | `scale:` on the property in `core.yaml` (same for every row) |
| `_embed_text` | nowhere: text is rebuilt from properties (v4.2) |
| `Tenant._schema_fingerprint` | `PipelineState.schema_fingerprint` |

**Labels:** exactly one entity label (`Ticket`), any state labels (`OpenTicket`), one scope label per product (`Support`).

**`PipelineState`:** one node per tenant database, stored in the metadata database (`neo4j`), never in the tenant graph. It holds `schema_fingerprint`, `embed_model`, `embed_dims`, `embed_next_model`, `embed_next_dims` and `embed_fields`. On Community, where everything shares one database, it sits beside the data but has no edges and no vector.

## 2. Catalog removed from the graph

The `EntityType` nodes had their own embeddings, so vector search for a question could return the schema description of `Ticket` instead of a ticket, and agents answered from it. They are deleted, with their vector index, pending index, constraint and stub declaration. `src/catalog.py` and `config/catalog/hubs.yaml` are deleted.

Hubs stay as product entry points (57 relationships in `core.yaml` hang off them), with only `name`, `organization_id` and their scope label.

`config/catalog/column_descriptions.json` and `scripts/build_column_catalog.py` are no longer read by the pipeline. They are kept as documentation for Meilisearch or agent prompts.

## 3. Product scopes

**Vocabulary** (`config/scopes.yaml`), one scope per hub:

| Scope | `access.domain` | Hub |
|---|---|---|
| Setup | common | SetupHub |
| Support | ticketing | SupportHub |
| Sales | crm | SalesHub |
| Marketing | campaign | MarketingHub |
| ContactCentre | ccaas | ContactCentreHub |
| CustomerData | cdp | CustomerDataHub |
| WebTracking | webtracking | WebTrackingHub |
| Catalog | catalog | ProductCatalog |

**Rules** (`src/scope.py`, computed once per tenant database):

- **Home scope** of a table: `access.domain`, except the overrides in `scopes.yaml`. `Contact` goes to CustomerData, and the six product and price tables go to Catalog.
- **Visible to**: the home scope, plus each product in `readable_by`, plus the home of every table merged into the same label. `Staff` is fed by ContactCentre and Support tables, so it carries both.
- **Structural nodes** take the scopes of what creates them: collections (`EventHistory`), month buckets (`LogPeriod`, per collection), promoted category nodes and stub targets.
- **Hubs** carry the scope they represent.
- **Overlays** (industry packs, org files) may add to `readable_by`, never remove from it.
- **Validation**: an unknown product in `readable_by` fails at load, and so does a scope name that collides with an entity label.

**Enforcement:**

- **Enterprise:** `python main.py scopes --grants --org-db orgX` prints one read-only role per product (`GRANT MATCH {*} ... NODES Support`). Nodes without the label are invisible to the role, even in traversals.
- **Community:** agents read through `src.scope.scoped_read`. It runs a read-only transaction and drops any record touching a node outside the scope. Queries should still put the scope label in every `MATCH`.

**Why labels, not a property, a hub traversal or a database per product:** labels are indexed, a label check is one hop, and Enterprise can grant on them. A property is slower and cannot be granted on. A hub traversal is the slow path this avoids. A database per product would cut the links between products (Ticket to Contact to Purchase).

## 4. Deletes

| Case | Behaviour |
|---|---|
| Row deleted in Postgres | node deleted with its edges and vector (`DETACH DELETE`) |
| Table that only adds properties to another table's node (`accounts.setup_email_configs`, `setup_webchat_configs`, `setup_social_configs`, `cdp.contact_identities`) | `on_delete: remove_own_properties`: removes only its properties; the node stays for its owner |
| Application soft delete | `delete_when: {deleted_at: not_null}` on the table turns that update into a delete. No core table needs it today. |
| Delete missed (pipeline down past retention, reset topic) | `python main.py reconcile-deletes --org X [--apply]` compares keys per label and deletes graph nodes whose row is gone. It skips stubs, labels whose source cannot be read, and labels with no source rows at all. It refuses a fallback/shared database. |

`on_delete: soft` is removed.

Most other merged tables use their own key prefix (`support:{id}`, `quote:{id}`), so each row is its own node and a normal delete is already correct.

## 5. Embeddings

- The router sets only `_embed_pending`. The worker builds the text from the node's current properties and removes the flag when it writes the vector.
- Re-embedding happens only when a listed embed field changes. A field missing from the event because Debezium left out unchanged TOASTed text counts as unchanged.
- Packs and orgs can add fields: `extends: <table>: embed_fields: [...]`. `provision` compares each label's field list with `PipelineState.embed_fields` and re-queues only the labels that changed.
- **Model guard:** each database records its model. A worker whose model differs refuses that database and logs why. A database with no record adopts the worker's model.

**Model switch** (`src/model_switch.py`). Vectors from different models cannot be compared, so every node is re-embedded next to the old vectors:

1. `embed --switch-model M --dims D` records the next model, creates the `_embedding_next` index and sets `_embed_next_pending` on every node.
2. The old worker keeps serving live changes into `_embedding`, and forwards each change to the new model by setting `_embed_next_pending`. A second worker started with `--model M` writes `_embedding_next`.
3. `embed --switch-status` shows the nodes still waiting per label.
4. `embed --switch-finish` requires zero waiting (or `--force`). It drops the old index, moves `_embedding_next` into `_embedding` and re-queues anything left over. It also drops any vector of the old size, then rebuilds the index and records the new model as active.
5. `embed --switch-abort` removes everything the switch created; the old model stays active.

`embed --truncate-dims N` shortens stored vectors of a Matryoshka-trained model (Qwen3-Embedding, EmbeddingGemma) and re-normalises them, with no model run. It is refused for models that aren't trained that way, such as bge.

`provision` sizes vector indexes from `PipelineState.embed_dims`, so after a switch it never drops a correct index because `connections.yaml` is stale.

## 6. Rollout

1. Stop the pipeline and the embedder. Update the code.
2. `python main.py migrate --dry-run` prints counts per tenant.
3. `python main.py migrate` (or `--org X`). Per tenant, it:
   - records the model from the old `_embed_model` tags and re-queues any minority-model vectors
   - deletes the catalog
   - strips the removed properties
   - trims the hubs
   - writes scope labels, removing scopes a label no longer has
   - re-provisions the tenant

   It is idempotent and safe to rerun.
4. Start the pipeline and the embedder.
5. `python main.py scopes --scope Support` spot-checks what each product sees.
6. Optional: schedule `reconcile-deletes --apply` nightly after reviewing a dry run.

## 7. Decisions taken, to review

1. **`access.domain` wins over `product:`** on the 18 tables where they disagree:

   | Table | Label | `product:` | `access.domain` |
   |---|---|---|---|
   | `accounts.setup_voice_dids` | PhoneNumber | ccaas | common |
   | `accounts.setup_voice_prompt_profiles` | VoicePromptSet | ccaas | common |
   | `accounts.setup_ccaas_queues` | QueueSetup | ccaas | common |
   | `accounts.setup_ccaas_queue_agents` | (edge) | ccaas | common |
   | `accounts.setup_queue_routing_profiles` | RoutingProfile | ccaas | common |
   | `cdp.identity_keys` | ContactIdentity | platform | cdp |
   | `cdp.contact_identities` | ContactIdentity | platform | cdp |
   | `cdp.identity_links` | (edge) | platform | cdp |
   | `ccass.agents` | Staff | platform | ccaas |
   | `ccass.sla_policies` | SLA | platform | ccaas |
   | `crm.notifications` | Notification | platform | crm |
   | `ticketing.agents` | Staff | platform | ticketing |
   | `ticketing.sla_policies` | SLA | platform | ticketing |
   | `ticketing.notifications` | Notification | platform | ticketing |
   | `campaign.branches` | Branch | platform | campaign |
   | `campaign.organization_products` | Subscription | platform | campaign |
   | `campaign.tenant_industry_packs` | IndustryPack | platform | campaign |
   | `campaign.app_notifications` | Notification | platform | campaign |

   The first five mean ContactCentre agents do not see phone numbers, voice prompts, queue setups or routing profiles. Add `readable_by: [ccaas]` to those tables if they should.

2. **Visible only to Setup:** Employee, Team, Role, Tenant, PhoneNumber, QueueSetup, VoicePromptSet. Widen with `readable_by` where agents need them.
3. **Neo4j edition:** both Enterprise grants and the Community filter are provided.
4. `product:` stays in config for product entitlement (which tables a tenant gets); it no longer affects scope.

## 8. Not verified yet

- No run against a live Neo4j. The generated Cypher was rendered for all 178 tables without errors, and 142 unit tests pass.
- The scoped `CALL (n) { ... } IN TRANSACTIONS` syntax needs Neo4j 5.23 or later. Earlier worker code already used it.
- The first test on real data should be `migrate --dry-run`, then `reconcile-deletes` without `--apply`.

## 9. Files

New: `config/scopes.yaml`, `src/scope.py`, `src/graph_state.py`, `src/model_switch.py`, `src/migrate.py`, `src/reconcile.py`, `tests/test_v43.py`, `docs/PLAN_V4_3.md`.
Changed: `main.py`, `src/config_loader.py`, `src/cypher_builder.py`, `src/router.py`, `src/embedding_worker.py`, `src/provision.py`, `config/mappings/core.yaml`, `config/connections.yaml`, `tests/test_ontology.py`, `tests/test_cypher_builder.py`, `README.md`, `SETUP.md`, `docs/UPGRADE_V4.md`, `docs/fgac_example.md`, `docs/RBAC_NODE_MAPPING.md`.
Deleted: `src/catalog.py`, `config/catalog/hubs.yaml`.
