# Phase 1.1: a deterministic pipeline (no LLM)

Postgres CDC and backfill write every tenant graph through one code path. This page is
the checklist of what that path guarantees, and how each guarantee is tested.

| Guarantee | How | Tested by |
|---|---|---|
| One product label per product node, none on setup/catalog nodes | Writer removes other product labels and sets its own (none for setup/catalog); stubs get one only if they have none | `tests/test_engine_graph.py::test_every_node_gets_exactly_one_product_label`, `integrity: product_labels` |
| Agents never read another product | Read rule: no product label, or an allowed one; enforced by `scoped_read` | `main.py validate`, `integrity: cross_product_edges`, `test_reset`/`test_v43` grant tests |
| One-to-many from a contact goes through one collection per kind | `collection:` on the item table, `owns_collections:` on the owner | `test_contact_reaches_items_through_one_collection_per_product` |
| Collections follow their items and never go stale | Re-parent detaches from the old collection; empty collections/buckets deleted; owner delete removes its collections | `test_item_moving_to_another_owner…`, `test_deleting_…`, `integrity: collections_*` |
| Messages only under conversations | `ccass.conversation_messages` → `(Conversation)-[:HAS_MESSAGE]->(Message)`, attachments under messages | `test_messages_connect_only_to_their_conversation` |
| Edges follow their columns | Old edge removed when the FK changes or is cleared | `test_reassigned_ticket…`, `test_clearing_a_foreign_key…`, `integrity: single_value_edges` |
| Order never matters | Stub targets; per-key version guard; backfill in dependency order | `test_child_before_parent…`, `test_shuffled_and_replayed_events_give_an_identical_graph` (6 seeds) |
| Older events never overwrite newer | `_source_version` guard; backfill stamped with the DB clock at read time | `test_older_event_never_overwrites_newer`, `test_live_change_newer_than_the_backfill_read_wins` |
| Labels written by two products never collide | Product-prefixed keys (`support:5`, `service:5`), enforced by the validator | `test_ticketing_and_contact_centre_queues_are_separate_nodes` |
| Deletes leave nothing behind | Cascading delete query; reconcile after backfill (graph read first, then Postgres) | engine delete tests, `integrity` |
| Start clean on demand | `main.py reset --all / --org` | `tests/test_reset.py` |
| Start from now | `run --from-now` jumps every partition to its end and commits it | manual (needs Kafka) |

## Fresh start

```bash
python main.py validate
python main.py reset --all
./run_pipeline.sh --from-now
python main.py backfill --all          # or --org 1012
python main.py integrity
./run_embedder.sh --mark-missing
```

## Not covered by the automated tests
- Kafka itself (`--from-now` offset jump) and real Postgres: run `scripts/consumer_selftest.py` and a
  backfill of one test tenant, then `main.py integrity`.
- Neo4j-specific syntax: the engine tests run the "legacy" subquery form on an embedded openCypher
  engine. Production on Neo4j 5.23+ uses the scoped form of the same queries; run
  `main.py dry-run --org org1000` and `main.py provision --all` against Neo4j once before the backfill.

## Next: Phase 1.2
LLM conversation intake (E2), the ontology-by-module endpoint (E1) and contact 360 (E3), on top of this graph.
