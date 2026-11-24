# Setup and run guide

Everything you need to install, start, run in the background and look after the pipeline. For how the graph is designed, see `README.md`.

---

## 1. What runs

| Process | Command | What it does | Needs |
|---|---|---|---|
| CDC pipeline | `./run_pipeline.sh` | Reads Debezium events from Kafka and writes nodes and edges to Neo4j | Kafka, Neo4j, and Postgres for tenant lookups |
| Embedding worker | `./run_embedder.sh` | Adds vectors to nodes marked `_embed_pending` | Neo4j only |
| Backfill (on demand) | `python main.py backfill --org <id>` | Loads the current rows of a tenant straight from Postgres | Postgres, Neo4j |

The pipeline and the embedder are independent, so you can start and stop each one on its own. Run only **one** copy of each at a time.

---

## 2. Install (once per machine)

```bash
cd ~/Uday/Knowledge_Graph_Pipeline_v4
conda activate my_env

# CPU machine
pip install torch --index-url https://download.pytorch.org/whl/cpu
# GPU server: install the CUDA build of torch instead (pip install torch)

pip install -r requirements.txt
chmod +x run_pipeline.sh run_embedder.sh
```

The embedding model downloads from Hugging Face on first use. bge-large is about 1.3 GB; Qwen3-0.6B is about 1.2 GB.

---

To run the tests (including the real-engine graph tests): `pip install -r requirements-dev.txt`, then `python -m pytest -q`.

## 3. Configure

### `.env` (secrets, never commit it)
Copy `.env.example` to `.env` and fill in these:

| Variable | Used for |
|---|---|
| `PG_HOST` `PG_PORT` `PG_DATABASE` `PG_USER` `PG_PASSWORD` | Postgres, as the pipeline reaches it |
| `DEBEZIUM_PG_HOST` `DEBEZIUM_PG_PORT` | Postgres, as the Debezium container reaches it |
| `KAFKA_BOOTSTRAP` `CONNECT_URL` | Kafka and Kafka Connect |
| `NEO4J_URI` `NEO4J_USER` `NEO4J_PASSWORD` | Neo4j |
| `EMBEDDING_MODEL` | Optional. If it is set, it overrides the config file. |

### `config/connections.yaml` (settings you may change)

```yaml
kafka:
  group_id: cdc-neo4j-sink-v7.1      # use a new name to start from scratch; never reuse an old one
  auto_offset_reset: earliest
  max_poll_records: 500              # small batches keep polls frequent
  fetch_partition_max_bytes: 262144  # small fetches keep events flowing on a slow link
  fetch_max_bytes: 5242880

embedding:
  model: BAAI/bge-large-en-v1.5      # server later: Qwen/Qwen3-Embedding-0.6B
  device: cpu                        # cuda on a GPU server
  dimensions: 1024
  max_seq_length: 512
  batch_size: 16                     # 64 on a GPU
  threads: 4                         # physical CPU cores
```

Check the configuration after any change:
```bash
python main.py validate          # must print OK
```

---

## 4. First start (or start clean)

This is the whole procedure, for a first install and for rebuilding from scratch. Nothing older than the moment you start the pipeline is taken from Kafka; everything that already exists comes from Postgres through backfill.

```bash
python main.py validate                        # config + graph rules; must print OK

# optional, only to start clean: remove every tenant graph the pipeline wrote
python main.py reset --all --dry-run           # shows what would go
python main.py reset --all                     # asks you to type DROP (or add --yes)
python main.py provision --all                 # create every tenant database, constraints, indexes

./run_pipeline.sh --from-now                   # terminal 1: live changes from now on
python main.py backfill --all                  # terminal 2: everything that exists today
python main.py integrity                       # every line must be 0
python main.py verify --database org1012       # product questions + data checks on real data
./run_embedder.sh --mark-missing               # terminal 3, after the backfill
```

- **`--from-now`** starts every Kafka partition at its current end and commits that position, so the old backlog is skipped. Use it for the first start after a reset. Later restarts are just `./run_pipeline.sh`; they continue where they stopped.
- **Start the pipeline before the backfill.** Changes made while the backfill runs then arrive live. Every backfilled row carries the database time of its read, so a newer live change always wins, whichever is written first.
- **`backfill --all`** does every active organisation; `--org 1012` does one. Tables run in dependency order (the tables other tables point at come first), through exactly the same code as live events, so labels, product labels, state labels, collections and edges are identical. At the end it removes nodes whose rows were deleted while it ran (`--no-reconcile` skips that). It is safe to rerun. Run it off-peak: it reads Postgres directly.
- **`reset`** drops each tenant database on Neo4j Enterprise, or deletes everything in place on Community (where all tenants share one database, so only `--all` is allowed). It also clears the pipeline's per-tenant state, so provisioning runs in full next time.

### Upgrading from v4.3 or earlier
The graph shape changed in v4.4 (one product label per node, collections, no hubs). Don't migrate in place: stop the pipeline and the embedder, update the code, then follow the procedure above with the `reset` step.

---

## 5. Run in the background

### nohup (quickest)
```bash
nohup ./run_pipeline.sh > logs/run.out 2>&1 &
nohup ./run_embedder.sh > logs/embed.out 2>&1 &

pgrep -af "main.py"                  # what is running
pkill -INT -f "main.py run"          # stop the pipeline cleanly (same as Ctrl+C)
pkill -INT -f "main.py embed"        # stop the embedder
```

### tmux (easy to come back to)
```bash
sudo apt install tmux
tmux new -s kg
conda activate my_env && ./run_pipeline.sh      # window 1
# Ctrl+B then C (new window)
conda activate my_env && ./run_embedder.sh      # window 2
# Ctrl+B then D (detach; both keep running)
tmux attach -t kg                               # come back
```

### systemd user services (start at boot, restart on crash)
Create `~/.config/systemd/user/kg-pipeline.service`:
```ini
[Unit]
Description=Knowledge Graph CDC pipeline
After=network-online.target

[Service]
WorkingDirectory=/home/govindaraajan/Uday/Knowledge_Graph_Pipeline_v4
Environment=PYTHON=/home/govindaraajan/miniconda3/envs/my_env/bin/python
ExecStart=/bin/bash run_pipeline.sh
Restart=on-failure
RestartSec=30
KillSignal=SIGINT
TimeoutStopSec=120

[Install]
WantedBy=default.target
```
Copy it to `kg-embedder.service` and change only `Description` and `ExecStart=/bin/bash run_embedder.sh`. Then:
```bash
systemctl --user daemon-reload
systemctl --user enable --now kg-pipeline kg-embedder
loginctl enable-linger $USER             # keep running after logout
systemctl --user status kg-pipeline
systemctl --user restart kg-pipeline     # after a config change
systemctl --user stop kg-pipeline kg-embedder
journalctl --user -u kg-pipeline -f      # startup errors (normal logs stay in logs/)
```

---

## 6. Everyday commands

| Command | Use |
|---|---|
| `python main.py validate` | Check the config and ontology rules |
| `python main.py dry-run --org org1012` | Print the generated Cypher |
| `python main.py provision --all` | Run a full provisioning, even if nothing changed |
| `./run_pipeline.sh --from-now` | Start consuming from the current end of Kafka (skip the backlog) |
| `python main.py backfill --all` or `--org 1012` | Load the current rows from Postgres (dependency order, then delete-reconcile) |
| `python main.py backfill --org 1012 --table cdp.customer_profile` | Reload one table |
| `python main.py reset --all` or `--org 1012` | Remove every graph the pipeline wrote (asks to confirm; `--dry-run` to preview) |
| `python main.py integrity [--org 1012]` | Check the graph rules; every line should be 0 (`--fix` removes empty collections) |
| `python main.py verify --database org1012` | Read-only report: nodes per product, the 38 product questions on real data, id-space data checks (`logs/verify_org1012.md`) |
| `python scripts/reasoning_check.py` | Re-check the product questions against the config (`docs/ONTOLOGY_REASONING.md`) |
| `python scripts/core_overview.py` | Regenerate `docs/CORE_OVERVIEW.md` after changing `core.yaml` |
| `python main.py embed --status` | Embedding backlog per label |
| `./run_embedder.sh --mark-missing` | Queue nodes that have no vector yet |
| `python main.py embed --switch-status` | Progress of a running embedding model switch |
| `python main.py scopes [--scope Support]` | The product label of every label |
| `python main.py scopes --write-doc` | Regenerate `docs/RBAC_NODE_MAPPING.md` |
| `python main.py scopes --grants --org-db org1012` | Neo4j Enterprise role grants, one read-only role per product |
| `python main.py reconcile-deletes --org 1012 [--apply]` | Remove nodes whose Postgres row is gone (dry run without `--apply`) |
| `python main.py replay-quarantine --date 2026-09-25` | Retry quarantined events |
| `python scripts/diagnose_pipeline.py [--skip-neo4j] [--org 1012]` | Checks Debezium state, group members, lag and retention, node and vector counts |
| `python scripts/peek_topic.py cdp.customer_profile --find "<value>"` | Did Debezium push my change? |
| `python scripts/set_group_start.py --at "YYYY-MM-DD HH:MM"` or `--latest` | Skip old Kafka history |
| `python scripts/reset_graph.py [--graph-only\|--kafka-only]` | Wipe the graphs and/or rewind the group |
| `python scripts/delete_index_topics.py [--delete]` | Remove empty topics named after Postgres indexes |
| `python scripts/consumer_selftest.py` | Test the pipeline's Kafka settings with a throwaway group |
| `python scripts/bench_embeddings.py` | Embedding speed and RAM on this machine |
| `python scripts/reconcile.py --org 1012` | Compare Postgres row counts with Neo4j |

---

## 7. Embeddings

- **Start:** `./run_embedder.sh`. Add `--mark-missing` the first time and after a backfill.
- **Progress:** `python main.py embed --status`.
- **What is stored on a node:** only `_embedding` (the vector), and `_embed_pending = true` while it waits. The flag is removed in the same write that stores the vector. The text is never stored; the worker builds it from the node's own properties, e.g. `Ticket. subject: Login fails. description: ...`.
- **Which model made the vectors** is recorded once per tenant database, in a `PipelineState` node in the metadata database (`neo4j`), not on every node. The worker refuses to embed a database whose recorded model differs from its own, so vectors from two models are never mixed.
- **When a node is re-embedded:** only when one of its label's `embed.fields` changes. A status change does not re-embed. Tip: `REPLICA IDENTITY FULL` on embedded tables lets the pipeline see the old values and skip needless re-embeds.
- **Embedded labels:** the 18 that carry text: Contact, Ticket, KnowledgeArticle, Product, Deal, Campaign, Persona, Macro, InternalNote, Notification, MessageTemplate, Alert, Recommendation, NextBestAction, SalesProfile, KnowledgeFile, Event and Tenant. Change the list with `embed:` in `core.yaml`; an industry pack or org overlay can add fields with `extends: <table>: embed_fields: [...]`. The next `provision` re-queues only the labels whose fields changed.

### Switching the model (e.g. bge to Qwen on the GPU server)
Vectors from two models cannot be compared, so every node is re-embedded. It happens next to the old vectors, so search keeps working the whole time.

1. Check the GPU: `python -c "import torch; print(torch.cuda.is_available())"` must print `True`.
2. Start the switch (queues every node for the new model and creates its index):
   ```bash
   python main.py embed --switch-model Qwen/Qwen3-Embedding-0.6B --dims 1024
   ```
3. Keep the **old** worker running (it serves live changes and forwards each one to the new model), and start a **second** worker with the new model:
   ```bash
   ./run_embedder.sh --model Qwen/Qwen3-Embedding-0.6B --dims 1024
   ```
   It writes `_embedding_next`. It can be stopped and restarted; it continues where it left off.
4. Watch progress: `python main.py embed --switch-status`.
5. When every label shows 0 remaining:
   ```bash
   python main.py embed --switch-finish
   ```
   This moves `_embedding_next` into `_embedding`, rebuilds the vector index and makes Qwen the active model.
6. In `connections.yaml` set `embedding.model: Qwen/Qwen3-Embedding-0.6B`, `device: cuda`, `batch_size: 64` (and `EMBEDDING_MODEL` in `.env`, or delete that line). Stop both workers and start one normal `./run_embedder.sh`.
7. Switch the query side (the agents) to Qwen at the same moment as step 5.

To cancel at any point: `python main.py embed --switch-abort` (the old model stays active, nothing is lost). Add `--org 1012` to any of these to do one tenant at a time.

**Smaller vectors, same model:** Qwen3-Embedding is trained so its vectors can be shortened. `python main.py embed --truncate-dims 512` shortens every stored vector without re-embedding. bge cannot do this.

---

## 7a. The graph shape

See README section 5 for the full model. In short:
- **Product labels:** `Support` (ticketing), `Sales` (crm), `Marketing` (campaign), `ContactCentre` (ccass), `CDP` (cdp), `WebTracking`. Setup and catalog nodes (Tenant, User, Team, Channel, Product, …) carry **no** product label and are readable by every product.
- **People:** `(User)-[:ACTS_AS]->(ContactCentreAgent | SupportAgent | SalesRep | MarketingAgent)`.
- **Relationships:** `HAS` for containment, verbs for meaning.
- **Collections** on Contact, Tenant, Account, Campaign and Ticket; e.g. `(Contact)-[:HAS]->(TicketHistory)-[:HAS]->(Ticket)`.
- **Tables left out of the graph** (configuration, logs, high-volume metrics) are listed in `config/archive/dropped_from_graph_v45.yaml`. An org overlay that still extends one is ignored with a warning.

Check a label's product with `python main.py scopes --scope Support`, or regenerate `docs/RBAC_NODE_MAPPING.md` with `python main.py scopes --write-doc`.

- **Every table, node, key, edge and collection:** `docs/CORE_OVERVIEW.md`.
- **How agents answer product questions (paths, directions, boundaries):** `docs/ONTOLOGY_REASONING.md`, which also explains how a conversation finds its channel.
- **Campaign contact ids:** Campaign stores a bigint projection of `cdp_id`; every Contact carries `campaign_contact_id` (`src/id_mapping.py`). When Campaign switches to the full `cdp_id`, point the three campaign references in `core.yaml` back at `cdp_id` and rebuild.

## 7b. Deletes

A row deleted in Postgres deletes its node, its edges and its vector, and any collection it leaves empty. Also:
- **Merged tables** that only add properties to another table's node (the email/webchat/social channel settings, contact identities) remove only their own properties; the node stays until its owning row is deleted.
- **Soft deletes in your apps:** add `delete_when: {deleted_at: not_null}` (or `{is_deleted: [true]}`) to the table in `core.yaml`. That update then deletes the node.
- **Missed deletes** (pipeline down past Kafka retention): `python main.py reconcile-deletes --org 1012` shows nodes whose row is gone; `--apply` deletes them. Backfill runs it automatically at the end. Stubs are never touched by it.

---

## 8. Why startup takes a while

Before the pipeline starts reading Kafka, it:
1. loads the tenant list and the partition map from Postgres (one query each)
2. **provisions every active tenant** in Neo4j: the database, a uniqueness constraint per label, the hubs and the vector indexes

Step 2 is several hundred small Neo4j queries per tenant. Over a slow link, each one waits a full network round trip, so ten tenants took about 12 minutes.

**Now:** each tenant's schema fingerprint is stored in its `PipelineState` node in the metadata database. At startup the pipeline compares fingerprints and skips provisioning for tenants whose schema hasn't changed (log line: `schema unchanged, provisioning skipped`). The first start after a change is still slow; later starts take seconds per tenant. `python main.py provision --all` always runs everything.

---

## 9. Troubleshooting

| Symptom | Check |
|---|---|
| `No message received yet` for minutes | Run `diagnose_pipeline.py`. `members` should be 1. Check that the group has lag. Then run `consumer_selftest.py`. |
| Pipeline looks idle but events are waiting | Run the speed test. A slow link makes each fetch take minutes. Use small fetch settings, `set_group_start.py` and backfill. |
| Change not visible | Run `peek_topic.py <table> --find <value>`: is it in Kafka? Then look at that topic's `lag` in `diagnose_pipeline.py`. |
| `DEBEZIUM CONNECTOR CRASHED` | Postgres was unreachable for Debezium. When it is back: `curl -X POST "$CONNECT_URL/connectors/cdc-postgres-source/restart?includeTasks=true&onlyFailed=true"` |
| Offsets reset fails | `kafka-consumer-groups.sh` is not needed any more. Use `reset_graph.py --kafka-only`, or a new `group_id`. |
| Pipeline stuck | `sudo env "PATH=$PATH" py-spy dump --pid $(pgrep -f "main.py run" \| head -1)` shows where it is waiting |
| Kafka client details | `KAFKA_DEBUG=cgrp,fetch ./run_pipeline.sh 2> kafka_debug.log` |
| Embedder logs `uses embedding model X but this worker runs Y` | The database was embedded with another model. Run the worker with the active model, or start a switch (section 7). |
| An agent sees nodes of another product | Run `python main.py integrity`: `product_labels` must be 0, and the agent must read through `scoped_read` (or the agent endpoints). |
| `integrity` shows `stubs_old` | Rows point at something that never arrived (wrong key type, missing table in the connector, or a deleted parent). Look at the examples; `--remove-stubs-older-than 30` cleans them up. |
| Edges missing after a backfill | Run `python main.py integrity`, then re-run `backfill --org <id>`; it is safe to repeat. |
| Logs look incomplete | Several processes share `logs/pipeline.log`, and it rotates at midnight. Also check `logs/pipeline.log.<date>`. |
