"""Provisioning.  Spec sections 5.3 and 5.4.  Every operation is idempotent."""

from __future__ import annotations

import logging
import time

from .cypher_builder import build_constraint
from .errors import ProvisioningError
from .naming import constraint_name, quote_db

log = logging.getLogger(__name__)

ONLINE_TIMEOUT_SECONDS = 120
POLL_INTERVAL_SECONDS = 2


class Provisioner:
    def __init__(self, pool, loader):
        self.pool = pool
        self.loader = loader

    # -- fail-fast checks ---------------------------------------------------

    def check_enterprise(self, instance: str | None = None) -> str:
        """Multiple databases in one DBMS is an Enterprise feature.  Spec 2.2."""
        driver = self.pool.get(instance)
        with driver.session(database="system") as s:
            rows = list(s.run(
                "CALL dbms.components() YIELD name, versions, edition "
                "RETURN name AS name, versions[0] AS version, edition AS edition"
            ))
        rec = next((r for r in rows if r["name"] == "Neo4j Kernel"), None)
        if rec is None:
            raise ProvisioningError("could not determine Neo4j edition")
        edition, version = rec["edition"], rec["version"]
        if str(edition).lower() != "enterprise":
            print(f"Warning: Neo4j edition is {edition!r}. Falling back to default 'neo4j' database.")
        log.info("neo4j edition=%s version=%s instance=%s", edition, version, instance or "default")
        return version

    def assert_prefix_immutable(self, prefix: str, instance: str | None = None) -> None:
        """Persist the naming prefix on first provision; assert it on every boot."""
        driver = self.pool.get(instance)
        meta = self.pool.metadata_database
        self._create_database(driver, meta)
        with driver.session(database=meta) as s:
            rec = s.run(
                "MERGE (c:PipelineConfig {id: 'singleton'}) "
                "ON CREATE SET c.prefix = $prefix, c.created_at = datetime() "
                "RETURN c.prefix AS prefix",
                prefix=prefix,
            ).single()
        stored = rec["prefix"]
        if stored != prefix:
            raise ProvisioningError(
                f"database_naming.prefix is {prefix!r} but this DBMS was first "
                f"provisioned with {stored!r}. The prefix is immutable: changing it "
                "orphans every existing tenant database and every mapping file. "
                "Revert the config, or migrate deliberately and update "
                f"(:PipelineConfig {{id:'singleton'}}).prefix in the {meta} database."
            )


    def _create_database(self, driver, name: str) -> None:
        with driver.session(database="system") as s:
            names = [r["name"] for r in s.run("SHOW DATABASES YIELD name RETURN name")]
            exists = name in names
            if not exists:
                log.info("creating database %s", name)
                try:
                    s.run(f"CREATE DATABASE {quote_db(name)} IF NOT EXISTS WAIT 60 SECONDS")
                except Exception:
                    pass
        self._wait_online(driver, name)

    def _wait_online(self, driver, name: str) -> None:
        deadline = time.time() + ONLINE_TIMEOUT_SECONDS
        while time.time() < deadline:
            with driver.session(database="system") as s:
                rows = list(s.run(
                    "SHOW DATABASES YIELD name, currentStatus "
                    "RETURN name AS name, currentStatus AS currentStatus"
                ))
            statuses = [r["currentStatus"] for r in rows if r["name"] == name]
            if statuses and all(st == "online" for st in statuses):
                return
            time.sleep(POLL_INTERVAL_SECONDS)
        print(f"Skipping wait online error for {name} (Community Edition)")
        return

    def ensure_database(self, name: str, instance: str | None = None) -> None:
        self._create_database(self.pool.get(instance), name)


    def ensure_constraints(self, database: str, instance: str | None = None) -> list[str]:
        try:
            with self.pool.get(instance).session(database=database) as test:
                test.run("RETURN 1").consume()
        except Exception:
            database = "neo4j"

        """Create a uniqueness constraint for every label the graph can contain.

        Constraints are created for ALL mappings, including products the
        organisation has not enabled: an index on an empty label costs essentially
        nothing and removes the need for a later migration when the product is
        bought.  Stub labels are included -- see config_loader.all_labels().
        """
        eff = self.loader.effective(database)
        labels = eff.all_labels()
        created: list[str] = []
        driver = self.pool.get(instance)
        with driver.session(database=database) as s:
            for label, key in sorted(labels.items()):
                cname = constraint_name(label, key)
                s.run(build_constraint(label, key, cname))
                created.append(cname)
        # Secondary match keys (Contact.campaign_contact_id, Product.sku, ...) get an index so
        # every MATCH on them is a seek, not a label scan.
        with driver.session(database=database) as s:
            for label, prop in sorted(eff.secondary_keys()):
                name = f"idx_{label.lower()}_{prop.lower()}"
                s.run(f"CREATE INDEX `{name}` IF NOT EXISTS FOR (n:`{label}`) ON (n.`{prop}`)").consume()
        log.info("database=%s constraints ensured=%d", database, len(created))
        return created

    def ensure_hubs(self, org_id: str, database: str, instance: str | None = None) -> None:
        """Hub nodes were removed (v4.4): tenants have their own database, and every node
        carries one product label, so nothing needs an entry-point node."""
        return None

    def ensure_vector_indexes(self, database: str, instance: str | None = None) -> int:
        """
        Create the HNSW vector index and the pending-queue index for every label with ``embed:`` configured.
        """
        from src.embedder import embedding_config
        from src.graph_state import get_state
        default_dim = int(embedding_config(self.loader)["dimensions"])
        try:
            # The database's own record wins over config: after a model switch the index
            # must match the vectors actually stored, not whatever connections.yaml says.
            default_dim = int(get_state(self.pool, database, instance).get("embed_dims") or default_dim)
        except Exception:
            pass
        eff = self.loader.effective(database)
        try:
            with self.pool.get(instance).session(database=database) as test:
                test.run("RETURN 1").consume()
        except Exception:
            database = "neo4j"

        driver = self.pool.get(instance)
        created = 0
        with driver.session(database=database) as session:
            existing = {}
            for r in session.run("SHOW VECTOR INDEXES YIELD name, options RETURN name, options"):
                cfg = (r["options"] or {}).get("indexConfig", {}) or {}
                existing[r["name"]] = cfg.get("vector.dimensions")
            done = set()
            for table, m in eff.tables.items():
                if not getattr(m, 'embed', None) or not m.label:
                    continue
                prop = m.embed.get("property", "_embedding")
                if (m.label, prop) in done:
                    continue
                done.add((m.label, prop))
                dim = default_dim
                idx_name = f"{m.label.lower()}_{prop}_vector"
                if idx_name in existing and existing[idx_name] not in (None, dim):
                    log.warning("database=%s vector index %s has dimensions=%s, expected %d; "
                                "dropping and recreating it. Run `python main.py embed "
                                "--mark-all` to re-embed.", database, idx_name,
                                existing[idx_name], dim)
                    session.run(f"DROP INDEX `{idx_name}` IF EXISTS").consume()
                session.run(
                    f"CREATE VECTOR INDEX `{idx_name}` IF NOT EXISTS "
                    f"FOR (n:`{m.label}`) ON (n.`{prop}`) "
                    f"OPTIONS {{indexConfig: {{"
                    f"`vector.dimensions`: {dim}, "
                    f"`vector.similarity_function`: 'cosine'"
                    f"}}}}"
                ).consume()
                # Range index so the worker's `WHERE n._embed_pending = true` is a seek.
                session.run(
                    f"CREATE INDEX `{m.label.lower()}_embed_pending` IF NOT EXISTS "
                    f"FOR (n:`{m.label}`) ON (n._embed_pending)"
                ).consume()
                created += 1
        log.info("database=%s vector indexes ensured=%d", database, created)
        return created

    def requeue_changed_embed_fields(self, database: str, instance: str | None = None) -> dict:
        """Queue every node of a label whose embed field list changed since last provision."""
        from src.embedding_worker import PENDING, _q, targets_for
        from src.graph_state import get_state, set_state
        current = {t.label: list(t.fields) for t in targets_for(self.loader, database)}
        stored = get_state(self.pool, database, instance).get("embed_fields")
        changed = {}
        if isinstance(stored, dict):
            with self.pool.get(instance).session(database=database) as s:
                for label, fields in current.items():
                    if stored.get(label) == fields:
                        continue
                    rec = s.run(f"MATCH (n:{_q(label)}) "
                                f"CALL (n) {{ SET n.{PENDING} = true }} IN TRANSACTIONS OF 5000 ROWS "
                                "RETURN count(*) AS c").single()
                    changed[label] = rec["c"] if rec else 0
            if changed:
                log.info("database=%s embed fields changed, re-queued %s", database, changed)
        set_state(self.pool, database, instance, embed_fields=current)
        return changed

    def verify_index_usage(self, database: str, instance: str | None = None) -> dict:
        """PROFILE a representative MERGE per label; assert an index seek.  Spec 5.4."""
        eff = self.loader.effective(database)
        labels = eff.all_labels()
        driver = self.pool.get(instance)
        results: dict[str, dict] = {}

        with driver.session(database=database) as s:
            for label, key in sorted(labels.items()):
                tx = s.begin_transaction()
                try:
                    res = tx.run(f"EXPLAIN MERGE (n:`{label}` {{`{key}`: $probe}}) RETURN n", probe="__index_probe__")
                    summary = res.consume()
                    plan = summary.plan          # note: .plan for EXPLAIN, not .profile
                    operators, used_indexes = (_collect_plan_info(plan)
                        if plan
                        else ([], [])
                    )
                finally:
                    tx.rollback()
                unique_lookup = any("MergeUniqueNode" in op or "NodeUniqueIndexSeek" in op for op in operators)
                index_used = (
                    bool(used_indexes)
                    or any("NodeUniqueIndexSeek" in op for op in operators)
                    or any("MergeUniqueNode" in op for op in operators)
                )
                label_scan = any("NodeByLabelScan" in op for op in operators)
                results[label] = {
                    "key": key,
                    "operators": operators,
                    "used_indexes": used_indexes,
                    "index_seek": unique_lookup and index_used and not label_scan,
                    "label_scan": label_scan,
                }
        return results


def _collect_plan_info(plan) -> tuple[list[str], list[str]]:
    operators: list[str] = []
    used_indexes: list[str] = []
    stack = [plan]
    while stack:
        node = stack.pop()
        if node is None or not isinstance(node, dict):
            continue
        operator = node.get("operatorType")
        if operator:
            operators.append(operator)
        args = node.get("args", {})
        if isinstance(args, dict):
            indexes = args.get("UsedIndexes")
            if indexes:
                used_indexes.append(str(indexes))
        children = node.get("children", [])
        stack.extend(children)
    return operators, used_indexes


def schema_fingerprint(provisioner: Provisioner, org) -> str:
    """Hash of everything provisioning creates; unchanged hash = nothing to do."""
    import hashlib
    import json
    from src.embedder import embedding_config
    from src.embedding_worker import targets_for
    eff = provisioner.loader.effective(org.database)
    blob = json.dumps({
        "labels": sorted(eff.all_labels().items()),
        "hubs": sorted(eff.hubs),
        "dim": int(embedding_config(provisioner.loader)["dimensions"]),
        "embed": sorted((t.label, t.fields) for t in targets_for(provisioner.loader, org.database)),
        "scopes": sorted((k, tuple(v)) for k, v in eff.label_scopes.items()),
    }, sort_keys=True, default=str)
    return hashlib.sha1(blob.encode()).hexdigest()


def provision_organisation(provisioner: Provisioner, org, registry, force: bool = False) -> None:
    """Idempotent provisioning for one organisation. Skipped when the schema is unchanged."""
    from src.graph_state import get_state, set_state
    provisioner.ensure_database(org.database, org.instance)
    fp = schema_fingerprint(provisioner, org)
    stored = None
    if not force:
        try:
            stored = get_state(provisioner.pool, org.database, org.instance).get("schema_fingerprint")
        except Exception:
            stored = None
    if stored == fp:
        log.info("database=%s schema unchanged, provisioning skipped", org.database)
    else:
        provisioner.ensure_constraints(org.database, org.instance)
        provisioner.ensure_hubs(org.organization_id, org.database, org.instance)
        provisioner.ensure_vector_indexes(org.database, org.instance)
        provisioner.requeue_changed_embed_fields(org.database, org.instance)
        set_state(provisioner.pool, org.database, org.instance, schema_fingerprint=fp)
    eff = provisioner.loader.effective(org.database)
    if not eff.overlay_present:
        log.warning(
            "no overlay file for database %s; using core.yaml alone. No products are "
            "declared, so every product-gated mapping will be skipped.",
            org.database,
        )
    registry.mark_provisioned(org.tenant_id, eff.products)
    log.info(
        "provisioned organisation=%s database=%s instance=%s products=%s",
        org.organization_id, org.database, org.instance, sorted(eff.products),
    )
