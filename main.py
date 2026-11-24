#!/usr/bin/env python3
"""cdc-pipeline -- PostgreSQL -> Neo4j multi-tenant CDC pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.config_loader import ConfigLoader              # noqa: E402
from src.errors import ConfigError                      # noqa: E402


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO") -> None:
    import os
    import logging.handlers
    os.makedirs("logs", exist_ok=True)
    handler = logging.StreamHandler(sys.stdout)

    # Rotate at midnight, keep the last 2 days to save disk space
    file_handler = logging.handlers.TimedRotatingFileHandler(
        "logs/pipeline.log", when="midnight", interval=1, backupCount=2
    )

    # Dedicated error log handler as requested
    error_handler = logging.FileHandler("logs/error.log")
    error_handler.setLevel(logging.ERROR)

    formatter = JsonFormatter()
    handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)

    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO),
                        handlers=[handler, file_handler, error_handler], force=True)


def _load(args) -> ConfigLoader:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    neo4j_overrides = {}
    if getattr(args, "neo4j_uri", None): neo4j_overrides["uri"] = args.neo4j_uri
    if getattr(args, "neo4j_user", None): neo4j_overrides["user"] = args.neo4j_user
    if getattr(args, "neo4j_password", None): neo4j_overrides["password"] = args.neo4j_password

    overrides = {"neo4j": neo4j_overrides} if neo4j_overrides else None

    domain_overlay = getattr(args, "domain_overlay", None)

    loader = ConfigLoader(args.config, overrides=overrides, domain_overlay=domain_overlay)
    from src import coercion
    coercion.set_money_mode((loader.connections.get("money") or {}).get("store_as", "decimal"))
    return loader


def _pg_factory(loader):
    import psycopg
    pg = loader.section("postgres")

    def factory():
        return psycopg.connect(
            host=pg["host"], port=pg.get("port", 5432), dbname=pg["database"],
            user=pg["user"], password=pg["password"],
        )
    return factory


def _build_runtime(loader, load_registry: bool = True):
    from src.drivers import DriverPool
    from src.metrics import Metrics
    from src.provision import Provisioner
    from src.quarantine import Quarantine
    from src.registry import Registry
    from src.router import Router
    from src.tenant_resolver import TenantResolver
    from src.writer import Writer

    pool = DriverPool(loader.section("neo4j"))
    metrics = Metrics()
    registry = Registry(loader.prefix, loader.section("control_plane"), pool.default_instance)
    provisioner = Provisioner(pool, loader)
    cp = loader.section("control_plane")
    tenant_column = cp.get("tenant_column", "tenant_id")
    pg_factory = _pg_factory(loader)

    control_conn = pg_factory()
    control_conn.autocommit = True
    source_conn = pg_factory()
    source_conn.autocommit = True     # read-only lookups: never leave a transaction open

    tenant_resolver = TenantResolver(loader.core_doc, control_conn, source_conn)
    writer = Writer(pool, loader, loader.connections, metrics, tenant_resolver=tenant_resolver)

    # Query tables without tenant col from information_schema
    cur = source_conn.cursor()
    schemas = sorted({t.split(".")[0] for t in loader.core_tables})
    cur.execute("""
        SELECT table_schema || '.' || table_name
        FROM information_schema.tables t
        WHERE table_schema = ANY(%s)
          AND table_type = 'BASE TABLE'
          AND NOT EXISTS (
              SELECT 1 FROM information_schema.columns c
              WHERE c.table_schema = t.table_schema
                AND c.table_name = t.table_name
                AND c.column_name = %s
          )
    """, (schemas, tenant_column))
    tables_without_tenant_col = {row[0] for row in cur.fetchall()}

    problems = tenant_resolver.validate(tables_without_tenant_col)
    if problems:
        raise SystemExit("\n".join(problems))

    router = Router(
        loader, registry, loader.section("kafka")["topic_prefix"],
        cp["organisations_table"], metrics,
        False,  # money scale is no longer written on nodes (v4.3)
        tenant_column,
        tenant_resolver=tenant_resolver,
    )
    from src.derived_lookup import postgres_fallback
    router.lookup_cache.fallback = postgres_fallback(pg_factory)
    quarantine = Quarantine(loader.connections.get("quarantine", {}).get("path", "./quarantine"))

    if load_registry:
        provisioner.check_enterprise()
        provisioner.assert_prefix_immutable(loader.prefix)
        with pg_factory() as conn:
            print("CONTROL PLANE:", loader.section("control_plane"))
            registry.load_from_postgres(conn)
            from src.registry import apply_industries
            apply_industries(loader, registry)
            router.load_partition_map_from_postgres(conn)

    return dict(pool=pool, metrics=metrics, registry=registry, provisioner=provisioner,
                writer=writer, router=router, quarantine=quarantine, pg_factory=pg_factory)


def cmd_validate(args) -> int:
    from src.validation import validate_all
    loader = _load(args)
    report = validate_all(loader)
    # Graph rules (product labels, collections, keys) on the core mapping plus every overlay.
    from src import graph_rules
    # ...and with every industry pack, since a tenant's pack is only known at runtime
    packs = sorted(p.stem for p in (loader.config_dir / "mappings" / "domains").glob("*.yaml"))
    for pack in packs:
        loader.set_industry(f"_pack_{pack}", pack)
    for db in ["org1000", *[f.stem for f in loader.overlay_files()], *[f"_pack_{p}" for p in packs]]:
        try:
            eff = loader.effective(db)
        except Exception as exc:                      # noqa: BLE001 - reported as a config error
            report.errors.append(f"{db}: {exc}")
            continue
        for problem in graph_rules.check(eff, loader.scopes_cfg):
            if problem not in report.errors:
                report.errors.append(problem)
    for w in report.warnings:
        print(f"WARN   {w}")
    for e in report.errors:
        print(f"ERROR  {e}")
    if report.ok:
        print(
            f"OK     core.yaml: {len(loader.core_tables)} table(s), "
            f"{len(loader.stub_labels)} stub label(s); "
            f"{len(loader.overlay_files())} overlay(s) validated"
        )
        return 0
    print(f"\nFAILED with {len(report.errors)} error(s)")
def cmd_manifest(args) -> int:
    from src.manifest import build_manifest
    import json
    
    loader = _load(args)
    rt = _build_runtime(loader)
    targets = (
        rt["registry"].active()
        if args.all
        else [o for o in rt["registry"].all() if o.organization_id == str(args.org)]
    )
    if not targets:
        print("no matching organisation")
        return 1
        
    for org in targets:
        doc = build_manifest(loader, rt["pool"], org.database)
        print(json.dumps(doc, indent=2))
        
    rt["pool"].close()
    return 0

def cmd_dry_run(args) -> int:
    from src.cypher_builder import build_all, build_constraint
    from src.naming import constraint_name
    loader = _load(args)
    syntax = loader.connections.get("cypher", {}).get("subquery_syntax", "legacy")

    databases = [args.org] if args.org else [p.stem for p in loader.overlay_files()]
    if not databases:
        databases = ["<core-only>"]

    for db in databases:
        eff = loader.effective(db)
        print(f"\n{'=' * 78}\nDATABASE {db}"
              f"  products={sorted(eff.products) or '(none)'}"
              f"  overlay={'yes' if eff.overlay_present else 'NO -- core only'}\n{'=' * 78}")

        print("\n--- CONSTRAINTS (created before any data) ---")
        for label, key in sorted(eff.all_labels().items()):
            print(f"  {build_constraint(label, key, constraint_name(label, key))}")

        for table, m in sorted(eff.tables.items()):
            gate = ""
            if m.product and m.product not in eff.products:
                gate = "  [SKIPPED -- product not purchased]"
            print(f"\n--- {table}  (mode={m.mode}, product={m.product or '-'}){gate}")
            for op, query in build_all(m, syntax).items():
                note = ""
                if m.mode == "properties_on" and op == "delete":
                    note = ("   (property name is built per flush from the row's "
                            "attr_name; DYNAMIC is a placeholder)")
                print(f"  # {op}{note}")
                for line in query.splitlines():
                    print(f"    {line}")
    return 0


def cmd_preflight(args) -> int:
    from src import preflight
    loader = _load(args)
    factory = _pg_factory(loader)
    tables = sorted(loader.core_tables)
    control = loader.section("control_plane")["organisations_table"]
    tenant_column = loader.section("control_plane").get("tenant_column", "tenant_id")
    with factory() as conn:
        report = preflight.run_all(conn, tables, tenant_column=tenant_column,
                                   control_table=control, strict=not args.warn_only,
                                   core_doc=loader.core_doc)
    print(json.dumps(report, indent=2, default=str))
    return 0


def cmd_provision(args) -> int:
    from src.provision import provision_organisation
    loader = _load(args)
    rt = _build_runtime(loader)
    targets = (
        rt["registry"].active()
        if args.all
        else [o for o in rt["registry"].all() if o.organization_id == str(args.org)]
    )
    if not targets:
        print("no matching organisation")
        return 1
    for org in targets:
        provision_organisation(rt["provisioner"], org, rt["registry"], force=True)
        print(f"provisioned {org.organization_id} -> {org.database} on {org.instance}")
    rt["pool"].close()
    return 0


def cmd_verify_indexes(args) -> int:
    loader = _load(args)
    rt = _build_runtime(loader)
    orgs = (
        [o for o in rt["registry"].all() if o.organization_id == str(args.org)]
        if args.org else rt["registry"].active()
    )
    failed = False
    for org in orgs:
        results = rt["provisioner"].verify_index_usage(org.database, org.instance)
        print(f"\n{org.database}:")
        for label, r in sorted(results.items()):
            status = "OK  " if r["index_seek"] and not r["label_scan"] else "FAIL"
            if status == "FAIL":
                failed = True
            print(f"  {status} :{label}({r['key']})  operators={r['operators']}")
    rt["pool"].close()
    if failed:
        print("\nFAILED: at least one MERGE is not using a unique index seek. "
              "Do NOT proceed to load data.")
        return 1
    print("\nAll merges use NodeUniqueIndexSeek.")
    return 0


def cmd_run(args) -> int:
    from src.consumer import Consumer
    loader = _load(args)
    rt = _build_runtime(loader, load_registry=False)
    mcfg = loader.connections.get("metrics", {})
    if mcfg.get("port"):
        rt["metrics"].serve(int(mcfg["port"]))

    rt["provisioner"].check_enterprise()
    rt["provisioner"].assert_prefix_immutable(loader.prefix)

    consumer = Consumer(
        loader, rt["registry"], rt["router"], rt["writer"], rt["provisioner"],
        rt["quarantine"], rt["metrics"], rt["pg_factory"],
    )
    consumer.start_from_now = bool(getattr(args, "from_now", False))
    consumer.bootstrap()
    try:
        consumer.run()
    finally:
        rt["pool"].close()
    return 0


def cmd_run_ch(args) -> int:
    from src.ch_consumer import ClickHouseConsumer
    loader = _load(args)
    rt = _build_runtime(loader, load_registry=False)
    mcfg = loader.connections.get("metrics", {})
    if mcfg.get("port"):
        rt["metrics"].serve(int(mcfg["port"]) + 1) # run on a different port than main consumer

    consumer = ClickHouseConsumer(
        loader, rt["registry"], rt["router"], rt["writer"], rt["provisioner"],
        rt["quarantine"], rt["metrics"], rt["pg_factory"],
    )
    consumer.bootstrap()
    try:
        consumer.run()
    finally:
        rt["pool"].close()
    return 0

def cmd_run_ch_poll(args) -> int:
    from src.ch_poller import ClickHousePoller, WatermarkStore
    from confluent_kafka import Producer
    import clickhouse_connect

    loader = _load(args)
    kafka_conf = loader.section("kafka")
    producer = Producer({'bootstrap.servers': kafka_conf["brokers"]})

    # We expect clickhouse connection details in config (or use defaults for skeleton)
    ch_conf = loader.connections.get("clickhouse", {})
    ch_client = clickhouse_connect.get_client(
        host=ch_conf.get("host", "localhost"),
        port=ch_conf.get("port", 8123),
        username=ch_conf.get("user", "default"),
        password=ch_conf.get("password", ""),
        database=ch_conf.get("database", "default")
    )

    poller_conf = loader.connections.get("clickhouse_poller", {"tables": []})
    wm = WatermarkStore("data/ch_poller_watermarks.db")

    poller = ClickHousePoller(poller_conf, ch_client, producer, wm, loader)
    try:
        poller.run_forever()
    finally:
        producer.flush()

def cmd_reset(args) -> int:
    """Drop everything the pipeline wrote, for all organisations or one."""
    from src.reset import reset_database, shared_database
    loader = _load(args)
    pool, registry = _light_runtime(loader)
    orgs = registry.all() if args.all else [o for o in registry.all()
                                            if o.organization_id == str(args.org)]
    if not orgs:
        print(f"organisation {args.org} is not in the control plane")
        return 1
    shared = shared_database(pool, orgs)
    if shared and not args.all:
        print("This Neo4j keeps every tenant in one database (Community edition), so one "
              "organisation cannot be removed on its own. Use --all.")
        return 1
    targets = [("neo4j", None)] if shared else sorted({(o.database, o.instance) for o in orgs})
    print("Will remove everything the pipeline wrote in: " + ", ".join(d for d, _ in targets))
    if not args.dry_run and not args.yes:
        if not sys.stdin.isatty():
            print("Refusing without --yes (no terminal to confirm on).")
            return 1
        if input("Type DROP to confirm: ").strip() != "DROP":
            print("Cancelled.")
            return 1
    for database, instance in targets:
        print(json.dumps(reset_database(pool, database, instance, dry_run=args.dry_run), default=str))
    if not args.dry_run:
        print("Done. Next: ./run_pipeline.sh --from-now, then python main.py backfill --all")
    pool.close()
    return 0


def cmd_integrity(args) -> int:
    """Check every tenant graph against the graph rules. Exit 1 when anything is off."""
    from src import integrity
    loader = _load(args)
    pool, registry = _light_runtime(loader)
    orgs = _select_orgs(registry, args.org)
    bad = False
    for org in orgs:
        eff = loader.effective(org.database)
        with pool.get(org.instance).session(database=org.database) as s:
            if args.fix:
                print(f"{org.database}: removed {integrity.fix_collections(s, eff)} empty/orphan collection node(s)")
            if args.remove_stubs_older_than is not None:
                n = integrity.remove_old_stubs(s, eff, args.remove_stubs_older_than)
                print(f"{org.database}: removed {n} stub(s) older than {args.remove_stubs_older_than} day(s)")
            report = integrity.run_checks(s, eff, loader.scopes_cfg, stub_days=args.stub_days)
        problems = {k: v for k, v in report.items() if v["count"]}
        bad = bad or bool(problems)
        status = "OK" if not problems else "PROBLEMS"
        print(f"{org.database}: {status}")
        for k, v in report.items():
            mark = "  " if not v["count"] else "!!"
            ex = f"   e.g. {v['examples']}" if v["count"] else ""
            print(f"  {mark} {k:<24} {v['count']}{ex}")
    pool.close()
    return 1 if bad else 0


def cmd_verify(args) -> int:
    """Read-only check of a live tenant graph: product labels, product questions, data checks."""
    from src.drivers import DriverPool
    from src.verify import verify
    loader = _load(args)
    pool = DriverPool(loader.section("neo4j"))
    database = args.database
    eff = loader.effective(database)
    with pool.get(args.instance).session(database=database) as s:
        lines, summary = verify(s, eff)
    pool.close()
    report = [f"# Graph verification: {database}", ""] + lines
    Path("logs").mkdir(exist_ok=True)
    out = Path("logs") / f"verify_{database}.md"
    out.write_text("\n".join(report) + "\n", encoding="utf-8")
    print("\n".join(report))
    print(f"\nwritten to {out}")
    return 1 if summary["wrong_product_labels"] else 0


def cmd_backfill(args) -> int:
    """Load current Postgres data into Neo4j for one organisation (--org) or all (--all).

    Uses the same router and writer as live CDC, so labels, product labels, state labels,
    collections and edges come out exactly as live events would produce them. Safe to run
    while `run` is consuming: every row carries the database time of its read, so newer
    live changes always win. Ends with a delete-reconcile per organisation (rows deleted
    while the backfill ran must not come back); --no-reconcile skips it.
    """
    from src.backfill import Backfiller
    from src.provision import provision_organisation
    from src.reconcile import reconcile_org
    loader = _load(args)
    rt = _build_runtime(loader)
    active = rt["registry"].active()
    excluded = {x.strip() for v in (args.exclude or []) for x in str(v).split(",") if x.strip()}
    if excluded and not args.all:
        print("--exclude only works together with --all")
        rt["pool"].close()
        return 1
    if args.all:
        orgs = [o for o in active if o.organization_id not in excluded]
        if excluded:
            print(f"skipping organisation(s): {', '.join(sorted(excluded))}")
    else:
        orgs = [o for o in active if o.organization_id == str(args.org)]
        if not orgs:
            print(f"organisation {args.org} is not active in the control plane")
            rt["pool"].close()
            return 1
    bf = Backfiller(loader, rt["registry"], rt["router"], rt["writer"], rt["pg_factory"],
                    tenant_resolver=rt["router"].tenant_resolver)
    tenant_col = loader.section("control_plane").get("tenant_column", "tenant_id")
    summary, code = {}, 0
    for org in orgs:
        provision_organisation(rt["provisioner"], org, rt["registry"])
        rt["writer"].warm_cache(org.database)
        stats = bf.run(org.organization_id, args.product, args.table, args.resume_from)
        errors = {t: v["error"] for t, v in stats.items() if v.get("error")}
        totals = {k: sum(v.get(k, 0) for v in stats.values())
                  for k in ("read", "written", "skipped", "quarantined")}
        entry = {"tables": len(stats), **totals, "failed_tables": errors}
        if not args.no_reconcile and not args.table:
            with rt["pg_factory"]() as conn:
                entry["reconciled_deletes"] = reconcile_org(
                    rt["pool"], loader, org, conn, tenant_col, apply=True)
        summary[org.database] = entry
        code = code or (1 if errors else 0)
        print(f"{org.database}: {json.dumps(entry, default=str)}", flush=True)
    if args.verbose:
        print(json.dumps(summary, indent=2, default=str))
    rt["pool"].close()
    return code


def cmd_replay(args) -> int:
    loader = _load(args)
    rt = _build_runtime(loader)
    from src.provision import provision_organisation
    for org in rt["registry"].active():
        provision_organisation(rt["provisioner"], org, rt["registry"])
        rt["writer"].warm_cache(org.database)

    def handler(topic, payload) -> bool:
        from src.router import RoutedEvent
        decision = rt["router"].route(topic, payload)
        if isinstance(decision, RoutedEvent):
            rt["writer"].add(decision)
            rt["writer"].flush_all()
            return True
        return False

    stats = rt["quarantine"].replay(args.date, handler)
    print(json.dumps(stats, indent=2))
    rt["pool"].close()
    return 0


def _light_runtime(loader):
    """Neo4j pool + organisation registry, without Kafka or the router."""
    from src.drivers import DriverPool
    from src.registry import Registry, apply_industries
    pool = DriverPool(loader.section("neo4j"))
    registry = Registry(loader.prefix, loader.section("control_plane"), pool.default_instance)
    with _pg_factory(loader)() as conn:
        registry.load_from_postgres(conn)
    apply_industries(loader, registry)
    return pool, registry


def _select_orgs(registry, org_id):
    active = registry.active()
    return [o for o in active if o.organization_id == str(org_id)] if org_id else active


def cmd_scopes(args) -> int:
    """Show / document the product label of every label. Reads config only."""
    from src.scope import rbac_markdown, scope_report
    loader = _load(args)
    database = args.org_db or "org1000"
    eff = loader.effective(database)
    if args.write_doc:
        path = Path(__file__).parent / "docs" / "RBAC_NODE_MAPPING.md"
        path.write_text(rbac_markdown(eff), encoding="utf-8")
        print(f"wrote {path}")
    else:
        for row in scope_report(eff):
            if args.scope and not set(args.scope) & set(row["products"]):
                continue
            print(f"{row['label']:<32} {', '.join(row['products'])}")
    return 0


def cmd_reconcile_deletes(args) -> int:
    """Delete graph nodes whose source row is gone. Dry run unless --apply."""
    from src.reconcile import reconcile_org
    loader = _load(args)
    pool, registry = _light_runtime(loader)
    tenant_col = loader.section("control_plane").get("tenant_column", "tenant_id")
    orgs = _select_orgs(registry, args.org)
    if not orgs:
        print("no matching active organisation")
        return 1
    with _pg_factory(loader)() as conn:
        for org in orgs:
            report = reconcile_org(pool, loader, org, conn, tenant_col, apply=args.apply,
                                   labels=args.label)
            print(org.database, "APPLIED" if args.apply else "DRY RUN", json.dumps(report))
    pool.close()
    return 0


def cmd_embed(args) -> int:
    """Embedding worker. Separate process from `run`; see src/embedding_worker.py."""
    import os
    from src.drivers import DriverPool
    from src.embedder import embedding_config
    from src.embedding_worker import EmbeddingWorker
    from src.registry import Registry

    loader = _load(args)
    cfg = embedding_config(loader)
    if args.model:
        cfg["model"] = args.model
    if args.dims:
        cfg["dimensions"] = int(args.dims)
    # Torch/ONNX read these at import time; set them before the model loads.
    for var in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
        os.environ[var] = str(cfg["threads"])
    pool = DriverPool(loader.section("neo4j"))
    registry = Registry(loader.prefix, loader.section("control_plane"), pool.default_instance)
    with _pg_factory(loader)() as conn:
        registry.load_from_postgres(conn)
    from src.registry import apply_industries
    apply_industries(loader, registry)

    def orgs():
        active = registry.active()
        return [o for o in active if o.organization_id == str(args.org)] if args.org else active

    if not orgs():
        print("no matching active organisation")
        return 1

    if args.status:
        # No model load needed just to read the backlog.
        worker = EmbeddingWorker(loader, registry, pool, cfg, embedder=object())
        for org in orgs():
            print(org.database, worker.backlog(org))
        pool.close()
        return 0

    if args.switch_model or args.switch_status or args.switch_finish or args.switch_abort \
            or args.truncate_dims:
        from src import model_switch as ms
        code = 0
        for org in orgs():
            try:
                if args.switch_model:
                    new_cfg = dict(cfg, model=args.switch_model)
                    if args.dims:
                        new_cfg["dimensions"] = int(args.dims)
                    out = ms.start(pool, loader, org, new_cfg, labels=args.label)
                elif args.switch_status:
                    out = ms.status(pool, loader, org)
                elif args.switch_finish:
                    out = ms.finish(pool, loader, org, force=args.force)
                elif args.switch_abort:
                    out = ms.abort(pool, loader, org)
                else:
                    out = ms.truncate(pool, loader, org, int(args.truncate_dims))
                print(org.database, json.dumps(out, default=str))
            except ValueError as exc:
                print(f"{org.database}: {exc}", file=sys.stderr)
                code = 1
        pool.close()
        return code

    worker = EmbeddingWorker(loader, registry, pool, cfg)
    try:
        if args.mark_missing or args.mark_all:
            for org in orgs():
                print(org.database, worker.mark(org, "all" if args.mark_all else "missing"))
        if args.once:
            while worker.run_once(orgs()):
                pass
            print(json.dumps(worker.stats))
        else:
            worker.run_forever(orgs)
    finally:
        pool.close()
    return 0


def main() -> int:
    p = argparse.ArgumentParser(prog="cdc-pipeline", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default="config", help="config directory (default: config)")
    p.add_argument("--log-level", default="INFO")

    # Overrides for testing
    p.add_argument("--neo4j-uri", help="Override Neo4j URI")
    p.add_argument("--neo4j-user", help="Override Neo4j User")
    p.add_argument("--neo4j-password", help="Override Neo4j Password")
    p.add_argument("--domain-overlay", help="Path to domain overlay to force apply (e.g. config/mappings/domains/ecommerce.yaml)")

    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("validate").set_defaults(func=cmd_validate)
    
    m_parser = sub.add_parser("manifest")
    m_group = m_parser.add_mutually_exclusive_group(required=True)
    m_group.add_argument("--all", action="store_true")
    m_group.add_argument("--org")
    m_parser.set_defaults(func=cmd_manifest)

    d = sub.add_parser("dry-run")
    d.add_argument("--org", help="database name, e.g. orga1b2c3d4")
    d.set_defaults(func=cmd_dry_run)

    pf = sub.add_parser("preflight")
    pf.add_argument("--warn-only", action="store_true")
    pf.set_defaults(func=cmd_preflight)

    pr = sub.add_parser("provision")
    g = pr.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true")
    g.add_argument("--org")
    pr.set_defaults(func=cmd_provision)

    vi = sub.add_parser("verify-indexes")
    vi.add_argument("--org")
    vi.set_defaults(func=cmd_verify_indexes)

    rn = sub.add_parser("run", help="consume CDC events into Neo4j")
    rn.add_argument("--from-now", action="store_true",
                    help="skip events already waiting in Kafka; start from the current end")
    rn.set_defaults(func=cmd_run)
    sub.add_parser("run-ch").set_defaults(func=cmd_run_ch)
    sub.add_parser("run-ch-poll").set_defaults(func=cmd_run_ch_poll)

    rs = sub.add_parser("reset", help="drop every tenant graph the pipeline wrote (start clean)")
    rsw = rs.add_mutually_exclusive_group(required=True)
    rsw.add_argument("--all", action="store_true", help="every organisation in the control plane")
    rsw.add_argument("--org", help="one organisation, e.g. 1012 (Enterprise only)")
    rs.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    rs.add_argument("--dry-run", action="store_true", help="only show what would be removed")
    rs.set_defaults(func=cmd_reset)

    ig = sub.add_parser("integrity", help="check tenant graphs against the graph rules")
    ig.add_argument("--org", help="organization_id (default: all active)")
    ig.add_argument("--stub-days", type=int, default=7, help="report stubs older than this")
    ig.add_argument("--fix", action="store_true", help="remove empty and orphan collections")
    ig.add_argument("--remove-stubs-older-than", type=int, metavar="DAYS",
                    help="delete stubs never filled in after DAYS")
    ig.set_defaults(func=cmd_integrity)

    vf = sub.add_parser("verify", help="read-only check of a live tenant graph")
    vf.add_argument("--database", required=True, help="tenant database, e.g. org1012")
    vf.add_argument("--instance", default=None, help="neo4j instance name (default instance if omitted)")
    vf.set_defaults(func=cmd_verify)

    bf = sub.add_parser("backfill", help="load current Postgres data into Neo4j")
    which = bf.add_mutually_exclusive_group(required=True)
    which.add_argument("--all", action="store_true", help="every active organisation")
    which.add_argument("--org", dest="org", help="one organisation, e.g. 1012")
    bf.add_argument("--exclude", action="append", metavar="ORG",
                    help="with --all: skip these organisations (repeat, or comma-separated: 1012,1013)")
    bf.add_argument("--product")
    bf.add_argument("--table", help="only this table (skips the delete-reconcile)")
    bf.add_argument("--resume-from", dest="resume_from")
    bf.add_argument("--no-reconcile", action="store_true",
                    help="skip removing nodes whose rows were deleted during the backfill")
    bf.add_argument("--verbose", action="store_true", help="print the full summary at the end")
    bf.set_defaults(func=cmd_backfill)

    em = sub.add_parser("embed", help="run the embedding worker (separate from `run`)")
    em.add_argument("--org", help="organization_id, e.g. 1012 (default: all active)")
    em.add_argument("--mark-missing", action="store_true",
                    help="queue nodes that have no vector yet")
    em.add_argument("--mark-all", action="store_true",
                    help="queue every embeddable node with the active model")
    em.add_argument("--once", action="store_true", help="drain the queue, then exit")
    em.add_argument("--status", action="store_true", help="print pending counts and exit")
    em.add_argument("--model", help="run the worker with this model (overrides connections.yaml)")
    em.add_argument("--dims", type=int, help="vector size for --model / --switch-model")
    em.add_argument("--label", action="append", help="limit --switch-model to these labels")
    sw = em.add_mutually_exclusive_group()
    sw.add_argument("--switch-model", metavar="MODEL",
                    help="start re-embedding every node with MODEL next to the current vectors")
    sw.add_argument("--switch-status", action="store_true", help="progress of a running switch")
    sw.add_argument("--switch-finish", action="store_true",
                    help="swap in the new vectors and make the new model active")
    sw.add_argument("--switch-abort", action="store_true",
                    help="drop the new vectors; the current model stays active")
    sw.add_argument("--truncate-dims", type=int, metavar="N",
                    help="shorten vectors of a Matryoshka model (Qwen3-Embedding) to N, no re-embed")
    em.add_argument("--force", action="store_true",
                    help="--switch-finish even if nodes are still waiting (they re-embed afterwards)")
    em.set_defaults(func=cmd_embed)


    sc = sub.add_parser("scopes", help="show which products can see each label")
    sc.add_argument("--org-db", help="tenant database whose overlays to apply (default org1000)")
    sc.add_argument("--scope", action="append", help="filter to these scopes")
    sc.add_argument("--write-doc", action="store_true", help="regenerate docs/RBAC_NODE_MAPPING.md")
    sc.set_defaults(func=cmd_scopes)

    rc = sub.add_parser("reconcile-deletes", help="remove nodes whose source row is gone")
    rc.add_argument("--org", help="organization_id (default: all active)")
    rc.add_argument("--label", action="append", help="limit to these labels")
    rc.add_argument("--apply", action="store_true", help="delete (default is a dry run)")
    rc.set_defaults(func=cmd_reconcile_deletes)

    rq = sub.add_parser("replay-quarantine")
    rq.add_argument("--date", required=True)
    rq.set_defaults(func=cmd_replay)

    args = p.parse_args()
    setup_logging(args.log_level)
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"CONFIG ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())