#!/usr/bin/env python3
"""
One-shot health check: is data reaching Kafka, is the consumer reading it, is it landing in Neo4j, and are the embeddings there with the right size?
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv  # noqa: E402

from src.config_loader import ConfigLoader  # noqa: E402
from src.embedder import embedding_config, model_tag  # noqa: E402
from src.embedding_worker import targets_for  # noqa: E402


def kafka_section(loader):
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient
    k = loader.section("kafka")
    group, prefix = k["group_id"], k["topic_prefix"] + "."
    print(f"\n=== KAFKA  bootstrap={k['bootstrap_servers']}  group={group}")

    admin = AdminClient({"bootstrap.servers": k["bootstrap_servers"]})
    try:
        g = admin.describe_consumer_groups([group])[group].result(timeout=10)
        print(f"group state={g.state}  members={len(g.members)}")
        for mem in g.members:
            n = len(mem.assignment.topic_partitions) if mem.assignment else 0
            print(f"  member host={mem.host} client={mem.client_id} partitions={n}")
        if len(g.members) > 1:
            print("  !! more than one member: another `main.py run` (old terminal, "
                  "run_dummy_test.sh, ...) is sharing the partitions")
    except Exception as exc:
        print(f"group describe failed: {exc}")

    c = Consumer({"bootstrap.servers": k["bootstrap_servers"], "group.id": group,
                  "enable.auto.commit": False})
    md = c.list_topics(timeout=15)
    tps = [TopicPartition(t, p) for t in sorted(md.topics) if t.startswith(prefix)
           for p in md.topics[t].partitions]
    committed = {(tp.topic, tp.partition): tp.offset for tp in c.committed(tps, timeout=30)}
    kinds = Counter()
    rows = []
    for tp in tps:
        lo, hi = c.get_watermark_offsets(tp, timeout=10)
        com = committed.get((tp.topic, tp.partition), -1001)
        if hi == 0:
            kind = "never-written"
        elif lo == hi:
            kind = "EMPTY-all-expired"          # data existed, retention deleted it
        elif com < 0:
            kind = "not-started"
        elif com >= hi:
            kind = "caught-up"
        else:
            kind = "LAGGING"
        kinds[kind] += 1
        rows.append((tp.topic, lo, hi, com, max(0, hi - max(com, lo)), kind))
    c.close()
    print(f"{len(tps)} partitions: {dict(kinds)}")
    trimmed = [r for r in rows if r[1] > 0]
    print(f"{len(trimmed)} partitions have low watermark > 0 (oldest events already "
          f"deleted by retention -> a replay from Kafka CANNOT rebuild those tables; "
          f"use `main.py backfill`)")
    print(f"\n{'topic':55s} {'low':>9s} {'high':>9s} {'committed':>10s} {'lag':>9s}  status")
    for r in sorted(rows, key=lambda r: -r[4])[:40]:
        if r[5] in ("never-written",):
            continue
        print(f"{r[0]:55s} {r[1]:9d} {r[2]:9d} {r[3]:10d} {r[4]:9d}  {r[5]}")


def connect_section():
    import requests
    url = os.getenv("CONNECT_URL", "http://localhost:8083").rstrip("/")
    print(f"\n=== DEBEZIUM  {url}")
    try:
        st = requests.get(f"{url}/connectors/cdc-postgres-source/status", timeout=5).json()
        print("connector:", st.get("connector", {}).get("state"),
              "tasks:", [t.get("state") for t in st.get("tasks", [])])
        for t in st.get("tasks", []):
            if t.get("state") == "FAILED":
                print(t.get("trace", "")[:1500])
    except Exception as exc:
        print(f"could not reach Connect: {exc}")


def neo4j_section(loader, only_org):
    from neo4j import GraphDatabase
    n = loader.section("neo4j")
    inst = n["instances"][n.get("default_instance") or next(iter(n["instances"]))]
    cfg = embedding_config(loader)
    dim, tag = int(cfg["dimensions"]), model_tag(cfg)
    drv = GraphDatabase.driver(inst["uri"], auth=(inst["user"], inst["password"]))
    print(f"\n=== NEO4J  {inst['uri']}  expected vectors: {tag}")
    with drv.session(database="system") as s:
        dbs = [r["name"] for r in s.run(
            "SHOW DATABASES YIELD name, currentStatus WHERE name STARTS WITH 'org' "
            "RETURN DISTINCT name")]
    for db in sorted(dbs):
        if only_org and db != f"org{only_org}":
            continue
        try:
            with drv.session(database=db) as s:
                total = s.run("MATCH (n) RETURN count(n) AS c").single()["c"]
                print(f"\n{db}: {total:,} nodes")
                idx = {r["name"]: r for r in s.run(
                    "SHOW VECTOR INDEXES YIELD name, state, populationPercent, options "
                    "RETURN name, state, populationPercent, options")}
                for t in targets_for(loader, db):
                    q = (f"MATCH (n:`{t.label}`) RETURN count(n) AS total, "
                         f"count(n.`{t.prop}`) AS with_vec, "
                         f"sum(CASE WHEN n._embed_pending THEN 1 ELSE 0 END) AS pending, "
                         f"sum(CASE WHEN n.`{t.prop}` IS NOT NULL AND size(n.`{t.prop}`) <> $dim "
                         f"THEN 1 ELSE 0 END) AS wrong_dim")
                    r = s.run(q, dim=dim).single()
                    if not r["total"]:
                        continue
                    name = f"{t.label.lower()}_{t.prop}_vector"
                    i = idx.get(name)
                    idim = (i["options"] or {}).get("indexConfig", {}).get("vector.dimensions") if i else None
                    flag = ""
                    if idim is not None and idim != dim:
                        flag += f"  !! index dims={idim}"
                    if r["wrong_dim"]:
                        flag += "  !! old-size vectors present"
                    print(f"  {t.label:18s} total={r['total']:>9,} vectors={r['with_vec']:>9,} "
                          f"pending={r['pending']:>9,} index={(i['state'] if i else 'MISSING')}"
                          f"/{idim}{flag}")
        except Exception as exc:
            print(f"{db}: ERROR {exc}")
    drv.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config")
    ap.add_argument("--org")
    ap.add_argument("--skip-kafka", action="store_true")
    ap.add_argument("--skip-neo4j", action="store_true")
    a = ap.parse_args()
    load_dotenv()
    loader = ConfigLoader(a.config)
    if not a.skip_kafka:
        connect_section()
        kafka_section(loader)
    if not a.skip_neo4j:
        neo4j_section(loader, a.org)


if __name__ == "__main__":
    main()
