#!/usr/bin/env python3
"""Wipe the tenant graphs and/or rewind the consumer group to the earliest Kafka offsets.

    python scripts/reset_graph.py                # both
    python scripts/reset_graph.py --kafka-only   # only rewind offsets
    python scripts/reset_graph.py --graph-only   # only wipe org* databases

Stop ./run_pipeline.sh first: Kafka refuses offset changes while the group has members.
"""
import argparse
import os
import sys

import yaml
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def wipe_graphs():
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(os.environ["NEO4J_URI"],
                                  auth=(os.getenv("NEO4J_USER", "neo4j"), os.environ["NEO4J_PASSWORD"]))
    with driver.session(database="system") as s:
        dbs = [r["name"] for r in s.run("SHOW DATABASES YIELD name WHERE name STARTS WITH 'org' RETURN DISTINCT name")]
    for db in sorted(dbs):
        print(f"wiping {db} ...")
        with driver.session(database=db) as session:
            for c in list(session.run("SHOW CONSTRAINTS YIELD name RETURN name")):
                session.run(f"DROP CONSTRAINT `{c['name']}` IF EXISTS").consume()
            while session.run("MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(*) AS c").single()["c"]:
                pass
    driver.close()


def rewind_offsets(group, prefix):
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]

    desc = AdminClient({"bootstrap.servers": bootstrap}).describe_consumer_groups([group])[group]
    try:
        members = desc.result(timeout=15).members
    except Exception:
        members = []
    if members:
        sys.exit(f"group {group} still has {len(members)} active member(s); stop ./run_pipeline.sh and retry")

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
    md = c.list_topics(timeout=30)
    tps = []
    for topic in sorted(t for t in md.topics if t.startswith(prefix + ".")):
        for p in md.topics[topic].partitions:
            low, _high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            tps.append(TopicPartition(topic, p, low))
    for i in range(0, len(tps), 500):
        c.commit(offsets=tps[i:i + 500], asynchronous=False)
    c.close()
    print(f"group {group}: {len(tps)} partitions rewound to their earliest offset")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka-only", action="store_true")
    ap.add_argument("--graph-only", action="store_true")
    ap.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    a = ap.parse_args()
    os.chdir(ROOT)
    load_dotenv()
    kafka = yaml.safe_load(open("config/connections.yaml"))["kafka"]
    if not a.yes and input("This deletes graph data / rewinds offsets. Type 'yes': ").strip() != "yes":
        return
    if not a.kafka_only:
        wipe_graphs()
    if not a.graph_only:
        rewind_offsets(kafka["group_id"], kafka["topic_prefix"])
    print("done. next: python main.py provision --all, then ./run_pipeline.sh")


if __name__ == "__main__":
    main()
