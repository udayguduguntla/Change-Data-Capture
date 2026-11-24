#!/usr/bin/env python3
"""Move the pipeline's consumer group to a point in time, so it skips the old Kafka history.

    python scripts/set_group_start.py --at "2026-09-25 09:00"   # local time
    python scripts/set_group_start.py --latest                  # only new changes from now on

Use it together with `main.py backfill`: backfill loads the current rows from Postgres,
and the pipeline replays only the changes made after --at. Pick a time a little
BEFORE the backfill started so nothing is missed (replays are idempotent).
Stop ./run_pipeline.sh first. Reads and writes Kafka offsets only.
"""
import argparse
import os
import sys
from datetime import datetime

import yaml
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--at", help='local time, e.g. "2026-09-25 09:00"')
    g.add_argument("--latest", action="store_true")
    a = ap.parse_args()
    os.chdir(ROOT)
    load_dotenv(".env")
    kafka = yaml.safe_load(open("config/connections.yaml"))["kafka"]
    group, prefix, bootstrap = kafka["group_id"], kafka["topic_prefix"], os.environ["KAFKA_BOOTSTRAP"]

    desc = AdminClient({"bootstrap.servers": bootstrap}).describe_consumer_groups([group])[group]
    try:
        if desc.result(timeout=15).members:
            sys.exit(f"group {group} has active members; stop ./run_pipeline.sh first")
    except Exception:
        pass

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
    md = c.list_topics(timeout=30)
    tps = [TopicPartition(t, p) for t in sorted(md.topics) if t.startswith(prefix + ".")
           for p in md.topics[t].partitions]
    if a.latest:
        target = [TopicPartition(tp.topic, tp.partition, c.get_watermark_offsets(tp, timeout=10)[1]) for tp in tps]
    else:
        ms = int(datetime.strptime(a.at, "%Y-%m-%d %H:%M").timestamp() * 1000)
        found = c.offsets_for_times([TopicPartition(tp.topic, tp.partition, ms) for tp in tps], timeout=30)
        target = []
        for tp in found:
            high = c.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=10)[1]
            target.append(TopicPartition(tp.topic, tp.partition, tp.offset if tp.offset >= 0 else high))
    skipped = 0
    for tp in target:
        low, high = c.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=10)
        skipped += max(0, tp.offset - low)
    for i in range(0, len(target), 500):
        c.commit(offsets=target[i:i + 500], asynchronous=False)
    c.close()
    print(f"group {group}: {len(target)} partitions moved; {skipped:,} older events will not be replayed")


if __name__ == "__main__":
    main()
