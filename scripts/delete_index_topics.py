#!/usr/bin/env python3
"""Delete the empty cdc.* topics that were created for Postgres index names (..._idx, ..._pkey, ..._key).

    python scripts/delete_index_topics.py            # list only
    python scripts/delete_index_topics.py --delete   # delete them

Only topics whose name ends in _idx/_pkey/_key AND that hold zero messages are touched.
"""
import argparse
import os
import re

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

INDEX_NAME = re.compile(r"^cdc\..+_(idx|pkey|key)$")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delete", action="store_true")
    a = ap.parse_args()
    load_dotenv(".env")
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "index-topic-check"})
    md = c.list_topics(timeout=30)
    targets = []
    for topic in sorted(t for t in md.topics if INDEX_NAME.match(t)):
        total = 0
        for p in md.topics[topic].partitions:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            total += high - low
        if total == 0:
            targets.append(topic)
        else:
            print(f"kept (has {total} messages): {topic}")
    c.close()
    print(f"{len(targets)} empty index topics" + ("" if a.delete else " (run with --delete to remove)"))
    for t in targets:
        print("  ", t)
    if a.delete and targets:
        for topic, fut in AdminClient({"bootstrap.servers": bootstrap}).delete_topics(targets).items():
            try:
                fut.result()
            except Exception as exc:
                print(f"could not delete {topic}: {exc}")
        print("deleted")


if __name__ == "__main__":
    main()
