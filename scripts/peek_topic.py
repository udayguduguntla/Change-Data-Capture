#!/usr/bin/env python3
"""Show the latest Debezium events on a table's Kafka topic(s). Reads Kafka only.

    python scripts/peek_topic.py cdp.customer_profile
    python scripts/peek_topic.py cdp.customer_profile --find "some value" --last 500
"""
import argparse
import json
import os
from datetime import datetime

from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("table", help="schema.table, e.g. cdp.customer_profile")
    ap.add_argument("--find", help="only show events whose JSON contains this text")
    ap.add_argument("--last", type=int, default=20, help="how many recent events to scan per topic")
    a = ap.parse_args()
    load_dotenv(".env")
    c = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "group.id": "peek-topic",
                  "enable.auto.commit": False})
    base = f"cdc.{a.table}"
    topics = sorted(t for t in c.list_topics(timeout=15).topics if t == base or t.startswith(base + "_"))
    if not topics:
        print(f"no topic {base}: Debezium has never written this table (check table.include.list)")
        return
    for topic in topics:
        low, high = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
        print(f"\n== {topic}  events kept: {high - low}  newest offset: {high - 1}")
        if high <= low:
            continue
        start = max(low, high - a.last)
        c.assign([TopicPartition(topic, 0, start)])
        shown = 0
        while start < high:
            m = c.poll(10)
            if m is None:
                break
            start = m.offset() + 1
            if m.error() or m.value() is None:
                continue
            raw = m.value().decode("utf-8", "replace")
            if a.find and a.find not in raw:
                continue
            p = json.loads(raw).get("payload", {})
            when = datetime.fromtimestamp(p.get("ts_ms", 0) / 1000).strftime("%Y-%m-%d %H:%M:%S")
            print(f"offset {m.offset()}  op={p.get('op')}  captured={when}")
            if a.find:
                print(json.dumps(p.get("after") or p.get("before"), indent=1, default=str)[:1200])
            shown += 1
        if a.find and not shown:
            print(f"'{a.find}' not found in the last {a.last} events")
        c.unassign()
    c.close()


if __name__ == "__main__":
    main()
