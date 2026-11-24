#!/usr/bin/env python3
"""Run the pipeline's exact Kafka consumer settings and topic list under a throwaway
group for up to 90 s and report what arrives. Reads Kafka only; the pipeline can keep running.

    python scripts/consumer_selftest.py 2> selftest_debug.log
"""
import ast
import collections
import json
import os
import sys
import time

from confluent_kafka import Consumer
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config_loader import ConfigLoader  # noqa: E402


def pipeline_topics():
    """The topic list the running pipeline logged, else the list from config."""
    try:
        for line in reversed(open("logs/pipeline.log", encoding="utf-8").read().splitlines()):
            if '"subscribing to' in line:
                msg = json.loads(line)["message"]
                return ast.literal_eval(msg[msg.index("["):])
    except (OSError, ValueError, KeyError):
        pass
    loader = ConfigLoader("config")
    prefix = loader.section("kafka")["topic_prefix"]
    return sorted(f"{prefix}.{t}" for t in loader.core_tables)


def main():
    load_dotenv(".env")
    k = ConfigLoader("config").section("kafka")
    topics = pipeline_topics()
    c = Consumer({
        "bootstrap.servers": k["bootstrap_servers"], "group.id": f"selftest-{int(time.time())}",
        "auto.offset.reset": "earliest", "max.poll.interval.ms": 1800000,
        "enable.auto.commit": False, "enable.partition.eof": False,
        "log.connection.close": "false", "debug": "cgrp,fetch,topic", "log_level": 7,
    })
    c.subscribe(topics)
    per_topic, errors, first_at, t0 = collections.Counter(), collections.Counter(), None, time.time()
    while time.time() - t0 < 90:
        for m in c.consume(num_messages=int(k.get("max_poll_records", 5000)), timeout=1.0):
            if m.error():
                errors[str(m.error())] += 1
            else:
                per_topic[m.topic()] += 1
                first_at = first_at or time.time() - t0
        if sum(per_topic.values()) >= 20000:
            break
    assigned = len(c.assignment())
    c.close()
    print(f"subscribed topics: {len(topics)}   assigned partitions: {assigned}")
    print(f"messages: {sum(per_topic.values())}   first message after: "
          f"{'%.1fs' % first_at if first_at else 'never'}")
    for topic, n in per_topic.most_common(10):
        print(f"   {n:7d}  {topic}")
    for err, n in errors.most_common(10):
        print(f"   error x{n}: {err}")


if __name__ == "__main__":
    main()
