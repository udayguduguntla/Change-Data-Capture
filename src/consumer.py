"""The Kafka consumer loop.  Spec section 8."""

from __future__ import annotations

import json
import logging
import os
import signal
import time

from .router import ControlEvent, Parked, Quarantined, RoutedEvent, Skipped

log = logging.getLogger(__name__)


class Consumer:
    def __init__(self, loader, registry, router, writer, provisioner, quarantine,
                 metrics, pg_conn_factory=None):
        self.loader = loader
        self.registry = registry
        self.router = router
        self.writer = writer
        self.provisioner = provisioner
        self.quarantine = quarantine
        self.metrics = metrics
        self.pg_conn_factory = pg_conn_factory
        self._running = False
        self._consumer = None
        self.start_from_now = False       # set by `run --from-now`
        self._jumped: set = set()


    def _build_consumer(self):
        from confluent_kafka import Consumer as KafkaConsumer

        k = self.loader.section("kafka")
        conf = {
            "bootstrap.servers": k["bootstrap_servers"],
            "group.id": k["group_id"],
            "auto.offset.reset": k.get("auto_offset_reset", "earliest"),
            "max.poll.interval.ms": k.get("max_poll_interval_ms", 1800000),
            "enable.auto.commit": False,          # never true; see module docstring
            "enable.partition.eof": False,
            "log.connection.close": "false",
            "log_level": k.get("log_level", 3),
            # Small fetches keep events flowing on a slow link instead of one huge reply.
            "fetch.message.max.bytes": int(k.get("fetch_partition_max_bytes", 262144)),
            "fetch.max.bytes": int(k.get("fetch_max_bytes", 5242880)),
        }
        if k.get("enable_auto_commit"):
            raise ValueError(
                "kafka.enable_auto_commit must be false. Offsets are committed by "
                "watermark only after Neo4j has accepted the write."
            )
        debug = os.getenv("KAFKA_DEBUG")
        if debug:
            # librdkafka debug output goes to stderr: KAFKA_DEBUG=cgrp,fetch ./run_pipeline.sh 2> kafka_debug.log
            conf.update({"debug": debug, "log_level": 7})
            consumer = KafkaConsumer(conf)
        else:
            consumer = KafkaConsumer(conf, logger=logging.getLogger("rdkafka"))
        prefix = k["topic_prefix"]
        topics = sorted({f"{prefix}.{t}" for t in self._subscribed_tables()})
        self._ensure_topics(topics)
        log.info("subscribing to %d topics: %s", len(topics), topics)
        if self.start_from_now:
            consumer.subscribe(topics, on_assign=self._jump_to_end)
        else:
            consumer.subscribe(topics)
        return consumer

    def _ensure_topics(self, topics: list[str]) -> None:
        try:
            from confluent_kafka.admin import AdminClient, NewTopic
            k = self.loader.section("kafka")
            admin = AdminClient({"bootstrap.servers": k["bootstrap_servers"]})
            meta = admin.list_topics(timeout=5)
            existing = set(meta.topics.keys())
            missing = set(topics) - existing
            if missing:
                log.info("Auto-creating %d missing topics: %s", len(missing), sorted(missing))
                new_topics = [NewTopic(t, num_partitions=1, replication_factor=1) for t in missing]
                futures = admin.create_topics(new_topics)
                for topic, fut in futures.items():
                    try:
                        fut.result()
                    except Exception as e:
                        log.debug("Topic %s creation notice: %s", topic, e)
        except Exception as e:
            log.warning("Could not pre-verify Kafka topics: %s", e)

    def _jump_to_end(self, consumer, partitions) -> None:
        """`run --from-now`: skip everything already in the topics.

        On first assignment each partition starts at its current end and that position is
        committed straight away, so a later restart without --from-now continues from
        here instead of replaying the old backlog. Partitions assigned again after a
        rebalance keep their committed position (only the first assignment jumps).
        """
        jumped = []
        for p in partitions:
            ident = (p.topic, p.partition)
            if ident in self._jumped:
                continue
            _low, high = consumer.get_watermark_offsets(p, timeout=10, cached=False)
            p.offset = high
            self._jumped.add(ident)
            jumped.append(p)
        consumer.assign(partitions)
        if jumped:
            consumer.commit(offsets=jumped, asynchronous=False)
            log.info("--from-now: %d partition(s) start at their current end; older events skipped",
                     len(jumped))

    def _subscribed_tables(self) -> set[str]:
        tables = set(self.loader.core_tables)
        tables.add(self.loader.section("control_plane")["organisations_table"])
        for db in {p.stem for p in self.loader.overlay_files()}:
            tables.update(self.loader.effective(db).tables)
        if getattr(self.router, "partition_map", None):
            tables.update(self.router.partition_map.keys())
        return tables


    def bootstrap(self) -> None:
        """Load the registry and provision every active organisation.  Spec 5.1."""
        if self.pg_conn_factory:
            with self.pg_conn_factory() as conn:
                n = self.registry.load_from_postgres(conn)
                from .registry import apply_industries
                apply_industries(self.loader, self.registry)
                p = self.router.load_partition_map_from_postgres(conn)
            log.info("registry loaded: %d organisation(s), %d partition table(s)", n, p)
        for org in self.registry.active():
            from .provision import provision_organisation
            provision_organisation(self.provisioner, org, self.registry)
            eff = self.provisioner.loader.effective(org.database)
            self.registry.mark_provisioned(org.tenant_id, eff.products)
            org.provisioned = True
            self.writer.warm_cache(org.database)
            if self.metrics:
                self.metrics.inc("provisioning_total", result="ok")

    def run(self) -> None:
        self._consumer = self._build_consumer()
        self._running = True
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._stop)

        k = self.loader.section("kafka")
        batch_size = k.get("max_poll_records", 5000)
        log.info("consumer started")

        # --- Start Debezium Health Monitor ---
        import threading
        import requests
        import os

        def monitor_debezium():
            connect_url = os.getenv("CONNECT_URL", "http://localhost:8083").rstrip("/")
            url = f"{connect_url}/connectors/cdc-postgres-source/status"
            while self._running:
                try:
                    resp = requests.get(url, timeout=5)
                    if resp.status_code == 200:
                        data = resp.json()
                        for task in data.get("tasks", []):
                            if task.get("state") == "FAILED":
                                trace = task.get("trace", "")
                                log.error("🚨 CRITICAL: DEBEZIUM CONNECTOR CRASHED! 🚨\nIt is no longer reading from PostgreSQL.\nReason:\n%s", trace)
                    else:
                        log.warning("Debezium health check failed with HTTP %s", resp.status_code)
                except Exception as e:
                    log.warning("Could not reach Debezium to check health at %s. Error: %s", url, e)

                # Check every 60 seconds
                for _ in range(60):
                    if not self._running:
                        break
                    time.sleep(1)

        monitor_thread = threading.Thread(target=monitor_debezium, daemon=True)
        monitor_thread.start()

        retry_delay = 1.0
        max_retry_delay = 30.0

        try:
            while self._running:
                try:
                    if not getattr(self, "_first_msg_received", False):
                        if time.time() - getattr(self, "_last_heartbeat", 0) > 30:
                            log.info("No message received yet: waiting for partition assignment, "
                                     "or every partition is already caught up. Check with scripts/diagnose_pipeline.py")
                            self._last_heartbeat = time.time()
                    messages = self._consumer.consume(num_messages=batch_size, timeout=1.0)
                    if messages:
                        self._first_msg_received = True
                        self._handle_batch(messages)
                    self.writer.flush_due()
                    self._commit()
                    retry_delay = 1.0  # Reset delay on successful cycle
                except Exception as loop_exc:
                    log.error(
                        "Transient error in consumer loop: %s. Backing off for %.1fs before retry...",
                        loop_exc,
                        retry_delay,
                        exc_info=True,
                    )
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2.0, max_retry_delay)
        finally:
            self.shutdown()

    def _stop(self, *_):
        log.info("shutdown signal received; draining")
        self._running = False

    def shutdown(self) -> None:
        try:
            self.writer.flush_all()
        except Exception as exc:
            log.warning("Flush all during shutdown failed: %s", exc)
        try:
            self._commit()
        except Exception as exc:
            log.warning("Commit during shutdown failed: %s", exc)
        finally:
            if self._consumer:
                try:
                    self._consumer.close()
                except Exception as exc:
                    log.warning("Consumer close failed: %s", exc)
            log.info("consumer stopped")

    # -- per-batch ----------------------------------------------------------

    def _handle_batch(self, messages) -> None:
        control, data = [], []
        for msg in messages:
            if msg is None:
                continue
            if msg.error():
                err = msg.error()
                from confluent_kafka import KafkaError
                if err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.warning("kafka topic missing (waiting for Debezium): %s", err)
                else:
                    log.error("kafka error: %s", err)
                continue
            (control if self.router.is_control_topic(msg.topic()) else data).append(msg)

        for msg in control:
            self._handle_message(msg)
        for msg in data:
            self._handle_message(msg)

    def _handle_message(self, msg) -> None:
        start_time = time.time()
        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        log.info("Received message from Kafka: topic=%s, partition=%s, offset=%s", topic, partition, offset)
        self.writer.offsets.observe(topic, partition, offset)
        if self.metrics:
            self.metrics.inc("events_consumed_total", topic=topic)

        raw = msg.value()
        if raw is None:
            return                     # tombstone; tombstones.on.delete is false
        try:
            message = json.loads(raw)
            # Try to extract the Postgres change timestamp from Debezium payload
            pg_time = None
            if isinstance(message, dict):
                payload = message.get("payload", message)
                if isinstance(payload, dict) and "source" in payload:
                    source = payload["source"]
                    if isinstance(source, dict) and "ts_ms" in source:
                        pg_time = source["ts_ms"]
            if pg_time:
                pg_time_s = pg_time / 1000.0
                delay = start_time - pg_time_s
                log.info("Postgres change detected: topic=%s, pg_time=%.3f, delay_to_kafka_receive=%.3f seconds", topic, pg_time_s, delay)
        except (json.JSONDecodeError, TypeError) as exc:
            log.error("Failed to decode message: topic=%s, offset=%s, err=%s", topic, offset, exc)
            self.quarantine.write(topic, repr(raw)[:2000], "undecodable_message",
                                  str(exc), partition, offset)
            if self.metrics:
                self.metrics.inc("events_quarantined_total", reason="undecodable_message")
            return

        log.info("Message decoded successfully, routing: topic=%s, offset=%s", topic, offset)
        route_start = time.time()
        try:
            decision = self.router.route(topic, message, partition, offset)
        except Exception as exc:
            log.error("Routing error for topic=%s, offset=%s: %s", topic, offset, exc, exc_info=True)
            self.quarantine.write(topic, message, "router_error", str(exc), partition, offset)
            if self.metrics:
                self.metrics.inc("events_quarantined_total", reason="router_error")
            return

        route_time = time.time() - route_start
        log.info("Routing decision: %s, took %.4f seconds", type(decision).__name__, route_time)
        decisions = decision if isinstance(decision, list) else [decision]
        for dec in decisions:
            try:
                self._handle_decision(dec, topic, message, partition, offset, start_time)
            except Exception as exc:
                log.error("Decision handling error for topic=%s, offset=%s: %s", topic, offset, exc, exc_info=True)
                self.quarantine.write(topic, message, "handler_error", str(exc), partition, offset)
                if self.metrics:
                    self.metrics.inc("events_quarantined_total", reason="handler_error")

    def _handle_decision(self, decision, topic, message, partition, offset, start_time) -> None:
        if isinstance(decision, ControlEvent):
            self._handle_control(decision)
            log.info("Handled ControlEvent for topic=%s", topic)

            # Route it a second time as data so the Tenant node is actually written to the graph
            data_decision = self.router.route(topic, message, partition, offset, bypass_control=True)
            if isinstance(data_decision, RoutedEvent):
                self.writer.add(data_decision)
                log.info("Also routed ControlEvent as a regular node (Tenant)")
            return
        if isinstance(decision, Quarantined):
            log.warning("Message quarantined: topic=%s, reason=%s", topic, decision.reason)
            self.quarantine.write(topic, message, decision.reason, decision.detail,
                                  partition, offset)
            if self.metrics:
                self.metrics.inc("events_quarantined_total", reason=decision.reason)
            return
        if isinstance(decision, Skipped):
            log.info("Message skipped: topic=%s, reason=%s", topic, decision.reason)
            if self.metrics:
                self.metrics.inc("events_skipped_total", reason=decision.reason)
            return
        if isinstance(decision, Parked):
            log.info("Message parked waiting for parent %s[%s]: topic=%s, reason=%s",
                     decision.parent_table, decision.parent_id, topic, decision.reason)
            if self.metrics:
                self.metrics.inc("events_parked_total", table=topic)
            return
        if isinstance(decision, RoutedEvent):
            # Writer.add buffers event.companion too (properties_on renames).
            if decision.companion is not None and self.metrics:
                self.metrics.inc("attribute_renames_total", table=decision.table)
            log.info("Adding RoutedEvent to writer buffer: database=%s, table=%s", decision.database, decision.table)
            self.writer.add(decision)

            log.info("Finished processing message: topic=%s, offset=%s, total_time=%.4f seconds", topic, offset, time.time() - start_time)

    def _handle_control(self, event: ControlEvent) -> None:
        """Spec 5.2.  Provision before activating, never the other way round."""
        try:
            org = self.registry.upsert_from_event(event.op, event.before, event.after)
        except Exception as exc:
            log.error("Failed to parse control event: %s", exc, exc_info=True)
            return

        if org is None or not org.active:
            return
        if not org.provisioned:
            try:
                from .provision import provision_organisation
                provision_organisation(self.provisioner, org, self.registry)
                eff = self.provisioner.loader.effective(org.database)
                self.registry.mark_provisioned(org.tenant_id, eff.products)
                self.writer.warm_cache(org.database)
                if self.metrics:
                    self.metrics.inc("provisioning_total", result="ok")
            except Exception:
                log.exception("provisioning failed for organisation %s", org.organization_id)
                if self.metrics:
                    self.metrics.inc("provisioning_total", result="error")


    def _commit(self) -> None:
        from confluent_kafka import TopicPartition

        points = self.writer.offsets.commit_points()
        if not points:
            return
        tps = [TopicPartition(t, p, o) for (t, p), o in points.items()]
        try:
            self._consumer.commit(offsets=tps, asynchronous=True)
        except Exception:
            log.exception("offset commit failed; will retry next cycle")
            return
        if self.metrics:
            self.metrics.gauge("pending_offsets", self.writer.offsets.pending_count())

        # Sweep parked rows periodically
        if not hasattr(self, '_last_sweep'):
            self._last_sweep = time.time()
        if time.time() - self._last_sweep > 300:
            self._last_sweep = time.time()
            if self.router.tenant_resolver:
                def dlq_cb(child_table, payload, reason):
                    self.quarantine.write(self.router.topic_prefix + "." + child_table, payload, "expired_parked", reason, 0, 0)
                try:
                    self.router.tenant_resolver.sweep_parked(1, dlq_cb)
                except Exception as e:
                    log.error("Sweep parked failed: %s", e)

    def _report_lag(self) -> None:
        try:
            assignment = self._consumer.assignment()
            if not assignment:
                return
            total = 0
            for tp in assignment:
                lo, hi = self._consumer.get_watermark_offsets(tp, timeout=1.0, cached=True)
                pos = self._consumer.position([tp])[0].offset
                if pos is not None and pos >= 0 and hi is not None:
                    total += max(0, hi - pos)
            self.metrics.gauge("kafka_consumer_lag", total)
        except Exception:                                        # pragma: no cover
            pass


class MetricsLogger:
    """Periodic JSON metrics line, for deployments without Prometheus."""

    def __init__(self, metrics, interval: int = 60):
        self.metrics, self.interval = metrics, interval
        self._last = time.monotonic()

    def maybe_log(self) -> None:
        if time.monotonic() - self._last >= self.interval:
            self.metrics.log_line()
            self._last = time.monotonic()
