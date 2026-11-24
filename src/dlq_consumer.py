import json
import logging
import time
import threading
from confluent_kafka import Consumer

log = logging.getLogger(__name__)

class DLQConsumer:
    def __init__(self, loader, registry, router, writer, provisioner, quarantine, metrics, pg_factory):
        self.loader = loader
        self.registry = registry
        self.router = router
        self.writer = writer
        self.provisioner = provisioner
        self.quarantine = quarantine
        self.metrics = metrics
        self.pg_factory = pg_factory
        self.kafka_config = loader.section("kafka")
        self.topic = "cdc.quarantine"
        self._running = False

        self.consumer = Consumer({
            "bootstrap.servers": self.kafka_config.get("bootstrap_servers"),
            "group.id": self.kafka_config.get("group_id", "cdc-dlq-group") + "-dlq",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True
        })

    def bootstrap(self):
        log.info("Bootstrapping DLQ database table...")
        with self.pg_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS routing.quarantined_events (
                    id BIGSERIAL PRIMARY KEY,
                    topic TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    reason TEXT,
                    detail TEXT,
                    collapse_key TEXT,
                    retry_count INT DEFAULT 0,
                    next_retry_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
                """)
            conn.commit()
        log.info("DLQ table ready.")

    def run(self):
        self._running = True
        self.consumer.subscribe([self.topic])
        log.info("DLQ Consumer started, subscribed to %s", self.topic)

        # Start retry thread
        retry_thread = threading.Thread(target=self._retry_loop, daemon=True)
        retry_thread.start()

        while self._running:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Kafka error: %s", msg.error())
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                self._insert_to_pg(record)
            except Exception as e:
                log.error("Failed to process DLQ message: %s", e)

        self.consumer.close()

    def _insert_to_pg(self, record):
        with self.pg_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO routing.quarantined_events 
                    (topic, payload, reason, detail) 
                    VALUES (%s, %s, %s, %s)
                """, (
                    record.get("topic"),
                    json.dumps(record.get("payload")),
                    record.get("reason"),
                    record.get("detail")
                ))
            conn.commit()

    def _retry_loop(self):
        log.info("DLQ retry loop started.")
        while self._running:
            try:
                with self.pg_factory() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT id, topic, payload 
                            FROM routing.quarantined_events 
                            WHERE next_retry_at <= NOW()
                            LIMIT 100
                        """)
                        rows = cur.fetchall()

                        for row in rows:
                            row_id, topic, payload = row
                            success = self._attempt_retry(topic, payload)
                            if success:
                                cur.execute("DELETE FROM routing.quarantined_events WHERE id = %s", (row_id,))
                            else:
                                cur.execute("""
                                    UPDATE routing.quarantined_events 
                                    SET retry_count = retry_count + 1,
                                        next_retry_at = NOW() + (INTERVAL '1 minute' * POWER(2, retry_count))
                                    WHERE id = %s
                                """, (row_id,))
                    conn.commit()
            except Exception as e:
                log.error("Error in retry loop: %s", e)

            time.sleep(10)

    def _attempt_retry(self, topic, payload) -> bool:
        from src.router import RoutedEvent
        try:
            decision = self.router.route(topic, payload)
            if isinstance(decision, RoutedEvent) or isinstance(decision, list):
                decisions = decision if isinstance(decision, list) else [decision]
                for d in decisions:
                    if isinstance(d, RoutedEvent):
                        if d.op == "delete":
                            log.info("Discarding DLQ DELETE event (canceled): %s", d.collapse_key)
                            continue
                        self.writer.add(d)
                self.writer.flush_all()
                return True
            return False
        except Exception:
            return False
