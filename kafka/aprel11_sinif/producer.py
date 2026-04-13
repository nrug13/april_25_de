"""
Producer examples — the real DE way (no bash kafka-console-producer).

Key concepts shown:
  - acks="all"        → wait for all ISR replicas (safest)
  - linger.ms         → micro-batching for throughput
  - delivery callback → async confirmation per message
  - message key       → same key always goes to same partition (ordering guarantee)
  - headers           → attach metadata without affecting partitioning
  - manual partition  → force a message to a specific partition
"""
import json
import time
from confluent_kafka import Producer, KafkaException

BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"
TOPIC = "orders"

producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",              # leader + all ISR replicas must confirm
        "retries": 5,               # retry on transient errors
        "retry.backoff.ms": 500,
        "linger.ms": 10,            # wait 10 ms to batch messages before sending
        "batch.size": 16384,        # max batch size in bytes
        "compression.type": "snappy",  # snappy is a good balance of speed + ratio
    }
)


def delivery_callback(err, msg):
    """
    Kafka calls this once per message after it is acked (or fails).
    Always define this — silent failures are hard to debug in production.
    """
    if err:
        print(f"[FAIL] delivery error: {err}")
    else:
        print(
            f"[OK] topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"key={msg.key().decode() if msg.key() else None}"
        )


# ─── 1. SIMPLE PRODUCE ────────────────────────────────────────────────────────

def produce_simple(key: str, value: dict):
    """
    Produce one message.
    The key determines which partition the message goes to
    (same key → same partition → ordered delivery for that key).
    """
    producer.produce(
        topic=TOPIC,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
        callback=delivery_callback,
    )
    # poll(0) flushes the internal delivery-report queue without blocking
    producer.poll(0)


# ─── 2. PRODUCE WITH HEADERS ──────────────────────────────────────────────────

def produce_with_headers(key: str, value: dict, headers: dict):
    """
    Headers are key-value metadata attached to the message.
    Useful for routing, tracing IDs, schema versions, source system, etc.
    They do NOT affect partitioning.
    """
    producer.produce(
        topic=TOPIC,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
        headers=[(k, v.encode("utf-8")) for k, v in headers.items()],
        callback=delivery_callback,
    )
    producer.poll(0)


# ─── 3. PRODUCE TO SPECIFIC PARTITION ────────────────────────────────────────

def produce_to_partition(partition: int, key: str, value: dict):
    """
    Bypass key-based partitioning and force a message to a specific partition.
    Use sparingly — usually you want Kafka to balance load automatically.
    """
    producer.produce(
        topic=TOPIC,
        partition=partition,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
        callback=delivery_callback,
    )
    producer.poll(0)


# ─── 4. BATCH PRODUCE ────────────────────────────────────────────────────────

def produce_batch(messages: list[dict]):
    """
    Produce many messages quickly.
    linger.ms + batch.size handle the actual batching inside librdkafka.
    """
    for msg in messages:
        producer.produce(
            topic=TOPIC,
            key=str(msg["order_id"]).encode("utf-8"),
            value=json.dumps(msg).encode("utf-8"),
            callback=delivery_callback,
        )
        # poll periodically to drain the delivery-report queue
        producer.poll(0)

    # Block until all messages in the internal queue are delivered
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        print(f"[WARN] {remaining} messages were not delivered within timeout")


# ─── DEMO ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Simple messages — same user_id key → same partition → ordering preserved
    for i in range(6):
        order = {
            "order_id": i,
            "user_id": f"user_{i % 3}",   # 3 users → 3 partition groups
            "product": f"item_{i}",
            "amount": round(i * 9.99, 2),
            "ts": time.time(),
        }
        produce_simple(key=order["user_id"], value=order)

    # 2. Message with headers (tracing, source tagging)
    produce_with_headers(
        key="user_99",
        value={"order_id": 99, "product": "vip_item", "amount": 999.0, "ts": time.time()},
        headers={"source": "mobile-app", "trace_id": "abc-123", "schema_version": "v2"},
    )

    # 3. Force to partition 0
    produce_to_partition(
        partition=0,
        key="user_0",
        value={"order_id": 100, "note": "forced to partition 0", "ts": time.time()},
    )

    # 4. Batch
    batch = [
        {"order_id": 200 + i, "user_id": "user_bulk", "amount": i * 1.5, "ts": time.time()}
        for i in range(10)
    ]
    produce_batch(batch)

    producer.flush()
    print("\nAll messages produced.")
