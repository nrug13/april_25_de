"""
Consumer examples — the real DE way (no bash kafka-console-consumer).

Key concepts shown:
  - Consumer group     → Kafka balances partitions across all consumers in a group
  - Manual commit      → you control when an offset is marked "processed"
  - Rebalance callback → react when partitions are assigned / revoked
  - Seek               → jump to any offset (replay, skip ahead)
  - Assign (no group)  → read a specific partition directly, no group coordination
  - Lag check          → how far behind is the consumer?
"""
import json
from confluent_kafka import Consumer, KafkaException, TopicPartition, OFFSET_BEGINNING

BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"
TOPIC = "orders"
GROUP_ID = "orders-consumer-group"


# ─── 1. CONSUME WITH MANUAL COMMIT (production pattern) ──────────────────────

def consume_with_manual_commit():
    """
    The safest pattern:
      - enable.auto.commit = false  → you decide when an offset is "done"
      - commit AFTER successful processing, not before
      - if the process crashes, Kafka re-delivers from the last commit
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",  # start from the beginning if no prior commit
            "enable.auto.commit": False,       # manual commit for at-least-once guarantee
            "max.poll.interval.ms": 300000,    # 5 min max between polls (increase for slow processing)
            "session.timeout.ms": 30000,
        }
    )

    def on_assign(_consumer, partitions):
        print(f"[REBALANCE] Assigned: {[(p.topic, p.partition) for p in partitions]}")

    def on_revoke(consumer, partitions):
        print(f"[REBALANCE] Revoked:  {[(p.topic, p.partition) for p in partitions]}")
        consumer.commit()   # commit before losing the partitions

    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    print(f"Listening on '{TOPIC}' (group='{GROUP_ID}'). Ctrl-C to stop.\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            # ── deserialize ──────────────────────────────────────────────────
            key     = msg.key().decode("utf-8") if msg.key() else None
            value   = json.loads(msg.value().decode("utf-8"))
            headers = {k: v.decode() for k, v in (msg.headers() or [])}

            print(
                f"partition={msg.partition():>2}  offset={msg.offset():>6}  "
                f"key={key:<12}  value={value}  headers={headers}"
            )

            # ── your business logic here ──────────────────────────────────────
            # process_order(value)

            # ── commit only after successful processing ───────────────────────
            consumer.commit(message=msg)

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        consumer.close()


# ─── 2. READ FROM SPECIFIC PARTITION (no group, no offset tracking) ───────────

def consume_specific_partition(partition: int, start_offset: int = OFFSET_BEGINNING):
    """
    Bypass the consumer group protocol and read a single partition directly.
    Great for debugging, backfilling, or ad-hoc inspection.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "debug-reader",         # still required, but not tracked
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    tp = TopicPartition(TOPIC, partition, start_offset)
    consumer.assign([tp])
    print(f"Reading partition={partition} from offset={start_offset}\n")

    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                print("No more messages in this partition.")
                break
            if msg.error():
                raise KafkaException(msg.error())

            value = json.loads(msg.value().decode("utf-8"))
            print(f"  offset={msg.offset()}  value={value}")
    finally:
        consumer.close()


# ─── 3. SEEK — replay from a specific offset ──────────────────────────────────

def consume_with_seek(partition: int, seek_to_offset: int):
    """
    After subscribing, seek to any offset to replay or skip messages.
    Useful for reprocessing after a bug fix.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    # assign (not subscribe) so we control the exact partition
    tp = TopicPartition(TOPIC, partition)
    consumer.assign([tp])

    # seek MUST be called after assign
    consumer.seek(TopicPartition(TOPIC, partition, seek_to_offset))
    print(f"Seeked partition={partition} to offset={seek_to_offset}\n")

    try:
        for _ in range(10):
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())
            value = json.loads(msg.value().decode("utf-8"))
            print(f"  offset={msg.offset()}  value={value}")
    finally:
        consumer.close()


# ─── 4. CHECK CONSUMER LAG ────────────────────────────────────────────────────

def check_lag(num_partitions: int = 3):
    """
    Compare committed offsets (where consumer is) vs end offsets (latest message).
    The difference is the lag — how many messages are waiting to be consumed.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
        }
    )

    partitions = [TopicPartition(TOPIC, p) for p in range(num_partitions)]
    committed  = consumer.committed(partitions, timeout=5)

    # get the high-watermark (latest offset) for each partition
    total_lag = 0
    print(f"\nConsumer lag for group='{GROUP_ID}' on topic='{TOPIC}':")
    for tp in committed:
        _, high = consumer.get_watermark_offsets(tp, timeout=5)
        committed_offset = tp.offset if tp.offset >= 0 else 0
        lag = high - committed_offset
        total_lag += lag
        print(f"  partition={tp.partition}  committed={tp.offset}  latest={high}  lag={lag}")

    print(f"  TOTAL LAG: {total_lag}")
    consumer.close()


# ─── DEMO ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Run the main consumer loop (Ctrl-C to stop)
    consume_with_manual_commit()

    # Other examples — uncomment to try:
    # consume_specific_partition(partition=0, start_offset=0)
    # consume_with_seek(partition=0, seek_to_offset=3)
    # check_lag(num_partitions=3)
