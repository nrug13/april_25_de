"""
Admin operations via Python — no bash needed.
AdminClient lets DEs manage topics, partitions, and configs programmatically.
"""
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource
from confluent_kafka import KafkaException

BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"

admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


# ─── CREATE TOPIC ─────────────────────────────────────────────────────────────

def create_topic(topic_name: str, num_partitions: int = 3, replication_factor: int = 3):
    """Create a topic with explicit partition count and replication factor."""
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        config={
            "retention.ms": "86400000",    # keep messages for 1 day
            "cleanup.policy": "delete",    # delete old segments (vs compact)
            "min.insync.replicas": "2",    # at least 2 replicas must ack
        },
    )
    futures = admin.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"[OK] Created '{topic}'  partitions={num_partitions}  RF={replication_factor}")
        except KafkaException as e:
            print(f"[ERR] create_topic '{topic}': {e}")


# ─── LIST TOPICS ──────────────────────────────────────────────────────────────

def list_topics():
    """Print every topic, its partition count, and leader broker."""
    metadata = admin.list_topics(timeout=10)
    print(f"\nCluster ID : {metadata.cluster_id}")
    print(f"Brokers    : {[str(b) for b in metadata.brokers.values()]}")
    print(f"\nTopics ({len(metadata.topics)}):")
    for name, meta in sorted(metadata.topics.items()):
        if name.startswith("__"):          # skip internal topics
            continue
        print(f"  {name}  ({len(meta.partitions)} partitions)")


# ─── DESCRIBE TOPIC ───────────────────────────────────────────────────────────

def describe_topic(topic_name: str):
    """Show per-partition leader, replicas, and in-sync replicas (ISR)."""
    metadata = admin.list_topics(topic=topic_name, timeout=10)
    topic_meta = metadata.topics.get(topic_name)
    if not topic_meta:
        print(f"Topic '{topic_name}' not found.")
        return
    print(f"\nTopic: {topic_name}")
    for pid, part in sorted(topic_meta.partitions.items()):
        print(
            f"  partition={pid}  leader={part.leader}"
            f"  replicas={part.replicas}  isr={part.isrs}"
        )


# ─── ADD PARTITIONS ───────────────────────────────────────────────────────────

def add_partitions(topic_name: str, new_total: int):
    """
    Increase the partition count.
    Kafka only allows increasing — you cannot decrease partitions.
    """
    futures = admin.create_partitions([NewPartitions(topic_name, new_total)])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"[OK] '{topic}' partition count → {new_total}")
        except KafkaException as e:
            print(f"[ERR] add_partitions '{topic}': {e}")


# ─── DESCRIBE CONFIG ──────────────────────────────────────────────────────────

def describe_config(topic_name: str):
    """Read the current configuration of a topic."""
    resource = ConfigResource("topic", topic_name)
    futures = admin.describe_configs([resource])
    for res, future in futures.items():
        try:
            configs = future.result()
            print(f"\nConfig for '{topic_name}':")
            for key, entry in sorted(configs.items()):
                if not entry.is_default:   # only show non-default values
                    print(f"  {key} = {entry.value}")
        except KafkaException as e:
            print(f"[ERR] describe_config: {e}")


# ─── DELETE TOPIC ─────────────────────────────────────────────────────────────

def delete_topic(topic_name: str):
    """Permanently delete a topic and all its data."""
    futures = admin.delete_topics([topic_name])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"[OK] Deleted '{topic}'")
        except KafkaException as e:
            print(f"[ERR] delete_topic '{topic}': {e}")


# ─── DEMO ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    create_topic("orders", num_partitions=3, replication_factor=3)
    create_topic("users",  num_partitions=2, replication_factor=2)

    list_topics()
    describe_topic("orders")
    describe_config("orders")

    print("\n--- Increasing 'orders' partitions from 3 → 5 ---")
    add_partitions("orders", new_total=5)
    describe_topic("orders")

    # Uncomment to delete:
    # delete_topic("orders")
