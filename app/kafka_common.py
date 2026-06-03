import json
import os
import time
import uuid

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from traffic_generator import Query


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MAIN_TOPIC = os.getenv("KAFKA_MAIN_TOPIC", "geo-queries")
RETRY_TOPIC = os.getenv("KAFKA_RETRY_TOPIC", "geo-queries-retry")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "geo-queries-dlq")
TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "6"))


def json_serializer(value):
    return json.dumps(value).encode("utf-8")


def json_deserializer(value):
    return json.loads(value.decode("utf-8"))


def wait_for_kafka(timeout_s: int = 60):
    deadline = time.time() + timeout_s
    last_error = None

    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="geo-admin-wait",
            )
            admin.close()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(1)

    raise RuntimeError(f"Kafka no estuvo disponible: {last_error}")


def ensure_topics():
    wait_for_kafka()
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="geo-admin",
    )

    existing = set(admin.list_topics())
    topics = []

    for name in (MAIN_TOPIC, RETRY_TOPIC, DLQ_TOPIC):
        if name not in existing:
            topics.append(
                NewTopic(
                    name=name,
                    num_partitions=TOPIC_PARTITIONS,
                    replication_factor=1,
                )
            )

    if topics:
        try:
            admin.create_topics(topics)
        except TopicAlreadyExistsError:
            pass

    admin.close()


def make_producer():
    wait_for_kafka()
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=json_serializer,
        key_serializer=lambda value: value.encode("utf-8"),
        linger_ms=5,
    )


def make_message(query: Query) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "query": query.to_dict(),
        "attempts": 0,
        "created_at": time.time(),
        "last_error": None,
    }
