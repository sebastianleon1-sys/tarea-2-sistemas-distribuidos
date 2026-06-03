import argparse
import os
import time

from kafka import KafkaConsumer, TopicPartition

from kafka_common import MAIN_TOPIC, RETRY_TOPIC, ensure_topics
from kafka_metrics import record_backlog


GROUP_ID = os.getenv("KAFKA_GROUP_ID", "geo-workers")


def topic_backlog(consumer: KafkaConsumer, topic: str) -> int:
    partitions = consumer.partitions_for_topic(topic) or set()
    topic_partitions = [TopicPartition(topic, p) for p in partitions]

    if not topic_partitions:
        return 0

    end_offsets = consumer.end_offsets(topic_partitions)
    beginning_offsets = consumer.beginning_offsets(topic_partitions)
    total = 0

    for tp in topic_partitions:
        committed = consumer.committed(tp)

        if committed is None:
            committed = beginning_offsets.get(tp, 0)

        total += max(end_offsets.get(tp, 0) - committed, 0)

    return total


def run_monitor(interval_s: float):
    ensure_topics()
    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        group_id=GROUP_ID,
        client_id="geo-backlog-monitor",
        enable_auto_commit=False,
    )

    print(f"[Monitor] Midiendo backlog cada {interval_s}s.")

    try:
        while True:
            main_backlog = topic_backlog(consumer, MAIN_TOPIC)
            retry_backlog = topic_backlog(consumer, RETRY_TOPIC)
            record_backlog(
                {
                    "timestamp": time.time(),
                    "main_backlog": main_backlog,
                    "retry_backlog": retry_backlog,
                    "total_backlog": main_backlog + retry_backlog,
                }
            )
            time.sleep(interval_s)
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor de backlog Kafka")
    parser.add_argument("--interval-s", type=float, default=1.0)
    args = parser.parse_args()
    run_monitor(args.interval_s)
